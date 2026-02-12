/*
 * fast_engine.c — High-performance trading engine core
 *
 * Compiled to shared library, called from Python via ctypes.
 * Handles the HOT path: price comparison, signal detection, order decisions.
 *
 * Compile: cc -O3 -march=native -shared -fPIC -o fast_engine.so fast_engine.c -lm
 * On Apple Silicon: cc -O3 -mcpu=apple-m1 -shared -fPIC -o fast_engine.so fast_engine.c -lm
 */

#include <math.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>

/* ============================================================
 * Constants
 * ============================================================ */
#define MAX_EXCHANGES 10
#define MAX_PAIRS 20
#define MAX_CANDLES 2048
#define COINBASE_FEE 0.006    /* 0.6% taker fee */
#define MAKER_FEE    0.004    /* 0.4% maker fee */
#define MIN_SPREAD   0.008    /* 0.8% minimum arb spread */

/* ============================================================
 * Data Structures — packed for cache efficiency
 * ============================================================ */

typedef struct {
    double price;
    double volume;
    long   timestamp;
    int    exchange_id;
} __attribute__((packed)) PriceTick;

typedef struct {
    double open;
    double high;
    double low;
    double close;
    double volume;
    long   timestamp;
} __attribute__((packed)) Candle;

typedef struct {
    int    signal_type;   /* 0=none, 1=buy, 2=sell */
    double confidence;
    double target_price;
    double stop_price;
    int    strategy_id;
    char   reason[64];
} Signal;

typedef struct {
    double sma_20;
    double sma_50;
    double rsi_14;
    double atr_14;
    double bb_upper;
    double bb_lower;
    double vwap;
    double volume_ratio;
    int    regime;  /* 0=unknown, 1=uptrend, 2=downtrend, 3=ranging, 4=volatile */
} Indicators;

/* ============================================================
 * Arbitrage Scanner — O(n) price comparison across exchanges
 * ============================================================ */

typedef struct {
    int    has_opportunity;
    int    side;           /* 1=buy on coinbase, 2=sell on coinbase */
    double coinbase_price;
    double market_median;
    double spread_pct;
    double expected_profit_pct;
    double confidence;
    int    source_count;
} ArbResult;

/* Find median of array (modifies array in place via partial sort) */
static double median(double *arr, int n) {
    /* Simple insertion sort — n is always small (<10) */
    for (int i = 1; i < n; i++) {
        double key = arr[i];
        int j = i - 1;
        while (j >= 0 && arr[j] > key) {
            arr[j + 1] = arr[j];
            j--;
        }
        arr[j + 1] = key;
    }
    if (n % 2 == 0)
        return (arr[n/2 - 1] + arr[n/2]) / 2.0;
    return arr[n/2];
}

ArbResult check_arbitrage(double coinbase_price, double *other_prices, int n_others) {
    ArbResult result = {0};

    if (n_others < 2 || coinbase_price <= 0) return result;

    /* Compute median of other exchanges */
    double prices_copy[MAX_EXCHANGES];
    memcpy(prices_copy, other_prices, n_others * sizeof(double));
    double med = median(prices_copy, n_others);

    if (med <= 0) return result;

    /* Check price range of other exchanges — they should agree */
    double min_p = other_prices[0], max_p = other_prices[0];
    for (int i = 1; i < n_others; i++) {
        if (other_prices[i] < min_p) min_p = other_prices[i];
        if (other_prices[i] > max_p) max_p = other_prices[i];
    }
    double range_pct = (max_p - min_p) / med;
    if (range_pct > 0.003) return result; /* Exchanges disagree too much */

    /* Calculate spread */
    double spread = (coinbase_price - med) / med;
    double abs_spread = fabs(spread);

    if (abs_spread < MIN_SPREAD) return result;

    /* Calculate expected profit after fees */
    double profit = abs_spread - COINBASE_FEE;
    if (profit <= 0) return result;

    result.has_opportunity = 1;
    result.coinbase_price = coinbase_price;
    result.market_median = med;
    result.spread_pct = spread * 100.0;
    result.expected_profit_pct = profit * 100.0;
    result.source_count = n_others + 1;
    result.side = (spread > 0) ? 2 : 1; /* positive spread = sell, negative = buy */

    /* Confidence based on spread strength and source count */
    double safe_spread = 0.012;
    result.confidence = fmin(1.0, (abs_spread / safe_spread) * ((double)(n_others + 1) / 5.0));

    return result;
}

/* ============================================================
 * Technical Indicators — vectorized for speed
 * ============================================================ */

void compute_sma(const double *closes, int n, int period, double *out) {
    if (n < period) return;

    /* Running sum for O(n) SMA computation */
    double sum = 0;
    for (int i = 0; i < period; i++) sum += closes[i];
    out[period - 1] = sum / period;

    for (int i = period; i < n; i++) {
        sum += closes[i] - closes[i - period];
        out[i] = sum / period;
    }
}

double compute_rsi(const double *closes, int n, int period) {
    if (n < period + 1) return 50.0;

    double avg_gain = 0, avg_loss = 0;

    /* Initial average */
    for (int i = 1; i <= period; i++) {
        double change = closes[n - period - 1 + i] - closes[n - period - 1 + i - 1];
        if (change > 0) avg_gain += change;
        else avg_loss -= change;
    }
    avg_gain /= period;
    avg_loss /= period;

    if (avg_loss < 1e-10) return 100.0;
    double rs = avg_gain / avg_loss;
    return 100.0 - (100.0 / (1.0 + rs));
}

double compute_atr(const Candle *candles, int n, int period) {
    if (n < period + 1) return 0;

    double atr = 0;
    for (int i = n - period; i < n; i++) {
        double tr1 = candles[i].high - candles[i].low;
        double tr2 = fabs(candles[i].high - candles[i-1].close);
        double tr3 = fabs(candles[i].low - candles[i-1].close);
        double tr = fmax(tr1, fmax(tr2, tr3));
        atr += tr;
    }
    return atr / period;
}

/* Full indicator computation — single pass where possible */
Indicators compute_all_indicators(const Candle *candles, int n) {
    Indicators ind = {0};
    if (n < 50) return ind;

    double closes[MAX_CANDLES];
    double volumes[MAX_CANDLES];
    for (int i = 0; i < n && i < MAX_CANDLES; i++) {
        closes[i] = candles[i].close;
        volumes[i] = candles[i].volume;
    }

    /* SMA 20 and 50 */
    double sma20[MAX_CANDLES] = {0};
    double sma50[MAX_CANDLES] = {0};
    compute_sma(closes, n, 20, sma20);
    compute_sma(closes, n, 50, sma50);
    ind.sma_20 = sma20[n-1];
    ind.sma_50 = sma50[n-1];

    /* RSI 14 */
    ind.rsi_14 = compute_rsi(closes, n, 14);

    /* ATR 14 */
    ind.atr_14 = compute_atr(candles, n, 14);

    /* Bollinger Bands (20-period, 2 std dev) */
    double sum = 0, sum_sq = 0;
    for (int i = n - 20; i < n; i++) {
        sum += closes[i];
        sum_sq += closes[i] * closes[i];
    }
    double mean = sum / 20.0;
    double variance = (sum_sq / 20.0) - (mean * mean);
    double std_dev = sqrt(fmax(0, variance));
    ind.bb_upper = mean + 2.0 * std_dev;
    ind.bb_lower = mean - 2.0 * std_dev;

    /* VWAP */
    double cum_vol_price = 0, cum_vol = 0;
    for (int i = 0; i < n; i++) {
        cum_vol_price += closes[i] * volumes[i];
        cum_vol += volumes[i];
    }
    ind.vwap = (cum_vol > 0) ? cum_vol_price / cum_vol : closes[n-1];

    /* Volume ratio (current vs 20-period average) */
    double avg_vol = 0;
    for (int i = n - 20; i < n; i++) avg_vol += volumes[i];
    avg_vol /= 20.0;
    ind.volume_ratio = (avg_vol > 0) ? volumes[n-1] / avg_vol : 1.0;

    /* Market regime detection */
    double sma_slope = (sma20[n-1] - sma20[n-5]) / sma20[n-5];
    double price = closes[n-1];
    double atr_pct = ind.atr_14 / price;

    if (sma_slope > 0.001 && price > ind.sma_20) {
        ind.regime = 1; /* UPTREND */
    } else if (sma_slope < -0.001 && price < ind.sma_20) {
        ind.regime = 2; /* DOWNTREND */
    } else if (atr_pct > 0.02) {
        ind.regime = 4; /* VOLATILE */
    } else {
        ind.regime = 3; /* RANGING */
    }

    return ind;
}

/* ============================================================
 * Signal Generation — multi-strategy in single pass
 * ============================================================ */

Signal generate_signal(const Candle *candles, int n, const Indicators *ind) {
    Signal sig = {0};
    if (n < 50) return sig;

    double price = candles[n-1].close;

    /* Strategy 1: RSI extremes */
    if (ind->rsi_14 < 30 && price < ind->bb_lower) {
        sig.signal_type = 1; /* BUY */
        sig.confidence = fmin(0.95, 0.65 + (30.0 - ind->rsi_14) / 100.0);
        sig.target_price = ind->sma_20;
        sig.stop_price = price * 0.98;
        sig.strategy_id = 1;
        snprintf(sig.reason, sizeof(sig.reason), "RSI=%.0f+below_BB", ind->rsi_14);
        return sig;
    }

    /* Strategy 2: VWAP deviation */
    double vwap_dev = (price - ind->vwap) / ind->vwap;
    if (vwap_dev < -0.01 && ind->volume_ratio > 1.2) {
        sig.signal_type = 1; /* BUY */
        sig.confidence = fmin(0.90, 0.60 + fabs(vwap_dev) * 10.0);
        sig.target_price = ind->vwap;
        sig.stop_price = price * 0.98;
        sig.strategy_id = 2;
        snprintf(sig.reason, sizeof(sig.reason), "below_VWAP_%.1f%%", vwap_dev * 100);
        return sig;
    }

    /* Strategy 3: Bollinger Band bounce */
    if (price <= ind->bb_lower * 1.001 && ind->regime != 2) {
        sig.signal_type = 1; /* BUY */
        sig.confidence = fmin(0.85, 0.55 + (ind->bb_lower - price) / (ind->bb_upper - ind->bb_lower));
        sig.target_price = ind->sma_20;
        sig.stop_price = ind->bb_lower * 0.99;
        sig.strategy_id = 3;
        snprintf(sig.reason, sizeof(sig.reason), "BB_bounce");
        return sig;
    }

    /* Strategy 4: Sell signals (only in uptrend, only for profit-taking) */
    if (ind->regime == 1 && ind->rsi_14 > 70 && price > ind->bb_upper) {
        sig.signal_type = 2; /* SELL */
        sig.confidence = fmin(0.85, 0.55 + (ind->rsi_14 - 70.0) / 100.0);
        sig.target_price = ind->sma_20;
        sig.stop_price = price * 1.01;
        sig.strategy_id = 4;
        snprintf(sig.reason, sizeof(sig.reason), "RSI=%.0f+above_BB", ind->rsi_14);
        return sig;
    }

    return sig; /* No signal */
}

/* ============================================================
 * Batch Processing — process multiple pairs simultaneously
 * ============================================================ */

typedef struct {
    char pair[16];
    Signal signal;
    Indicators indicators;
    int regime;
} PairAnalysis;

int analyze_pairs(const Candle candles[][MAX_CANDLES], const int candle_counts[],
                  const char pairs[][16], int n_pairs, PairAnalysis *results) {
    int signals_found = 0;

    for (int p = 0; p < n_pairs; p++) {
        int n = candle_counts[p];
        if (n < 50) continue;

        strncpy(results[p].pair, pairs[p], 15);
        results[p].indicators = compute_all_indicators(candles[p], n);
        results[p].regime = results[p].indicators.regime;

        /* Skip downtrend pairs — Rule #1: NEVER LOSE MONEY */
        if (results[p].regime == 2) {
            results[p].signal.signal_type = 0;
            continue;
        }

        results[p].signal = generate_signal(candles[p], n, &results[p].indicators);
        if (results[p].signal.signal_type > 0) signals_found++;
    }

    return signals_found;
}

/* ============================================================
 * Performance Benchmarking
 * ============================================================ */

void benchmark(void) {
    printf("Fast Engine Benchmark\n");
    printf("=====================\n");

    /* Generate synthetic data */
    Candle candles[MAX_CANDLES];
    double base_price = 68000.0;
    for (int i = 0; i < MAX_CANDLES; i++) {
        double noise = ((double)rand() / RAND_MAX - 0.5) * 200.0;
        candles[i].open = base_price + noise;
        candles[i].high = candles[i].open + fabs(noise) * 0.5;
        candles[i].low = candles[i].open - fabs(noise) * 0.5;
        candles[i].close = candles[i].open + noise * 0.3;
        candles[i].volume = 100.0 + (double)rand() / RAND_MAX * 200.0;
        candles[i].timestamp = 1700000000 + i * 300;
        base_price = candles[i].close;
    }

    /* Benchmark indicator computation */
    struct timespec start, end;
    int iterations = 100000;

    clock_gettime(CLOCK_MONOTONIC, &start);
    for (int i = 0; i < iterations; i++) {
        compute_all_indicators(candles, MAX_CANDLES);
    }
    clock_gettime(CLOCK_MONOTONIC, &end);

    double elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
    printf("Indicators (%d candles, %d iterations): %.3f ms total, %.1f ns/iter\n",
           MAX_CANDLES, iterations, elapsed * 1000, elapsed * 1e9 / iterations);

    /* Benchmark signal generation */
    Indicators ind = compute_all_indicators(candles, MAX_CANDLES);
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (int i = 0; i < iterations; i++) {
        generate_signal(candles, MAX_CANDLES, &ind);
    }
    clock_gettime(CLOCK_MONOTONIC, &end);

    elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
    printf("Signals (%d iterations): %.3f ms total, %.1f ns/iter\n",
           iterations, elapsed * 1000, elapsed * 1e9 / iterations);

    /* Benchmark arb check */
    double other_prices[] = {68100.0, 68050.0, 68075.0, 68090.0};
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (int i = 0; i < iterations * 10; i++) {
        check_arbitrage(67500.0, other_prices, 4);
    }
    clock_gettime(CLOCK_MONOTONIC, &end);

    elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
    printf("Arb check (%d iterations): %.3f ms total, %.1f ns/iter\n",
           iterations * 10, elapsed * 1000, elapsed * 1e9 / (iterations * 10));

    printf("\nRegime: %d | RSI: %.1f | ATR: %.2f | VWAP: %.2f\n",
           ind.regime, ind.rsi_14, ind.atr_14, ind.vwap);
}

/* Entry point for testing */
int main(void) {
    srand(time(NULL));
    benchmark();
    return 0;
}
