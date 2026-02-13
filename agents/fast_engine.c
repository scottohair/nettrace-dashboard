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
 * Adaptive Risk Engine — Kelly-inspired position sizing
 *
 * Scales all risk parameters with portfolio value automatically.
 * No static constants — everything is a function of capital.
 * ============================================================ */

typedef struct {
    double portfolio_value;
    double max_trade_usd;
    double max_daily_loss;
    double min_reserve;
    double optimal_grid_size;
    int    optimal_grid_levels;
    double optimal_dca_daily;
    double streak_multiplier;
    int    win_streak;
    int    loss_streak;
    double kelly_fraction;    /* optimal bet fraction */
} AdaptiveRisk;

/* Kelly Criterion: f* = (bp - q) / b
 * where b = odds, p = win probability, q = 1-p
 * We use fractional Kelly (25%) for safety */
static double kelly_fraction(double win_rate, double avg_win, double avg_loss) {
    if (avg_loss <= 0 || win_rate <= 0) return 0.01;
    double b = avg_win / avg_loss;  /* payoff ratio */
    double p = win_rate;
    double q = 1.0 - p;
    double f = (b * p - q) / b;
    /* Fractional Kelly: use 25% of full Kelly */
    f *= 0.25;
    /* Clamp to [1%, 20%] of portfolio */
    if (f < 0.01) f = 0.01;
    if (f > 0.20) f = 0.20;
    return f;
}

AdaptiveRisk compute_adaptive_risk(
    double portfolio_value,
    int win_streak,
    int loss_streak,
    double win_rate,    /* historical win rate 0-1 */
    double avg_win,     /* average win in USD */
    double avg_loss     /* average loss in USD */
) {
    AdaptiveRisk risk = {0};
    risk.portfolio_value = portfolio_value;
    risk.win_streak = win_streak;
    risk.loss_streak = loss_streak;

    /* Streak multiplier: size up on wins, down on losses */
    if (win_streak > 0) {
        double bonus = win_streak * 0.05;
        if (bonus > 0.25) bonus = 0.25;
        risk.streak_multiplier = 1.0 + bonus;
    } else if (loss_streak > 0) {
        risk.streak_multiplier = 1.0 / (1.0 + loss_streak * 0.5);
        if (risk.streak_multiplier < 0.25) risk.streak_multiplier = 0.25;
    } else {
        risk.streak_multiplier = 1.0;
    }

    /* Kelly-optimal fraction */
    risk.kelly_fraction = kelly_fraction(win_rate, avg_win, avg_loss);

    /* Max trade: Kelly fraction * portfolio * streak */
    risk.max_trade_usd = portfolio_value * risk.kelly_fraction * risk.streak_multiplier;
    if (risk.max_trade_usd < 1.00) risk.max_trade_usd = 1.00;
    /* Hard cap at 20% of portfolio */
    if (risk.max_trade_usd > portfolio_value * 0.20)
        risk.max_trade_usd = portfolio_value * 0.20;

    /* Daily loss: 5% of portfolio */
    risk.max_daily_loss = portfolio_value * 0.05;
    if (risk.max_daily_loss < 1.00) risk.max_daily_loss = 1.00;

    /* Reserve: 15% of portfolio, minimum $1.50 */
    risk.min_reserve = portfolio_value * 0.15;
    if (risk.min_reserve < 1.50) risk.min_reserve = 1.50;

    /* Grid sizing: deploy up to 60% across levels */
    double deployable = portfolio_value * 0.60;
    risk.optimal_grid_levels = (int)(deployable / fmax(1.0, risk.max_trade_usd));
    if (risk.optimal_grid_levels < 2) risk.optimal_grid_levels = 2;
    if (risk.optimal_grid_levels > 10) risk.optimal_grid_levels = 10;
    risk.optimal_grid_size = deployable / risk.optimal_grid_levels;
    if (risk.optimal_grid_size < 1.00) risk.optimal_grid_size = 1.00;

    /* DCA: 2-5% of portfolio per day */
    risk.optimal_dca_daily = portfolio_value * 0.03;
    if (risk.optimal_dca_daily < 0.30) risk.optimal_dca_daily = 0.30;
    if (risk.optimal_dca_daily > 100.0) risk.optimal_dca_daily = 100.0;

    return risk;
}

/* ============================================================
 * Grid Price Calculator — compute optimal grid levels in C
 * Returns number of levels written to output arrays
 * ============================================================ */

int compute_grid_levels(
    double center_price,
    double spacing_pct,
    int levels_above,
    int levels_below,
    double *buy_prices,   /* output: buy level prices */
    double *sell_prices,  /* output: sell level prices */
    int max_levels
) {
    int total = 0;

    for (int i = 1; i <= levels_below && total < max_levels; i++) {
        buy_prices[i-1] = center_price * (1.0 - spacing_pct * i);
        total++;
    }
    for (int i = 1; i <= levels_above && total < max_levels; i++) {
        sell_prices[i-1] = center_price * (1.0 + spacing_pct * i);
        total++;
    }

    return total;
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

    /* Benchmark adaptive risk */
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (int i = 0; i < iterations * 10; i++) {
        compute_adaptive_risk(1200.0, 3, 0, 0.65, 2.50, 1.80);
    }
    clock_gettime(CLOCK_MONOTONIC, &end);
    elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
    printf("Adaptive risk (%d iterations): %.3f ms total, %.1f ns/iter\n",
           iterations * 10, elapsed * 1000, elapsed * 1e9 / (iterations * 10));

    /* Show adaptive risk at different portfolio sizes */
    printf("\nAdaptive Risk Scaling:\n");
    double test_vals[] = {13.48, 100, 500, 1200, 5000, 10000};
    for (int i = 0; i < 6; i++) {
        AdaptiveRisk r = compute_adaptive_risk(test_vals[i], 0, 0, 0.60, 2.0, 1.5);
        printf("  $%9.2f -> max_trade $%.2f | daily_loss $%.2f | reserve $%.2f | "
               "grid $%.2f x %d | kelly %.1f%%\n",
               r.portfolio_value, r.max_trade_usd, r.max_daily_loss, r.min_reserve,
               r.optimal_grid_size, r.optimal_grid_levels, r.kelly_fraction * 100);
    }

    /* Benchmark grid level calculation */
    double buy_p[20], sell_p[20];
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (int i = 0; i < iterations * 10; i++) {
        compute_grid_levels(68000.0, 0.01, 5, 5, buy_p, sell_p, 20);
    }
    clock_gettime(CLOCK_MONOTONIC, &end);
    elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
    printf("\nGrid levels (%d iterations): %.3f ms total, %.1f ns/iter\n",
           iterations * 10, elapsed * 1000, elapsed * 1e9 / (iterations * 10));
}

/* ============================================================
 * HF Tick Scorer — sub-microsecond momentum detection
 *
 * Processes raw tick stream to detect momentum bursts,
 * spread changes, and volume acceleration in real-time.
 * ============================================================ */

#define MAX_TICK_WINDOW 256

typedef struct {
    double prices[MAX_TICK_WINDOW];
    double volumes[MAX_TICK_WINDOW];
    long   timestamps[MAX_TICK_WINDOW];
    int    count;
    int    head;  /* circular buffer head */
} TickWindow;

typedef struct {
    int    signal;          /* 0=none, 1=buy, 2=sell */
    double confidence;
    double momentum;        /* price velocity per second */
    double vol_acceleration; /* volume rate of change */
    double spread_score;    /* bid-ask tightening indicator */
    double composite;       /* weighted composite score */
    char   reason[64];
} TickSignal;

TickSignal score_tick_momentum(
    const double *recent_prices,   /* last N prices (newest first) */
    const double *recent_volumes,  /* last N volumes */
    int n_ticks,
    double current_bid,
    double current_ask,
    double avg_spread
) {
    TickSignal sig = {0};
    if (n_ticks < 10) return sig;

    /* Price velocity: weighted linear regression slope */
    double sum_x = 0, sum_y = 0, sum_xy = 0, sum_x2 = 0;
    int window = (n_ticks < 20) ? n_ticks : 20;
    for (int i = 0; i < window; i++) {
        double x = (double)i;
        double y = recent_prices[i];
        sum_x += x;
        sum_y += y;
        sum_xy += x * y;
        sum_x2 += x * x;
    }
    double slope = (window * sum_xy - sum_x * sum_y) / (window * sum_x2 - sum_x * sum_x);
    double price_mean = sum_y / window;
    sig.momentum = (price_mean > 0) ? -slope / price_mean : 0;  /* negative slope = price going up (newest first) */

    /* Volume acceleration: compare recent 5 vs previous 10 */
    double recent_vol = 0, baseline_vol = 0;
    int rv_n = (window < 5) ? window : 5;
    int bv_n = (window < 15) ? window - rv_n : 10;
    for (int i = 0; i < rv_n; i++) recent_vol += recent_volumes[i];
    for (int i = rv_n; i < rv_n + bv_n; i++) baseline_vol += recent_volumes[i];
    recent_vol /= rv_n;
    baseline_vol = (bv_n > 0) ? baseline_vol / bv_n : recent_vol;
    sig.vol_acceleration = (baseline_vol > 0) ? recent_vol / baseline_vol : 1.0;

    /* Spread analysis */
    double current_spread = (current_ask > 0 && current_bid > 0) ?
        (current_ask - current_bid) / current_bid : 0;
    sig.spread_score = (avg_spread > 0) ? 1.0 - (current_spread / avg_spread) : 0;
    if (sig.spread_score < -1.0) sig.spread_score = -1.0;
    if (sig.spread_score > 1.0) sig.spread_score = 1.0;

    /* Composite score: momentum + volume + spread tightening */
    sig.composite = sig.momentum * 0.50
                  + (sig.vol_acceleration - 1.0) * 0.10 * 0.30
                  + sig.spread_score * 0.20;

    /* Generate signal if strong enough */
    double abs_composite = fabs(sig.composite);
    if (abs_composite > 0.001 && sig.vol_acceleration > 1.2) {
        sig.signal = (sig.composite > 0) ? 1 : 2;  /* positive = BUY */
        sig.confidence = fmin(0.95, 0.60 + abs_composite * 50.0);
        snprintf(sig.reason, sizeof(sig.reason),
                 "tick_mom=%.4f vol=%.1fx spr=%.2f",
                 sig.momentum, sig.vol_acceleration, sig.spread_score);
    }

    return sig;
}

/* ============================================================
 * Multi-Strategy Fast Scanner — score all strategies in one pass
 *
 * Returns a bitmap of which strategies triggered + best signal.
 * Used by strike teams for rapid opportunity detection.
 * ============================================================ */

typedef struct {
    int    strategies_triggered;  /* bitmap: bit0=momentum, bit1=mean_rev, bit2=breakout, bit3=arb */
    Signal best_signal;
    double momentum_score;
    double mean_rev_zscore;
    double breakout_score;
    double arb_spread;
    int    confirming_count;      /* how many strategies agree */
} MultiStrategyResult;

MultiStrategyResult multi_strategy_scan(
    const Candle *candles, int n_candles,
    double coinbase_price,
    double *other_prices, int n_others
) {
    MultiStrategyResult res = {0};
    if (n_candles < 20) return res;

    double closes[MAX_CANDLES];
    int n = (n_candles < MAX_CANDLES) ? n_candles : MAX_CANDLES;
    for (int i = 0; i < n; i++) closes[i] = candles[i].close;

    double price = closes[n-1];
    int direction = 0;  /* 0=none, 1=buy, 2=sell */

    /* Strategy 1: Momentum (EMA3 vs EMA8 crossover) */
    {
        double ema3 = closes[n-1], ema8 = closes[n-1];
        double m3 = 2.0 / 4.0, m8 = 2.0 / 9.0;
        for (int i = n-2; i >= 0 && i >= n-20; i--) {
            ema3 = closes[i] * m3 + ema3 * (1-m3);
            ema8 = closes[i] * m8 + ema8 * (1-m8);
        }
        res.momentum_score = (ema3 - ema8) / ema8;
        if (fabs(res.momentum_score) > 0.002) {
            res.strategies_triggered |= 1;
            direction = (res.momentum_score > 0) ? 1 : 2;
            res.confirming_count++;
        }
    }

    /* Strategy 2: Mean Reversion (z-score) */
    {
        double mean = 0, sq = 0;
        for (int i = 0; i < n; i++) { mean += closes[i]; sq += closes[i]*closes[i]; }
        mean /= n;
        double var = sq/n - mean*mean;
        double std = sqrt(fmax(0, var));
        res.mean_rev_zscore = (std > 0) ? (price - mean) / std : 0;

        if (fabs(res.mean_rev_zscore) > 2.0) {
            res.strategies_triggered |= 2;
            int mr_dir = (res.mean_rev_zscore < -2) ? 1 : 2;
            if (direction == 0) direction = mr_dir;
            if (mr_dir == direction) res.confirming_count++;
        }
    }

    /* Strategy 3: Breakout (new high/low with volume) */
    {
        double max_h = candles[1].high, min_l = candles[1].low;
        double avg_vol = 0;
        for (int i = 1; i < n && i < 20; i++) {
            if (candles[i].high > max_h) max_h = candles[i].high;
            if (candles[i].low < min_l) min_l = candles[i].low;
            avg_vol += candles[i].volume;
        }
        avg_vol /= (n < 20 ? n-1 : 19);

        if (price > max_h && candles[0].volume > avg_vol * 1.5) {
            res.breakout_score = (price - max_h) / max_h;
            res.strategies_triggered |= 4;
            if (direction == 0) direction = 1;
            if (direction == 1) res.confirming_count++;
        } else if (price < min_l && candles[0].volume > avg_vol * 1.5) {
            res.breakout_score = (min_l - price) / min_l;
            res.strategies_triggered |= 4;
            if (direction == 0) direction = 2;
            if (direction == 2) res.confirming_count++;
        }
    }

    /* Strategy 4: Arbitrage */
    if (n_others >= 2 && coinbase_price > 0) {
        ArbResult arb = check_arbitrage(coinbase_price, other_prices, n_others);
        res.arb_spread = arb.spread_pct;
        if (arb.has_opportunity) {
            res.strategies_triggered |= 8;
            int arb_dir = arb.side;
            if (direction == 0) direction = arb_dir;
            if (arb_dir == direction) res.confirming_count++;
        }
    }

    /* Build best signal from composite */
    if (res.confirming_count >= 2) {
        res.best_signal.signal_type = direction;
        res.best_signal.confidence = fmin(0.95,
            0.55 + res.confirming_count * 0.12
            + fabs(res.momentum_score) * 5.0
            + fabs(res.mean_rev_zscore > 2 ? res.mean_rev_zscore - 2 : 0) * 0.05
        );
        res.best_signal.target_price = price;
        res.best_signal.stop_price = (direction == 1) ? price * 0.98 : price * 1.02;
        res.best_signal.strategy_id = 99; /* multi-strat composite */
        snprintf(res.best_signal.reason, sizeof(res.best_signal.reason),
                 "multi_%d_confirm(mom=%.3f,z=%.1f,brk=%.3f)",
                 res.confirming_count, res.momentum_score,
                 res.mean_rev_zscore, res.breakout_score);
    }

    return res;
}

/* Entry point for testing */
int main(void) {
    srand(time(NULL));
    benchmark();
    return 0;
}
