/*
 * fast_exec.c — native execution/risk scoring primitives for no-loss policy.
 *
 * Compile:
 *   cc -O3 -shared -fPIC -o fast_exec.so fast_exec.c -lm
 * Apple Silicon:
 *   cc -O3 -mcpu=apple-m1 -shared -fPIC -o fast_exec.so fast_exec.c -lm
 */

#include <math.h>

typedef struct {
    int approved;
    double required_edge_pct;
    double penalty_pct;
    double score;
} ExecDecision;

double latency_penalty_pct(double p90_latency_ms, double failure_rate, double amount_usd) {
    if (p90_latency_ms < 0.0) p90_latency_ms = 0.0;
    if (failure_rate < 0.0) failure_rate = 0.0;
    if (amount_usd < 0.0) amount_usd = 0.0;

    double latency_component = fmin(0.20, p90_latency_ms / 10000.0);
    double reliability_component = fmin(0.30, failure_rate * 0.50);
    double size_component = fmin(0.20, amount_usd / 10000.0);
    return latency_component + reliability_component + size_component;
}

double micro_edge_pct(double fast_avg, double slow_avg, double spread_pct) {
    if (slow_avg <= 0.0) return 0.0;
    if (spread_pct < 0.0) spread_pct = 0.0;

    double momentum_pct = ((fast_avg - slow_avg) / slow_avg) * 100.0;
    double edge = fmax(0.0, momentum_pct * 0.55) - fmax(0.0, spread_pct * 0.35);
    return fmax(0.0, edge);
}

ExecDecision no_loss_gate(
    double expected_edge_pct,
    double total_cost_pct,
    double spread_pct,
    double latency_ms,
    double failure_rate,
    double signal_confidence,
    double min_expected_edge_pct,
    double max_spread_pct,
    double max_latency_ms,
    double max_failure_rate,
    double confidence_floor,
    int buy_blocked_regime
) {
    ExecDecision d;
    d.approved = 1;
    d.penalty_pct = latency_penalty_pct(latency_ms, failure_rate, 1000.0);
    d.required_edge_pct = fmax(min_expected_edge_pct, total_cost_pct + d.penalty_pct);

    if (expected_edge_pct < d.required_edge_pct) d.approved = 0;
    if (spread_pct > max_spread_pct) d.approved = 0;
    if (latency_ms > max_latency_ms) d.approved = 0;
    if (failure_rate > max_failure_rate) d.approved = 0;
    if (signal_confidence < confidence_floor) d.approved = 0;
    if (buy_blocked_regime) d.approved = 0;

    /* Score >0 indicates margin above required edge after frictions. */
    d.score = expected_edge_pct - d.required_edge_pct;
    return d;
}
