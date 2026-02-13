/*
 * fast_exec_cpp.cpp — C++ implementation of execution policy primitives.
 *
 * Build:
 *   c++ -O3 -std=c++17 -shared -fPIC -o fast_exec_cpp.so fast_exec_cpp.cpp
 */

#include <algorithm>
#include <cmath>

extern "C" {

typedef struct {
    int approved;
    double required_edge_pct;
    double penalty_pct;
    double score;
} ExecDecisionCpp;

double latency_penalty_pct_cpp(double p90_latency_ms, double failure_rate, double amount_usd) {
    p90_latency_ms = std::max(0.0, p90_latency_ms);
    failure_rate = std::max(0.0, failure_rate);
    amount_usd = std::max(0.0, amount_usd);

    const double latency_component = std::min(0.20, p90_latency_ms / 10000.0);
    const double reliability_component = std::min(0.30, failure_rate * 0.50);
    const double size_component = std::min(0.20, amount_usd / 10000.0);
    return latency_component + reliability_component + size_component;
}

double micro_edge_pct_cpp(double fast_avg, double slow_avg, double spread_pct) {
    if (slow_avg <= 0.0) return 0.0;
    spread_pct = std::max(0.0, spread_pct);
    const double momentum_pct = ((fast_avg - slow_avg) / slow_avg) * 100.0;
    const double edge = std::max(0.0, momentum_pct * 0.55) - std::max(0.0, spread_pct * 0.35);
    return std::max(0.0, edge);
}

ExecDecisionCpp no_loss_gate_cpp(
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
    ExecDecisionCpp d{};
    d.penalty_pct = latency_penalty_pct_cpp(latency_ms, failure_rate, 1000.0);
    d.required_edge_pct = std::max(min_expected_edge_pct, total_cost_pct + d.penalty_pct);
    d.score = expected_edge_pct - d.required_edge_pct;

    const bool approved =
        expected_edge_pct >= d.required_edge_pct &&
        spread_pct <= max_spread_pct &&
        latency_ms <= max_latency_ms &&
        failure_rate <= max_failure_rate &&
        signal_confidence >= confidence_floor &&
        !buy_blocked_regime;
    d.approved = approved ? 1 : 0;
    return d;
}

}  // extern "C"
