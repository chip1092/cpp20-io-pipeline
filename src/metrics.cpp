#include "metrics.hpp"
#include <iomanip>
#include <sstream>

namespace io_pipeline {

PipelineMetrics::PipelineMetrics(const std::vector<StageConfig>& stages) {
    for (const auto& stage : stages) {
        stage_metrics_[stage.id] = StageMetrics{};
    }
}

void PipelineMetrics::record_stage_entry(StageId stage_id, 
                                        const std::shared_ptr<Message>& msg) {
    if (!msg) return;
    
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    if (stage_metrics_.find(stage_id) != stage_metrics_.end()) {
        msg->current_stage.store(stage_id, std::memory_order_release);
    }
}

void PipelineMetrics::record_stage_exit(StageId stage_id,
                                       const std::shared_ptr<Message>& msg,
                                       bool error) {
    if (!msg) return;

    std::lock_guard<std::mutex> lock(metrics_mutex_);
    auto it = stage_metrics_.find(stage_id);
    if (it == stage_metrics_.end()) return;

    auto& metrics = it->second;
    auto latency = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::high_resolution_clock::now() - msg->arrival_time
    ).count();

    metrics.messages_processed.fetch_add(1, std::memory_order_release);
    metrics.total_latency_ns.fetch_add(latency, std::memory_order_release);
    
    auto max_lat = metrics.max_latency_ns.load(std::memory_order_acquire);
    while (latency > max_lat && 
           !metrics.max_latency_ns.compare_exchange_weak(
               max_lat, latency, std::memory_order_release)) {
    }

    auto min_lat = metrics.min_latency_ns.load(std::memory_order_acquire);
    while (latency < min_lat && 
           !metrics.min_latency_ns.compare_exchange_weak(
               min_lat, latency, std::memory_order_release)) {
    }

    if (error) {
        metrics.errors.fetch_add(1, std::memory_order_release);
    }

    msg->stage_completion_mask |= (1ULL << stage_id);
}

void PipelineMetrics::record_buffer_event(StageId stage_id, std::size_t buffer_size,
                                         bool overflow) {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    auto it = stage_metrics_.find(stage_id);
    if (it == stage_metrics_.end()) return;

    auto& metrics = it->second;
    
    auto peak = metrics.buffer_peak_size.load(std::memory_order_acquire);
    while (buffer_size > peak &&
           !metrics.buffer_peak_size.compare_exchange_weak(
               peak, buffer_size, std::memory_order_release)) {
    }

    if (overflow) {
        metrics.buffer_overflow_count.fetch_add(1, std::memory_order_release);
    }
}

void PipelineMetrics::record_error(StageId stage_id, ErrorCode code,
                                  const std::string& details) {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    auto it = stage_metrics_.find(stage_id);
    if (it != stage_metrics_.end()) {
        it->second.errors.fetch_add(1, std::memory_order_release);
    }
}

const StageMetrics& PipelineMetrics::get_stage_metrics(StageId stage_id) const {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    static StageMetrics empty;
    auto it = stage_metrics_.find(stage_id);
    return it != stage_metrics_.end() ? it->second : empty;
}

const std::map<StageId, StageMetrics>& PipelineMetrics::get_all_metrics() const {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    return stage_metrics_;
}

PipelineMetrics::SystemMetrics PipelineMetrics::get_system_metrics() const {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    
    SystemMetrics sys{};
    double total_latency = 0;
    std::uint64_t total_messages = 0;
    std::uint64_t total_errors = 0;

    for (const auto& [stage_id, metrics] : stage_metrics_) {
        total_messages += metrics.messages_processed.load();
        total_errors += metrics.errors.load();
        total_latency += metrics.total_latency_ns.load();
    }

    sys.total_messages = total_messages;
    sys.total_errors = total_errors;
    sys.measurement_time = std::chrono::high_resolution_clock::now();
    
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
        sys.measurement_time - pipeline_start_
    ).count();

    sys.overall_throughput_mps = elapsed > 0 ? 
        static_cast<double>(total_messages) / elapsed : 0;
    sys.overall_avg_latency_us = total_messages > 0 ? 
        total_latency / total_messages / 1000.0 : 0;
    
    sys.pipeline_utilization = 0;
    for (const auto& [stage_id, metrics] : stage_metrics_) {
        sys.pipeline_utilization += metrics.get_utilization_percent();
    }
    if (!stage_metrics_.empty()) {
        sys.pipeline_utilization /= stage_metrics_.size();
    }

    return sys;
}

std::string PipelineMetrics::generate_report() const {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    std::ostringstream oss;

    oss << "=== IO Pipeline Metrics Report ===\n";
    oss << std::fixed << std::setprecision(2);

    auto sys = get_system_metrics();
    oss << "\n--- System Metrics ---\n";
    oss << "Total Messages: " << sys.total_messages << "\n";
    oss << "Total Errors: " << sys.total_errors << "\n";
    oss << "Throughput: " << sys.overall_throughput_mps << " msg/sec\n";
    oss << "Avg Latency: " << sys.overall_avg_latency_us << " µs\n";
    oss << "Pipeline Utilization: " << sys.pipeline_utilization << "%\n";

    oss << "\n--- Per-Stage Metrics ---\n";
    for (const auto& [stage_id, metrics] : stage_metrics_) {
        oss << "\nStage " << static_cast<int>(stage_id) << ":\n";
        oss << "  Messages: " << metrics.messages_processed.load() << "\n";
        oss << "  Errors: " << metrics.errors.load() << "\n";
        oss << "  Throughput: " << metrics.get_throughput_mps() << " msg/sec\n";
        oss << "  Avg Latency: " << metrics.get_avg_latency_us() << " µs\n";
        oss << "  P99 Latency: " << metrics.get_p99_latency_us() << " µs\n";
        oss << "  Buffer Peak: " << metrics.buffer_peak_size.load() << "\n";
        oss << "  Buffer Overflows: " << metrics.buffer_overflow_count.load() << "\n";
    }

    return oss.str();
}

void PipelineMetrics::reset() {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    for (auto& [stage_id, metrics] : stage_metrics_) {
        metrics.messages_processed.store(0);
        metrics.errors.store(0);
        metrics.total_latency_ns.store(0);
        metrics.max_latency_ns.store(0);
        metrics.min_latency_ns.store(UINT64_MAX);
        metrics.buffer_peak_size.store(0);
        metrics.buffer_overflow_count.store(0);
        metrics.measurement_start = std::chrono::high_resolution_clock::now();
    }
    pipeline_start_ = std::chrono::high_resolution_clock::now();
}

} // namespace io_pipeline