#pragma once

#include "pipeline_types.hpp"
#include <atomic>
#include <map>
#include <memory>
#include <chrono>
#include <algorithm>
#include <deque>
#include <mutex>
#include <sstream>
#include <iomanip>

namespace io_pipeline {

// Per-stage metrics
struct StageMetrics {
    std::atomic<std::uint64_t> messages_processed{0};
    std::atomic<std::uint64_t> errors{0};
    std::atomic<std::uint64_t> total_latency_ns{0};
    std::atomic<std::uint64_t> max_latency_ns{0};
    std::atomic<std::uint64_t> min_latency_ns{UINT64_MAX};
    
    std::atomic<std::size_t> buffer_peak_size{0};
    std::atomic<std::size_t> buffer_overflow_count{0};
    
    Timestamp measurement_start{std::chrono::high_resolution_clock::now()};
    
    double get_throughput_mps() const {
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::high_resolution_clock::now() - measurement_start
        ).count();
        if (elapsed == 0) return 0;
        return static_cast<double>(messages_processed.load()) / elapsed;
    }
    
    double get_avg_latency_us() const {
        auto count = messages_processed.load();
        if (count == 0) return 0;
        return static_cast<double>(total_latency_ns.load()) / count / 1000.0;
    }
    
    double get_p99_latency_us() const {
        auto max = max_latency_ns.load();
        return static_cast<double>(max) / 1000.0;
    }
    
    double get_utilization_percent() const {
        return static_cast<double>(buffer_peak_size.load()) * 100.0;
    }
};

// System-wide metrics
class PipelineMetrics {
public:
    explicit PipelineMetrics(const std::vector<StageConfig>& stages);

    // Record message entry to stage
    void record_stage_entry(StageId stage_id, const std::shared_ptr<Message>& msg);

    // Record message exit from stage
    void record_stage_exit(StageId stage_id, const std::shared_ptr<Message>& msg, 
                          bool error = false);

    // Record buffer event
    void record_buffer_event(StageId stage_id, std::size_t buffer_size, 
                            bool overflow = false);

    // Record error
    void record_error(StageId stage_id, ErrorCode code, const std::string& details);

    // Get metrics for specific stage
    [[nodiscard]] const StageMetrics& get_stage_metrics(StageId stage_id) const;

    // Get all stage metrics
    [[nodiscard]] const std::map<StageId, StageMetrics>& get_all_metrics() const;

    // System metrics
    struct SystemMetrics {
        std::uint64_t total_messages;
        std::uint64_t total_errors;
        double overall_throughput_mps;
        double overall_avg_latency_us;
        double pipeline_utilization;
        Timestamp measurement_time;
    };

    [[nodiscard]] SystemMetrics get_system_metrics() const;

    // Generate report
    [[nodiscard]] std::string generate_report() const;

    // Reset metrics
    void reset();

private:
    std::map<StageId, StageMetrics> stage_metrics_;
    mutable std::mutex metrics_mutex_;
    Timestamp pipeline_start_{std::chrono::high_resolution_clock::now()};
};

} // namespace io_pipeline