#pragma once

#include "pipeline_types.hpp"
#include "circular_buffer.hpp"
#include "metrics.hpp"
#include "error_handler.hpp"
#include "pipeline_stage.hpp"
#include "thread_pool.hpp"
#include <memory>
#include <vector>
#include <map>
#include <mutex>
#include <atomic>

namespace io_pipeline {

// Main IO Processing Pipeline
class IOPipeline {
public:
    explicit IOPipeline(const PipelineConfig& config);
    
    ~IOPipeline();

    // Non-copyable
    IOPipeline(const IOPipeline&) = delete;
    IOPipeline& operator=(const IOPipeline&) = delete;

    // Initialize pipeline
    [[nodiscard]] ErrorCode initialize();

    // Submit message to pipeline
    [[nodiscard]] ErrorCode submit_message(std::shared_ptr<Message> msg);

    // Register service handler for dispatch stage
    void register_service_handler(StageId service_id, ServiceHandler handler);

    // Unregister service handler
    void unregister_service_handler(StageId service_id);

    // Get pipeline metrics
    [[nodiscard]] std::shared_ptr<PipelineMetrics> get_metrics() const {
        return metrics_;
    }

    // Generate metrics report
    [[nodiscard]] std::string get_metrics_report() const {
        return metrics_->generate_report();
    }

    // Shutdown pipeline gracefully
    void shutdown();

    // Check if pipeline is running
    [[nodiscard]] bool is_running() const { return running_.load(); }

    // Get configuration
    [[nodiscard]] const PipelineConfig& get_config() const { return config_; }

private:
    PipelineConfig config_;
    std::shared_ptr<PipelineMetrics> metrics_;
    std::shared_ptr<ErrorLog> error_log_;
    std::shared_ptr<ErrorRecoveryHandler> recovery_handler_;
    std::shared_ptr<ThreadPool> thread_pool_;

    std::vector<std::shared_ptr<PipelineStage>> stages_;
    std::map<StageType, std::shared_ptr<PipelineStage>> stage_map_;

    std::atomic<bool> running_{false};
    std::atomic<std::uint64_t> message_counter_{0};

    mutable std::mutex stages_mutex_;

    // Initialize stages based on config
    [[nodiscard]] ErrorCode create_stages();

    // Process message through pipeline stages
    void process_message_async(std::shared_ptr<Message> msg, std::size_t stage_index);

    // Handle backpressure
    [[nodiscard]] ErrorCode handle_backpressure(const std::shared_ptr<Message>& msg);

    // Get or create stage
    [[nodiscard]] std::shared_ptr<PipelineStage> get_stage(StageType type);
};

// Convenience factory for creating pipelines
class PipelineBuilder {
public:
    PipelineBuilder& set_num_threads(std::size_t n) {
        config_.num_worker_threads = n;
        return *this;
    }

    PipelineBuilder& set_buffer_capacity(std::size_t capacity) {
        config_.initial_buffer_capacity = capacity;
        return *this;
    }

    PipelineBuilder& set_max_buffer_capacity(std::size_t capacity) {
        config_.max_buffer_capacity = capacity;
        return *this;
    }

    PipelineBuilder& set_stage_timeout(std::chrono::milliseconds timeout) {
        config_.stage_timeout = timeout;
        return *this;
    }

    PipelineBuilder& enable_metrics(bool enable = true) {
        config_.enable_metrics = enable;
        return *this;
    }

    PipelineBuilder& enable_logging(bool enable = true, 
                                   const std::string& path = "pipeline.log") {
        config_.enable_persistent_logging = enable;
        config_.log_path = path;
        return *this;
    }

    PipelineBuilder& set_max_memory(std::size_t bytes) {
        config_.max_memory_bytes = bytes;
        return *this;
    }

    PipelineBuilder& set_buffer_growth_factor(float factor) {
        config_.buffer_growth_factor = factor;
        return *this;
    }

    PipelineBuilder& set_throughput_target(float mps) {
        config_.throughput_target = mps;
        return *this;
    }

    PipelineBuilder& set_latency_target(float us) {
        config_.latency_target_us = us;
        return *this;
    }

    PipelineBuilder& add_stage(const StageConfig& stage_config) {
        config_.stages.push_back(stage_config);
        return *this;
    }

    [[nodiscard]] std::unique_ptr<IOPipeline> build() {
        return std::make_unique<IOPipeline>(config_);
    }

private:
    PipelineConfig config_{
        .num_worker_threads = std::thread::hardware_concurrency(),
        .stages = {},
        .initial_buffer_capacity = 1024,
        .max_buffer_capacity = 1024 * 1024,
        .stage_timeout = std::chrono::milliseconds(5000),
        .enable_metrics = true,
        .enable_persistent_logging = true,
        .log_path = "pipeline.log",
        .max_memory_bytes = 1024 * 1024 * 1024,
        .buffer_growth_factor = 2.0f,
        .throughput_target = 100000.0f,
        .latency_target_us = 100.0f
    };
};

} // namespace io_pipeline