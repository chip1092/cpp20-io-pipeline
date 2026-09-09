#include "io_pipeline.hpp"
#include <iostream>
#include <algorithm>

namespace io_pipeline {

IOPipeline::IOPipeline(const PipelineConfig& config)
    : config_(config) {
    metrics_ = std::make_shared<PipelineMetrics>(config.stages);
    error_log_ = std::make_shared<ErrorLog>(config.log_path);
    recovery_handler_ = std::make_shared<ErrorRecoveryHandler>(config.max_memory_bytes);
    thread_pool_ = std::make_shared<ThreadPool>(config.num_worker_threads);
}

IOPipeline::~IOPipeline() {
    shutdown();
}

ErrorCode IOPipeline::initialize() {
    if (running_.exchange(true)) {
        return ErrorCode::SUCCESS; // Already initialized
    }

    auto error = create_stages();
    if (error != ErrorCode::SUCCESS) {
        running_.store(false);
        error_log_->log_error(error, 0, 0, 0, "Failed to create pipeline stages");
        return error;
    }

    metrics_->reset();
    error_log_->log_info("IOPipeline initialized with " + 
                        std::to_string(config_.stages.size()) + " stages");

    return ErrorCode::SUCCESS;
}

ErrorCode IOPipeline::create_stages() {
    std::lock_guard<std::mutex> lock(stages_mutex_);
    
    for (const auto& stage_config : config_.stages) {
        std::shared_ptr<PipelineStage> stage;

        switch (stage_config.type) {
            case StageType::DESERIALIZE:
                stage = std::make_shared<DeserializeStage>(stage_config, metrics_);
                break;
            case StageType::DECOMPRESS:
                stage = std::make_shared<DecompressStage>(stage_config, metrics_);
                break;
            case StageType::DECRYPT:
                stage = std::make_shared<DecryptStage>(stage_config, metrics_);
                break;
            case StageType::DISPATCH:
                stage = std::make_shared<DispatchStage>(stage_config, metrics_);
                break;
            case StageType::SERIALIZE:
                stage = std::make_shared<SerializeStage>(stage_config, metrics_);
                break;
            case StageType::COMPRESS:
                stage = std::make_shared<CompressStage>(stage_config, metrics_);
                break;
            case StageType::ENCRYPT:
                stage = std::make_shared<EncryptStage>(stage_config, metrics_);
                break;
            default:
                return ErrorCode::STAGE_PROCESSING_ERROR;
        }

        if (!stage) {
            return ErrorCode::STAGE_PROCESSING_ERROR;
        }

        stages_.push_back(stage);
        stage_map_[stage_config.type] = stage;
    }

    return ErrorCode::SUCCESS;
}

ErrorCode IOPipeline::submit_message(std::shared_ptr<Message> msg) {
    if (!running_.load(std::memory_order_acquire)) {
        return ErrorCode::STAGE_PROCESSING_ERROR;
    }

    if (!msg) {
        return ErrorCode::INVALID_MESSAGE;
    }

    msg->id = message_counter_.fetch_add(1, std::memory_order_release);
    msg->arrival_time = std::chrono::high_resolution_clock::now();
    msg->current_stage.store(0, std::memory_order_release);

    if (recovery_handler_->is_session_rate_limited(msg->session_id)) {
        error_log_->log_warning("Session " + std::to_string(msg->session_id) + 
                               " is rate limited");
        return ErrorCode::SESSION_RATE_LIMITED;
    }

    auto error = handle_backpressure(msg);
    if (error != ErrorCode::SUCCESS) {
        return error;
    }

    // Submit first stage processing
    if (!stages_.empty()) {
        thread_pool_->submit([this, msg]() {
            process_message_async(msg, 0);
        });
    }

    return ErrorCode::SUCCESS;
}

ErrorCode IOPipeline::handle_backpressure(const std::shared_ptr<Message>& msg) {
    if (stages_.empty()) {
        return ErrorCode::SUCCESS;
    }

    auto& first_stage = stages_[0];
    auto stats = first_stage->get_buffer_stats();

    if (stats.current_size >= stats.current_capacity) {
        std::size_t current_size = stats.current_size;
        std::size_t max_size = stats.current_capacity;
        
        auto error = recovery_handler_->handle_buffer_full(msg, current_size, max_size);
        if (error != ErrorCode::SUCCESS) {
            error_log_->log_error(error, msg->session_id, msg->id, 
                                first_stage->get_id(), "Buffer full, backpressure applied");
            return error;
        }
    }

    return ErrorCode::SUCCESS;
}

void IOPipeline::process_message_async(std::shared_ptr<Message> msg, 
                                      std::size_t stage_index) {
    if (stage_index >= stages_.size()) {
        // Message completed all stages
        return;
    }

    auto& stage = stages_[stage_index];
    auto error = stage->process(msg);

    if (error != ErrorCode::SUCCESS) {
        error_log_->log_error(error, msg->session_id, msg->id, stage->get_id(),
                            "Stage processing error");
        metrics_->record_error(stage->get_id(), error, "Processing failed");
        return;
    }

    // Submit next stage
    if (stage_index + 1 < stages_.size()) {
        thread_pool_->submit([this, msg, next_index = stage_index + 1]() {
            process_message_async(msg, next_index);
        });
    }
}

void IOPipeline::register_service_handler(StageId service_id, ServiceHandler handler) {
    std::lock_guard<std::mutex> lock(stages_mutex_);
    
    auto it = stage_map_.find(StageType::DISPATCH);
    if (it != stage_map_.end()) {
        auto dispatch_stage = std::dynamic_pointer_cast<DispatchStage>(it->second);
        if (dispatch_stage) {
            dispatch_stage->register_handler(service_id, handler);
        }
    }
}

void IOPipeline::unregister_service_handler(StageId service_id) {
    std::lock_guard<std::mutex> lock(stages_mutex_);
    
    auto it = stage_map_.find(StageType::DISPATCH);
    if (it != stage_map_.end()) {
        auto dispatch_stage = std::dynamic_pointer_cast<DispatchStage>(it->second);
        if (dispatch_stage) {
            dispatch_stage->unregister_handler(service_id);
        }
    }
}

void IOPipeline::shutdown() {
    if (!running_.exchange(false)) {
        return; // Already shutdown
    }

    if (thread_pool_) {
        thread_pool_->wait_for_completion();
        thread_pool_->shutdown();
    }

    if (error_log_) {
        error_log_->flush();
    }

    error_log_->log_info("IOPipeline shutdown complete");
}

std::shared_ptr<PipelineStage> IOPipeline::get_stage(StageType type) {
    std::lock_guard<std::mutex> lock(stages_mutex_);
    auto it = stage_map_.find(type);
    return it != stage_map_.end() ? it->second : nullptr;
}

} // namespace io_pipeline