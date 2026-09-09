#include "pipeline_stage.hpp"
#include <iostream>

namespace io_pipeline {

PipelineStage::PipelineStage(const StageConfig& config,
                            std::shared_ptr<PipelineMetrics> metrics)
    : config_(config),
      metrics_(metrics) {
    try {
        input_buffer_ = std::make_unique<DynamicCircularBuffer>(
            config.input_buffer_capacity,
            config.input_buffer_capacity * 4,
            2.0f
        );
    } catch (const std::exception& e) {
        std::cerr << "Failed to create input buffer for stage " 
                  << config.id << ": " << e.what() << "\n";
    }
}

ErrorCode PipelineStage::push_message(const std::shared_ptr<Message>& msg) {
    if (!input_buffer_ || !msg) {
        return ErrorCode::INVALID_MESSAGE;
    }

    auto result = input_buffer_->push(msg);
    if (result == ErrorCode::SUCCESS && metrics_) {
        metrics_->record_buffer_event(config_.id, input_buffer_->size());
    }
    return result;
}

bool PipelineStage::pop_message(std::shared_ptr<Message>& msg) {
    if (!input_buffer_) {
        return false;
    }
    return input_buffer_->pop(msg);
}

DynamicCircularBuffer::Stats PipelineStage::get_buffer_stats() const {
    if (!input_buffer_) {
        return {};
    }
    return input_buffer_->get_stats();
}

bool PipelineStage::has_pending_messages() const {
    if (!input_buffer_) {
        return false;
    }
    return input_buffer_->size() > 0;
}

std::size_t PipelineStage::pending_message_count() const {
    if (!input_buffer_) {
        return 0;
    }
    return input_buffer_->size();
}

void PipelineStage::record_processing_start(const std::shared_ptr<Message>& msg) {
    if (msg && metrics_) {
        metrics_->record_stage_entry(config_.id, msg);
    }
}

void PipelineStage::record_processing_end(const std::shared_ptr<Message>& msg,
                                         bool error) {
    if (msg && metrics_) {
        metrics_->record_stage_exit(config_.id, msg, error);
    }
}

// DeserializeStage implementation
DeserializeStage::DeserializeStage(const StageConfig& config,
                                   std::shared_ptr<PipelineMetrics> metrics)
    : PipelineStage(config, metrics) {}

ErrorCode DeserializeStage::process(const std::shared_ptr<Message>& msg) {
    if (!msg) {
        return ErrorCode::INVALID_MESSAGE;
    }

    record_processing_start(msg);
    
    try {
        // Simulate deserialization
        msg->state = MessageState::DESERIALIZED;
        record_processing_end(msg);
        return ErrorCode::SUCCESS;
    } catch (...) {
        record_processing_end(msg, true);
        return ErrorCode::SERIALIZATION_ERROR;
    }
}

// DecompressStage implementation
DecompressStage::DecompressStage(const StageConfig& config,
                                std::shared_ptr<PipelineMetrics> metrics)
    : PipelineStage(config, metrics) {}

ErrorCode DecompressStage::process(const std::shared_ptr<Message>& msg) {
    if (!msg) {
        return ErrorCode::INVALID_MESSAGE;
    }

    record_processing_start(msg);
    
    try {
        // Simulate decompression
        msg->state = MessageState::DECOMPRESSED;
        record_processing_end(msg);
        return ErrorCode::SUCCESS;
    } catch (...) {
        record_processing_end(msg, true);
        return ErrorCode::COMPRESSION_ERROR;
    }
}

// DecryptStage implementation
DecryptStage::DecryptStage(const StageConfig& config,
                          std::shared_ptr<PipelineMetrics> metrics)
    : PipelineStage(config, metrics) {}

ErrorCode DecryptStage::process(const std::shared_ptr<Message>& msg) {
    if (!msg) {
        return ErrorCode::INVALID_MESSAGE;
    }

    record_processing_start(msg);
    
    try {
        // Simulate decryption
        msg->state = MessageState::DECRYPTED;
        record_processing_end(msg);
        return ErrorCode::SUCCESS;
    } catch (...) {
        record_processing_end(msg, true);
        return ErrorCode::ENCRYPTION_ERROR;
    }
}

// DispatchStage implementation
DispatchStage::DispatchStage(const StageConfig& config,
                            std::shared_ptr<PipelineMetrics> metrics)
    : PipelineStage(config, metrics) {}

void DispatchStage::register_handler(StageId service_id, ServiceHandler handler) {
    std::lock_guard<std::mutex> lock(handlers_mutex_);
    handlers_[service_id] = handler;
}

void DispatchStage::unregister_handler(StageId service_id) {
    std::lock_guard<std::mutex> lock(handlers_mutex_);
    handlers_.erase(service_id);
}

ErrorCode DispatchStage::process(const std::shared_ptr<Message>& msg) {
    if (!msg) {
        return ErrorCode::INVALID_MESSAGE;
    }

    record_processing_start(msg);
    
    std::lock_guard<std::mutex> lock(handlers_mutex_);
    
    auto it = handlers_.find(msg->target_service);
    if (it == handlers_.end()) {
        record_processing_end(msg, true);
        return ErrorCode::DISPATCH_ERROR;
    }

    try {
        it->second(msg);
        msg->state = MessageState::DISPATCHED;
        record_processing_end(msg);
        return ErrorCode::SUCCESS;
    } catch (...) {
        record_processing_end(msg, true);
        return ErrorCode::DISPATCH_ERROR;
    }
}

// SerializeStage implementation
SerializeStage::SerializeStage(const StageConfig& config,
                              std::shared_ptr<PipelineMetrics> metrics)
    : PipelineStage(config, metrics) {}

ErrorCode SerializeStage::process(const std::shared_ptr<Message>& msg) {
    if (!msg) {
        return ErrorCode::INVALID_MESSAGE;
    }

    record_processing_start(msg);
    
    try {
        // Simulate serialization
        msg->state = MessageState::SERIALIZED;
        record_processing_end(msg);
        return ErrorCode::SUCCESS;
    } catch (...) {
        record_processing_end(msg, true);
        return ErrorCode::SERIALIZATION_ERROR;
    }
}

// CompressStage implementation
CompressStage::CompressStage(const StageConfig& config,
                            std::shared_ptr<PipelineMetrics> metrics)
    : PipelineStage(config, metrics) {}

ErrorCode CompressStage::process(const std::shared_ptr<Message>& msg) {
    if (!msg) {
        return ErrorCode::INVALID_MESSAGE;
    }

    record_processing_start(msg);
    
    try {
        // Simulate compression
        msg->state = MessageState::COMPRESSED;
        record_processing_end(msg);
        return ErrorCode::SUCCESS;
    } catch (...) {
        record_processing_end(msg, true);
        return ErrorCode::COMPRESSION_ERROR;
    }
}

// EncryptStage implementation
EncryptStage::EncryptStage(const StageConfig& config,
                          std::shared_ptr<PipelineMetrics> metrics)
    : PipelineStage(config, metrics) {}

ErrorCode EncryptStage::process(const std::shared_ptr<Message>& msg) {
    if (!msg) {
        return ErrorCode::INVALID_MESSAGE;
    }

    record_processing_start(msg);
    
    try {
        // Simulate encryption
        msg->state = MessageState::ENCRYPTED;
        record_processing_end(msg);
        return ErrorCode::SUCCESS;
    } catch (...) {
        record_processing_end(msg, true);
        return ErrorCode::ENCRYPTION_ERROR;
    }
}

} // namespace io_pipeline