#pragma once

#include "pipeline_types.hpp"
#include "circular_buffer.hpp"
#include "metrics.hpp"
#include <functional>
#include <memory>
#include <map>
#include <mutex>

namespace io_pipeline {

// Abstract base for pipeline stage
class PipelineStage {
public:
    explicit PipelineStage(const StageConfig& config,
                          std::shared_ptr<PipelineMetrics> metrics);
    
    virtual ~PipelineStage() = default;

    // Process message - override in derived classes
    virtual ErrorCode process(const std::shared_ptr<Message>& msg) = 0;

    // Get stage ID
    [[nodiscard]] StageId get_id() const { return config_.id; }

    // Get stage name
    [[nodiscard]] const std::string& get_name() const { return config_.name; }

    // Get stage type
    [[nodiscard]] StageType get_type() const { return config_.type; }

    // Try to push message to input buffer
    [[nodiscard]] ErrorCode push_message(const std::shared_ptr<Message>& msg);

    // Pop message from input buffer
    [[nodiscard]] bool pop_message(std::shared_ptr<Message>& msg);

    // Get input buffer stats
    [[nodiscard]] DynamicCircularBuffer::Stats get_buffer_stats() const;

    // Check if stage has pending messages
    [[nodiscard]] bool has_pending_messages() const;

    // Get number of pending messages
    [[nodiscard]] std::size_t pending_message_count() const;

protected:
    StageConfig config_;
    std::shared_ptr<PipelineMetrics> metrics_;
    std::unique_ptr<DynamicCircularBuffer> input_buffer_;

    // Helper for derived classes to record metrics
    void record_processing_start(const std::shared_ptr<Message>& msg);
    void record_processing_end(const std::shared_ptr<Message>& msg, bool error = false);
};

// Concrete stage implementations

class DeserializeStage : public PipelineStage {
public:
    explicit DeserializeStage(const StageConfig& config,
                             std::shared_ptr<PipelineMetrics> metrics);
    
    ErrorCode process(const std::shared_ptr<Message>& msg) override;
};

class DecompressStage : public PipelineStage {
public:
    explicit DecompressStage(const StageConfig& config,
                            std::shared_ptr<PipelineMetrics> metrics);
    
    ErrorCode process(const std::shared_ptr<Message>& msg) override;
};

class DecryptStage : public PipelineStage {
public:
    explicit DecryptStage(const StageConfig& config,
                         std::shared_ptr<PipelineMetrics> metrics);
    
    ErrorCode process(const std::shared_ptr<Message>& msg) override;
};

class DispatchStage : public PipelineStage {
public:
    explicit DispatchStage(const StageConfig& config,
                          std::shared_ptr<PipelineMetrics> metrics);
    
    // Register handler for service
    void register_handler(StageId service_id, ServiceHandler handler);

    // Remove handler
    void unregister_handler(StageId service_id);

    ErrorCode process(const std::shared_ptr<Message>& msg) override;

private:
    std::map<StageId, ServiceHandler> handlers_;
    mutable std::mutex handlers_mutex_;
};

class SerializeStage : public PipelineStage {
public:
    explicit SerializeStage(const StageConfig& config,
                           std::shared_ptr<PipelineMetrics> metrics);
    
    ErrorCode process(const std::shared_ptr<Message>& msg) override;
};

class CompressStage : public PipelineStage {
public:
    explicit CompressStage(const StageConfig& config,
                          std::shared_ptr<PipelineMetrics> metrics);
    
    ErrorCode process(const std::shared_ptr<Message>& msg) override;
};

class EncryptStage : public PipelineStage {
public:
    explicit EncryptStage(const StageConfig& config,
                         std::shared_ptr<PipelineMetrics> metrics);
    
    ErrorCode process(const std::shared_ptr<Message>& msg) override;
};

} // namespace io_pipeline