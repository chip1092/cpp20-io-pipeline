#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <chrono>
#include <atomic>
#include <vector>
#include <functional>
#include <span>

namespace io_pipeline {

// Message types and identifiers
using MessageId = std::uint64_t;
using SessionId = std::uint32_t;
using StageId = std::uint8_t;

// Metrics types
using Timestamp = std::chrono::high_resolution_clock::time_point;
using Duration = std::chrono::nanoseconds;

// Forward declarations
struct Message;
struct PipelineConfig;
class PipelineMetrics;
class CircularBuffer;

// Message wrapper - sessions deliver messages wrapped in this struct
struct Message {
    MessageId id;                    // Unique message identifier
    SessionId session_id;            // Source session
    Timestamp arrival_time;          // When message entered pipeline
    void* data;                      // Pointer to actual message payload
    std::size_t size;                // Message size
    std::uint8_t priority;           // Priority level (0-255)
    bool is_input;                   // Input (network->service) or output (service->network)
    
    // Pipeline processing state
    mutable std::atomic<StageId> current_stage{0};
    mutable std::atomic<bool> error_flag{false};
    std::string error_message;
    
    // Track which stages have processed this message
    mutable std::uint64_t stage_completion_mask{0};
};

// Handler signature for service dispatch
using ServiceHandler = std::function<void(const std::shared_ptr<Message>&)>;

// Stage types
enum class StageType : std::uint8_t {
    DESERIALIZE = 0,
    DECOMPRESS = 1,
    DECRYPT = 2,
    DISPATCH = 3,
    SERIALIZE = 4,
    COMPRESS = 5,
    ENCRYPT = 6
};

// Pipeline error codes
enum class ErrorCode : std::uint32_t {
    SUCCESS = 0,
    BUFFER_FULL = 1,
    OUT_OF_MEMORY = 2,
    INVALID_MESSAGE = 3,
    STAGE_PROCESSING_ERROR = 4,
    DISPATCH_ERROR = 5,
    SERIALIZATION_ERROR = 6,
    COMPRESSION_ERROR = 7,
    ENCRYPTION_ERROR = 8,
    TIMEOUT = 9,
    SESSION_RATE_LIMITED = 10,
    UNKNOWN_ERROR = 255
};

// Stage configuration
struct StageConfig {
    StageType type;
    StageId id;
    std::string name;
    std::size_t buffer_capacity;      // Messages in buffer
    std::size_t min_buffer_size;      // Minimum before triggering growth
    bool enable_metrics;
};

// Pipeline configuration
struct PipelineConfig {
    std::size_t num_worker_threads;           // Thread pool size
    std::vector<StageConfig> stages;
    std::size_t initial_buffer_capacity;      // Per-stage buffer size
    std::size_t max_buffer_capacity;          // Hard limit per stage
    std::chrono::milliseconds stage_timeout;  // Processing timeout
    bool enable_metrics;
    bool enable_persistent_logging;
    std::string log_path;
    std::size_t max_memory_bytes;             // System memory limit
    float buffer_growth_factor;               // Multiplier for dynamic growth
    float throughput_target;                  // Messages/sec target
    float latency_target_us;                  // Target latency in microseconds
};

} // namespace io_pipeline