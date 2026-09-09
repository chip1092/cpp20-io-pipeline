#pragma once

#include "pipeline_types.hpp"
#include <string>
#include <fstream>
#include <memory>
#include <queue>
#include <mutex>
#include <thread>
#include <atomic>
#include <sstream>
#include <map>

namespace io_pipeline {

class ErrorLog {
public:
    struct ErrorEntry {
        Timestamp timestamp;
        ErrorCode code;
        SessionId session_id;
        MessageId message_id;
        StageId stage_id;
        std::string description;
    };

    explicit ErrorLog(const std::string& log_path = "pipeline_errors.log");
    ~ErrorLog();

    // Log error entry
    void log_error(ErrorCode code, SessionId session_id, MessageId message_id,
                   StageId stage_id, const std::string& description);

    // Log warning
    void log_warning(const std::string& message);

    // Log info
    void log_info(const std::string& message);

    // Flush pending logs
    void flush();

    // Get error count for session
    [[nodiscard]] std::size_t get_session_error_count(SessionId session_id) const;

    // Get last error for session
    [[nodiscard]] ErrorEntry get_last_error(SessionId session_id) const;

    // Check if should rate limit session
    [[nodiscard]] bool should_rate_limit_session(SessionId session_id, 
                                                  std::size_t max_errors_per_minute = 100);

private:
    std::string log_path_;
    std::ofstream log_file_;
    std::queue<ErrorEntry> pending_logs_;
    mutable std::mutex log_mutex_;
    std::thread flush_thread_;
    std::atomic<bool> running_{true};
    
    std::map<SessionId, std::vector<Timestamp>> session_error_timestamps_;
    
    void flush_worker();
    [[nodiscard]] std::string error_code_to_string(ErrorCode code) const;
};

// Error recovery handler
class ErrorRecoveryHandler {
public:
    explicit ErrorRecoveryHandler(std::size_t max_memory_bytes);

    // Handle buffer full error - attempts recovery
    [[nodiscard]] ErrorCode handle_buffer_full(
        const std::shared_ptr<Message>& msg,
        std::size_t& current_buffer_size,
        std::size_t max_buffer_size
    );

    // Handle out of memory error
    [[nodiscard]] ErrorCode handle_out_of_memory(
        std::size_t required_bytes,
        std::size_t available_bytes
    );

    // Trigger session rate limiting
    void rate_limit_session(SessionId session_id, std::chrono::milliseconds duration);

    // Check if session is rate limited
    [[nodiscard]] bool is_session_rate_limited(SessionId session_id) const;

    // Get current memory usage
    [[nodiscard]] std::size_t get_memory_usage() const;

    // Attempt to free memory
    [[nodiscard]] bool try_free_memory(std::size_t bytes);

private:
    std::size_t max_memory_bytes_;
    std::atomic<std::size_t> current_memory_usage_{0};
    
    std::map<SessionId, Timestamp> rate_limited_sessions_;
    mutable std::mutex rate_limit_mutex_;
};

} // namespace io_pipeline