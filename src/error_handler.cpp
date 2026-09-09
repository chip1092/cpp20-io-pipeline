#include "error_handler.hpp"
#include <chrono>
#include <thread>
#include <iostream>

namespace io_pipeline {

ErrorLog::ErrorLog(const std::string& log_path)
    : log_path_(log_path),
      log_file_(log_path, std::ios::app) {
    
    if (!log_file_.is_open()) {
        std::cerr << "Failed to open error log file: " << log_path << "\n";
    }

    flush_thread_ = std::thread(&ErrorLog::flush_worker, this);
}

ErrorLog::~ErrorLog() {
    running_.store(false, std::memory_order_release);
    if (flush_thread_.joinable()) {
        flush_thread_.join();
    }
    flush();
}

void ErrorLog::log_error(ErrorCode code, SessionId session_id, MessageId message_id,
                        StageId stage_id, const std::string& description) {
    ErrorEntry entry{
        .timestamp = std::chrono::high_resolution_clock::now(),
        .code = code,
        .session_id = session_id,
        .message_id = message_id,
        .stage_id = stage_id,
        .description = description
    };

    {
        std::lock_guard<std::mutex> lock(log_mutex_);
        pending_logs_.push(entry);
        session_error_timestamps_[session_id].push_back(entry.timestamp);
    }
}

void ErrorLog::log_warning(const std::string& message) {
    std::lock_guard<std::mutex> lock(log_mutex_);
    auto now = std::chrono::high_resolution_clock::now();
    
    log_file_ << "[WARNING] " << std::chrono::system_clock::now().time_since_epoch().count()
              << " - " << message << "\n";
}

void ErrorLog::log_info(const std::string& message) {
    std::lock_guard<std::mutex> lock(log_mutex_);
    auto now = std::chrono::high_resolution_clock::now();
    
    log_file_ << "[INFO] " << std::chrono::system_clock::now().time_since_epoch().count()
              << " - " << message << "\n";
}

void ErrorLog::flush() {
    std::lock_guard<std::mutex> lock(log_mutex_);
    
    while (!pending_logs_.empty()) {
        const auto& entry = pending_logs_.front();
        
        log_file_ << "[ERROR] "
                  << "Session=" << entry.session_id
                  << " Message=" << entry.message_id
                  << " Stage=" << static_cast<int>(entry.stage_id)
                  << " Code=" << static_cast<int>(entry.code)
                  << " - " << entry.description << "\n";
        
        pending_logs_.pop();
    }
    
    log_file_.flush();
}

void ErrorLog::flush_worker() {
    while (running_.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        flush();
    }
}

std::size_t ErrorLog::get_session_error_count(SessionId session_id) const {
    std::lock_guard<std::mutex> lock(log_mutex_);
    auto it = session_error_timestamps_.find(session_id);
    return it != session_error_timestamps_.end() ? it->second.size() : 0;
}

ErrorLog::ErrorEntry ErrorLog::get_last_error(SessionId session_id) const {
    std::lock_guard<std::mutex> lock(log_mutex_);
    static ErrorEntry empty{};
    
    auto it = session_error_timestamps_.find(session_id);
    if (it == session_error_timestamps_.end() || it->second.empty()) {
        return empty;
    }
    
    return empty;
}

bool ErrorLog::should_rate_limit_session(SessionId session_id, std::size_t max_errors_per_minute) {
    std::lock_guard<std::mutex> lock(log_mutex_);
    
    auto it = session_error_timestamps_.find(session_id);
    if (it == session_error_timestamps_.end()) {
        return false;
    }

    auto now = std::chrono::high_resolution_clock::now();
    auto one_minute_ago = now - std::chrono::minutes(1);

    std::size_t recent_errors = 0;
    for (const auto& ts : it->second) {
        if (ts > one_minute_ago) {
            recent_errors++;
        }
    }

    return recent_errors > max_errors_per_minute;
}

std::string ErrorLog::error_code_to_string(ErrorCode code) const {
    switch (code) {
        case ErrorCode::SUCCESS: return "SUCCESS";
        case ErrorCode::BUFFER_FULL: return "BUFFER_FULL";
        case ErrorCode::OUT_OF_MEMORY: return "OUT_OF_MEMORY";
        case ErrorCode::INVALID_MESSAGE: return "INVALID_MESSAGE";
        case ErrorCode::STAGE_PROCESSING_ERROR: return "STAGE_PROCESSING_ERROR";
        case ErrorCode::DISPATCH_ERROR: return "DISPATCH_ERROR";
        case ErrorCode::SERIALIZATION_ERROR: return "SERIALIZATION_ERROR";
        case ErrorCode::COMPRESSION_ERROR: return "COMPRESSION_ERROR";
        case ErrorCode::ENCRYPTION_ERROR: return "ENCRYPTION_ERROR";
        case ErrorCode::TIMEOUT: return "TIMEOUT";
        case ErrorCode::SESSION_RATE_LIMITED: return "SESSION_RATE_LIMITED";
        default: return "UNKNOWN_ERROR";
    }
}

ErrorRecoveryHandler::ErrorRecoveryHandler(std::size_t max_memory_bytes)
    : max_memory_bytes_(max_memory_bytes) {}

ErrorCode ErrorRecoveryHandler::handle_buffer_full(
    const std::shared_ptr<Message>& msg,
    std::size_t& current_buffer_size,
    std::size_t max_buffer_size) {
    
    if (current_buffer_size < max_buffer_size) {
        std::size_t growth = std::min(
            current_buffer_size,
            max_buffer_size - current_buffer_size
        );
        current_buffer_size += growth;
        return ErrorCode::SUCCESS;
    }

    auto memory_usage = current_memory_usage_.load(std::memory_order_acquire);
    if (memory_usage < max_memory_bytes_) {
        auto available = max_memory_bytes_ - memory_usage;
        if (available > sizeof(Message)) {
            return ErrorCode::SUCCESS;
        }
    }

    rate_limit_session(msg->session_id, std::chrono::milliseconds(100));
    return ErrorCode::SESSION_RATE_LIMITED;
}

ErrorCode ErrorRecoveryHandler::handle_out_of_memory(
    std::size_t required_bytes,
    std::size_t available_bytes) {
    
    if (available_bytes >= required_bytes) {
        return ErrorCode::SUCCESS;
    }

    if (!try_free_memory(required_bytes)) {
        return ErrorCode::OUT_OF_MEMORY;
    }

    return ErrorCode::SUCCESS;
}

void ErrorRecoveryHandler::rate_limit_session(SessionId session_id, 
                                              std::chrono::milliseconds duration) {
    std::lock_guard<std::mutex> lock(rate_limit_mutex_);
    rate_limited_sessions_[session_id] = 
        std::chrono::high_resolution_clock::now() + duration;
}

bool ErrorRecoveryHandler::is_session_rate_limited(SessionId session_id) const {
    std::lock_guard<std::mutex> lock(rate_limit_mutex_);
    
    auto it = rate_limited_sessions_.find(session_id);
    if (it == rate_limited_sessions_.end()) {
        return false;
    }

    auto now = std::chrono::high_resolution_clock::now();
    if (now > it->second) {
        rate_limited_sessions_.erase(it);
        return false;
    }

    return true;
}

std::size_t ErrorRecoveryHandler::get_memory_usage() const {
    return current_memory_usage_.load(std::memory_order_acquire);
}

bool ErrorRecoveryHandler::try_free_memory(std::size_t bytes) {
    return current_memory_usage_.load(std::memory_order_acquire) + bytes 
           <= max_memory_bytes_;
}

} // namespace io_pipeline