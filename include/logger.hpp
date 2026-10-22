#pragma once
// Minimal thread-safe logger. One mutex serialises writes so lines from
// different worker threads never interleave.

#include <pthread.h>

#include <atomic>
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <vector>

class Logger {
public:
    enum Level { DEBUG = 0, INFO = 1, WARN = 2, ERROR = 3 };

    static Logger& instance() {
        static Logger logger;
        return logger;
    }

    void set_level(Level level) { level_.store(level); }

    void log(Level level, const char* fmt, ...) __attribute__((format(printf, 3, 4))) {
        if (level < level_.load()) return;

        // Format outside the lock: keep the critical section to a single fprintf.
        va_list ap, ap2;
        va_start(ap, fmt);
        va_copy(ap2, ap);
        int len = vsnprintf(nullptr, 0, fmt, ap);
        va_end(ap);
        std::vector<char> msg(len > 0 ? static_cast<size_t>(len) + 1 : 1);
        if (len > 0) vsnprintf(msg.data(), msg.size(), fmt, ap2);
        va_end(ap2);

        timespec ts{};
        clock_gettime(CLOCK_REALTIME, &ts);
        tm tm_buf{};
        localtime_r(&ts.tv_sec, &tm_buf);
        char stamp[16];
        strftime(stamp, sizeof stamp, "%H:%M:%S", &tm_buf);

        static const char* const names[] = {"DEBUG", "INFO ", "WARN ", "ERROR"};
        pthread_mutex_lock(&mu_);
        fprintf(stderr, "%s.%03ld [%s] %s\n", stamp, ts.tv_nsec / 1000000, names[level], msg.data());
        pthread_mutex_unlock(&mu_);
    }

private:
    Logger() { pthread_mutex_init(&mu_, nullptr); }
    ~Logger() { pthread_mutex_destroy(&mu_); }
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    pthread_mutex_t mu_;
    std::atomic<int> level_{INFO};
};

#define LOG_DEBUG(...) Logger::instance().log(Logger::DEBUG, __VA_ARGS__)
#define LOG_INFO(...)  Logger::instance().log(Logger::INFO, __VA_ARGS__)
#define LOG_WARN(...)  Logger::instance().log(Logger::WARN, __VA_ARGS__)
#define LOG_ERROR(...) Logger::instance().log(Logger::ERROR, __VA_ARGS__)
