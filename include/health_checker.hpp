#pragma once
// Active health checking: a dedicated thread periodically opens a TCP
// connection to every backend. Success => UP, failure => DOWN. Combined with
// the passive checks in BackendPool this gives fast ejection and automatic
// re-admission of recovered servers.

#include <pthread.h>
#include <unistd.h>

#include <atomic>

#include "backend.hpp"
#include "net.hpp"

class HealthChecker {
public:
    HealthChecker(BackendPool& pool, int interval_ms, int timeout_ms)
        : pool_(pool), interval_ms_(interval_ms), timeout_ms_(timeout_ms) {}

    ~HealthChecker() { stop(); }
    HealthChecker(const HealthChecker&) = delete;
    HealthChecker& operator=(const HealthChecker&) = delete;

    // One synchronous sweep (used at start-up so the first connection already
    // sees an accurate view of the world).
    void check_all() {
        for (const auto& b : pool_.backends()) {
            net::Fd fd(net::connect_with_timeout(b->addr, timeout_ms_));
            if (fd)
                pool_.mark_up(*b);
            else
                pool_.mark_down(*b, "health probe failed");
        }
    }

    void start() {
        if (running_) return;
        stop_.store(false);
        running_ = pthread_create(&thread_, nullptr, &HealthChecker::entry, this) == 0;
    }

    void stop() {
        if (!running_) return;
        stop_.store(true);
        pthread_join(thread_, nullptr);
        running_ = false;
    }

private:
    static void* entry(void* self) {
        static_cast<HealthChecker*>(self)->loop();
        return nullptr;
    }

    void loop() {
        while (!stop_.load()) {
            // Sleep in short slices so stop() returns promptly.
            for (int slept = 0; slept < interval_ms_ && !stop_.load(); slept += 50) usleep(50 * 1000);
            if (stop_.load()) break;
            check_all();
        }
    }

    BackendPool& pool_;
    const int interval_ms_;
    const int timeout_ms_;
    pthread_t thread_{};
    bool running_ = false;
    std::atomic<bool> stop_{false};
};
