#pragma once
// Backend server registry + selection strategies.
//
// Locking rules (documented because interviewers ask):
//   * mu_ protects ONLY the selection step (strategy cursor + picking a
//     backend + incrementing its active count) so that two workers can never
//     both observe "backend A has the fewest connections" and pile onto it.
//   * No I/O is ever performed while mu_ is held, and no other lock is taken
//     while holding it => it cannot participate in a deadlock cycle.
//   * Per-backend counters are std::atomic, so stats readers and the health
//     checker never need mu_.

#include <netinet/in.h>
#include <pthread.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "logger.hpp"
#include "net.hpp"

enum class Strategy { RoundRobin, LeastConnections };

inline const char* to_string(Strategy s) {
    return s == Strategy::RoundRobin ? "round-robin" : "least-connections";
}

struct Backend {
    size_t index = 0;
    std::string host;
    uint16_t port = 0;
    sockaddr_in addr{};

    std::atomic<bool> healthy{true};
    std::atomic<int> active{0};
    std::atomic<int> consecutive_failures{0};
    std::atomic<uint64_t> total{0};
    std::atomic<uint64_t> failures{0};
    std::atomic<uint64_t> bytes_up{0};    // client -> backend
    std::atomic<uint64_t> bytes_down{0};  // backend -> client

    std::string name() const { return host + ":" + std::to_string(port); }
};

class BackendPool {
public:
    static constexpr int kPassiveFailureThreshold = 2;

    explicit BackendPool(Strategy strategy) : strategy_(strategy) { pthread_mutex_init(&mu_, nullptr); }
    ~BackendPool() { pthread_mutex_destroy(&mu_); }
    BackendPool(const BackendPool&) = delete;
    BackendPool& operator=(const BackendPool&) = delete;

    // Call before worker threads start; the vector is immutable afterwards.
    void add(const std::string& host, uint16_t port) {
        auto b = std::make_unique<Backend>();
        b->index = backends_.size();
        b->host = host;
        b->port = port;
        b->addr = net::resolve(host, port);
        backends_.push_back(std::move(b));
    }

    // Pick a healthy, non-excluded backend and reserve a connection slot on it.
    // Returns nullptr if none is available. Caller MUST release().
    Backend* acquire(const std::vector<bool>& excluded) {
        const size_t n = backends_.size();
        if (n == 0) return nullptr;

        pthread_mutex_lock(&mu_);
        Backend* best = nullptr;
        const size_t start = cursor_ % n;
        for (size_t i = 0; i < n; ++i) {
            Backend* b = backends_[(start + i) % n].get();
            if (excluded[b->index] || !b->healthy.load()) continue;
            if (strategy_ == Strategy::RoundRobin) {
                best = b;
                break;
            }
            // Least-connections; the rotating start index breaks ties fairly.
            if (!best || b->active.load() < best->active.load()) best = b;
        }
        if (best) {
            cursor_ = (strategy_ == Strategy::RoundRobin ? best->index : start) + 1;
            best->active.fetch_add(1);
            best->total.fetch_add(1);
        }
        pthread_mutex_unlock(&mu_);
        return best;
    }

    void release(Backend* b) { b->active.fetch_sub(1); }

    void report_success(Backend* b) { b->consecutive_failures.store(0); }

    // Passive health check: a few failed connects in a row eject the backend
    // immediately, without waiting for the next active probe.
    void report_failure(Backend* b) {
        b->failures.fetch_add(1);
        if (b->consecutive_failures.fetch_add(1) + 1 >= kPassiveFailureThreshold) mark_down(*b, "connect failures");
    }

    void mark_up(Backend& b) {
        b.consecutive_failures.store(0);
        if (!b.healthy.exchange(true)) LOG_INFO("backend %s is UP", b.name().c_str());
    }

    void mark_down(Backend& b, const char* reason) {
        if (b.healthy.exchange(false)) LOG_WARN("backend %s is DOWN (%s)", b.name().c_str(), reason);
    }

    const std::vector<std::unique_ptr<Backend>>& backends() const { return backends_; }
    size_t size() const { return backends_.size(); }
    Strategy strategy() const { return strategy_; }

private:
    std::vector<std::unique_ptr<Backend>> backends_;
    const Strategy strategy_;
    pthread_mutex_t mu_;
    size_t cursor_ = 0;
};
