// Multi-threaded TCP (layer-4) load balancer.
//
//   clients ──► [accept loop] ──► bounded queue ──► [POSIX worker pool]
//                                                        │  pick backend (RR / least-conn)
//                                                        │  connect with timeout, fail over on error
//                                                        ▼
//                                              poll()-driven bidirectional relay ──► backend
//
//   [health checker thread] probes backends and marks them UP / DOWN.

#include <csignal>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <utility>
#include <vector>

#include "backend.hpp"
#include "health_checker.hpp"
#include "logger.hpp"
#include "net.hpp"
#include "thread_pool.hpp"

namespace {

std::atomic<bool> g_running{true};     // false => stop accepting new connections
std::atomic<bool> g_force_stop{false};  // true  => abort connections still in flight
static_assert(std::atomic<bool>::is_always_lock_free, "must be async-signal-safe");

void on_signal(int) { g_running.store(false); }

struct Config {
    uint16_t port = 8080;
    std::vector<std::pair<std::string, uint16_t>> backends;
    size_t threads = 64;
    size_t queue = 1024;
    Strategy strategy = Strategy::LeastConnections;
    int connect_timeout_ms = 1000;
    int idle_timeout_ms = 30000;
    int health_interval_ms = 2000;
    int stats_interval_s = 10;
    int grace_s = 5;
    bool verbose = false;
};

struct Metrics {
    std::atomic<uint64_t> accepted{0};
    std::atomic<uint64_t> completed{0};
    std::atomic<uint64_t> rejected_overload{0};
    std::atomic<uint64_t> no_backend{0};
    std::atomic<uint64_t> failovers{0};
    std::atomic<uint64_t> bytes_up{0};
    std::atomic<uint64_t> bytes_down{0};
};

void usage(const char* prog) {
    fprintf(stderr,
            "Usage: %s --backend HOST:PORT [--backend HOST:PORT ...] [options]\n"
            "\n"
            "  --port N              listen port                         (default 8080)\n"
            "  --backend HOST:PORT   backend server, repeatable          (required)\n"
            "  --strategy rr|lc      round-robin or least-connections    (default lc)\n"
            "  --threads N           worker threads = max concurrent conns (default 64)\n"
            "  --queue N             pending-connection queue bound      (default 1024)\n"
            "  --connect-timeout MS  backend connect timeout             (default 1000)\n"
            "  --idle-timeout MS     close connections idle this long    (default 30000)\n"
            "  --health-interval MS  active health-check period          (default 2000)\n"
            "  --stats-interval S    print stats every S seconds, 0=off  (default 10)\n"
            "  --grace S             drain time on SIGINT/SIGTERM        (default 5)\n"
            "  -v                    debug logging\n",
            prog);
}

long parse_num(const std::string& s, long lo, long hi, const std::string& flag) {
    size_t pos = 0;
    long v = 0;
    try {
        v = std::stol(s, &pos);
    } catch (...) {
        pos = 0;
    }
    if (pos != s.size() || s.empty() || v < lo || v > hi)
        throw std::invalid_argument(flag + " expects an integer in [" + std::to_string(lo) + ", " +
                                    std::to_string(hi) + "], got '" + s + "'");
    return v;
}

Config parse_args(int argc, char** argv) {
    Config cfg;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 >= argc) throw std::invalid_argument("missing value for " + a);
            return argv[++i];
        };
        if (a == "--port") cfg.port = static_cast<uint16_t>(parse_num(next(), 1, 65535, a));
        else if (a == "--backend") {
            std::string hp = next();
            size_t colon = hp.rfind(':');
            if (colon == std::string::npos || colon == 0) throw std::invalid_argument("--backend expects HOST:PORT");
            cfg.backends.emplace_back(hp.substr(0, colon),
                                      static_cast<uint16_t>(parse_num(hp.substr(colon + 1), 1, 65535, a)));
        } else if (a == "--strategy") {
            std::string s = next();
            if (s == "rr") cfg.strategy = Strategy::RoundRobin;
            else if (s == "lc") cfg.strategy = Strategy::LeastConnections;
            else throw std::invalid_argument("--strategy must be 'rr' or 'lc'");
        } else if (a == "--threads") cfg.threads = static_cast<size_t>(parse_num(next(), 1, 100000, a));
        else if (a == "--queue") cfg.queue = static_cast<size_t>(parse_num(next(), 1, 10000000, a));
        else if (a == "--connect-timeout") cfg.connect_timeout_ms = static_cast<int>(parse_num(next(), 1, 600000, a));
        else if (a == "--idle-timeout") cfg.idle_timeout_ms = static_cast<int>(parse_num(next(), 1, 86400000, a));
        else if (a == "--health-interval") cfg.health_interval_ms = static_cast<int>(parse_num(next(), 50, 600000, a));
        else if (a == "--stats-interval") cfg.stats_interval_s = static_cast<int>(parse_num(next(), 0, 86400, a));
        else if (a == "--grace") cfg.grace_s = static_cast<int>(parse_num(next(), 0, 3600, a));
        else if (a == "-v") cfg.verbose = true;
        else if (a == "-h" || a == "--help") { usage(argv[0]); std::exit(0); }
        else throw std::invalid_argument("unknown option " + a);
    }
    if (cfg.backends.empty()) throw std::invalid_argument("at least one --backend is required");
    return cfg;
}

// Marks a connection finished no matter which path the handler returns on.
struct CompletionGuard {
    Metrics& m;
    ~CompletionGuard() { m.completed.fetch_add(1); }
};

// Runs on a worker thread, one invocation per client connection.
void handle_connection(int client_fd, BackendPool& pool, const Config& cfg, Metrics& m) {
    CompletionGuard guard{m};
    net::Fd client(client_fd);
    if (g_force_stop.load()) return;
    net::set_nodelay(client.get());

    // Pick a backend; if connect fails, exclude it and fail over to the next.
    std::vector<bool> excluded(pool.size(), false);
    Backend* backend = nullptr;
    net::Fd upstream;
    for (size_t attempt = 0; attempt < pool.size(); ++attempt) {
        Backend* cand = pool.acquire(excluded);
        if (!cand) break;
        int fd = net::connect_with_timeout(cand->addr, cfg.connect_timeout_ms);
        if (fd >= 0) {
            backend = cand;
            upstream.reset(fd);
            pool.report_success(cand);
            break;
        }
        LOG_WARN("connect to %s failed (%s), trying next backend", cand->name().c_str(), std::strerror(errno));
        pool.report_failure(cand);
        pool.release(cand);
        excluded[cand->index] = true;
        m.failovers.fetch_add(1);
    }
    if (!backend) {
        m.no_backend.fetch_add(1);
        LOG_ERROR("no healthy backend available; dropping client");
        return;
    }

    net::RelayStats st = net::relay(client.get(), upstream.get(), cfg.idle_timeout_ms, g_force_stop);

    backend->bytes_up.fetch_add(st.client_to_backend);
    backend->bytes_down.fetch_add(st.backend_to_client);
    m.bytes_up.fetch_add(st.client_to_backend);
    m.bytes_down.fetch_add(st.backend_to_client);
    pool.release(backend);
    LOG_DEBUG("done via %s: up=%llu down=%llu (%s)", backend->name().c_str(),
              static_cast<unsigned long long>(st.client_to_backend),
              static_cast<unsigned long long>(st.backend_to_client), st.reason);
}

std::string format_stats(const BackendPool& pool, const Metrics& m, ThreadPool& workers) {
    char line[256];
    std::string out = "--- stats ---\n";
    long long in_flight = static_cast<long long>(m.accepted.load()) - static_cast<long long>(m.completed.load());
    snprintf(line, sizeof line,
             "connections: accepted=%llu in_flight=%lld queued=%zu overload_rejects=%llu no_backend=%llu failovers=%llu\n",
             static_cast<unsigned long long>(m.accepted.load()), in_flight, workers.queued(),
             static_cast<unsigned long long>(m.rejected_overload.load()),
             static_cast<unsigned long long>(m.no_backend.load()), static_cast<unsigned long long>(m.failovers.load()));
    out += line;
    snprintf(line, sizeof line, "traffic: client->backend=%llu B, backend->client=%llu B\n",
             static_cast<unsigned long long>(m.bytes_up.load()), static_cast<unsigned long long>(m.bytes_down.load()));
    out += line;
    snprintf(line, sizeof line, "  %-22s %-5s %6s %9s %6s %12s %12s\n", "BACKEND", "STATE", "ACTIVE", "TOTAL", "FAILS",
             "BYTES_UP", "BYTES_DOWN");
    out += line;
    for (const auto& b : pool.backends()) {
        snprintf(line, sizeof line, "  %-22s %-5s %6d %9llu %6llu %12llu %12llu\n", b->name().c_str(),
                 b->healthy.load() ? "UP" : "DOWN", b->active.load(), static_cast<unsigned long long>(b->total.load()),
                 static_cast<unsigned long long>(b->failures.load()), static_cast<unsigned long long>(b->bytes_up.load()),
                 static_cast<unsigned long long>(b->bytes_down.load()));
        out += line;
    }
    if (!out.empty() && out.back() == '\n') out.pop_back();
    return out;
}

}  // namespace

int main(int argc, char** argv) {
    Config cfg;
    try {
        cfg = parse_args(argc, argv);
    } catch (const std::exception& e) {
        fprintf(stderr, "error: %s\n\n", e.what());
        usage(argv[0]);
        return 2;
    }
    if (cfg.verbose) Logger::instance().set_level(Logger::DEBUG);

    // A peer that disappears mid-write must not kill the whole process.
    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa{};
    sa.sa_handler = on_signal;  // no SA_RESTART: blocking calls return EINTR promptly
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    try {
        BackendPool pool(cfg.strategy);
        for (const auto& [host, port] : cfg.backends) pool.add(host, port);

        net::Fd listener(net::create_listener(cfg.port));
        ThreadPool workers(cfg.threads, cfg.queue);
        HealthChecker health(pool, cfg.health_interval_ms, cfg.connect_timeout_ms);
        health.check_all();
        health.start();
        Metrics metrics;

        LOG_INFO("listening on :%u | %zu backends | strategy=%s | workers=%zu | queue=%zu", cfg.port, pool.size(),
                 to_string(cfg.strategy), cfg.threads, cfg.queue);

        auto last_stats = std::chrono::steady_clock::now();
        while (g_running.load()) {
            pollfd p{listener.get(), POLLIN, 0};
            int n = ::poll(&p, 1, 500);  // timeout lets us notice g_running and print stats
            if (n < 0 && errno != EINTR) {
                LOG_ERROR("poll(listener): %s", std::strerror(errno));
                break;
            }
            if (n > 0 && (p.revents & POLLIN)) {
                sockaddr_in peer{};
                socklen_t len = sizeof peer;
                int fd = ::accept(listener.get(), reinterpret_cast<sockaddr*>(&peer), &len);
                if (fd < 0) {
                    if (errno == EMFILE || errno == ENFILE) {
                        LOG_ERROR("out of file descriptors; backing off");
                        usleep(10 * 1000);
                    }
                    continue;  // EINTR / ECONNABORTED / EAGAIN: just retry
                }
                metrics.accepted.fetch_add(1);
                bool queued = workers.submit([fd, &pool, &cfg, &metrics] { handle_connection(fd, pool, cfg, metrics); });
                if (!queued) {  // saturated: shed load instead of growing the queue forever
                    ::close(fd);
                    metrics.rejected_overload.fetch_add(1);
                    metrics.completed.fetch_add(1);
                    LOG_WARN("queue full, rejecting connection");
                }
            }
            if (cfg.stats_interval_s > 0 &&
                std::chrono::steady_clock::now() - last_stats >= std::chrono::seconds(cfg.stats_interval_s)) {
                LOG_INFO("%s", format_stats(pool, metrics, workers).c_str());
                last_stats = std::chrono::steady_clock::now();
            }
        }

        // ---- graceful shutdown: stop accepting, drain, then force ----
        LOG_INFO("shutting down: no longer accepting, draining in-flight connections (grace %ds)", cfg.grace_s);
        listener.reset();
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(cfg.grace_s);
        while (metrics.accepted.load() > metrics.completed.load() && std::chrono::steady_clock::now() < deadline)
            usleep(20 * 1000);
        g_force_stop.store(true);
        workers.shutdown();  // joins workers; remaining queued tasks exit immediately
        health.stop();
        LOG_INFO("%s", format_stats(pool, metrics, workers).c_str());
        LOG_INFO("bye");
    } catch (const std::exception& e) {
        LOG_ERROR("fatal: %s", e.what());
        return 1;
    }
    return 0;
}
