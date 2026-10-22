// Load generator: N client threads, each issuing M short request/response
// exchanges (one fresh TCP connection per request, like HTTP/1.0). Reports
// throughput, latency percentiles and the per-backend distribution.
#include <algorithm>
#include <csignal>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "net.hpp"

struct Result {
    std::vector<double> latency_ms;
    std::map<std::string, long> by_backend;
    long failed = 0;
};

static void worker(const sockaddr_in& addr, int tid, int requests, size_t payload, Result& out) {
    using clock = std::chrono::steady_clock;
    const std::string body(payload, 'x');
    for (int i = 0; i < requests; ++i) {
        auto t0 = clock::now();
        net::Fd fd(net::connect_with_timeout(addr, 3000));
        bool ok = false;
        std::string reply;
        if (fd) {
            std::string msg = "req-" + std::to_string(tid) + "-" + std::to_string(i) + " " + body + "\n";
            ok = ::send(fd.get(), msg.data(), msg.size(), MSG_NOSIGNAL) == static_cast<ssize_t>(msg.size());
            char buf[4096];
            while (ok && reply.find('\n') == std::string::npos) {
                ssize_t n = ::recv(fd.get(), buf, sizeof buf, 0);
                if (n <= 0) { ok = false; break; }
                reply.append(buf, static_cast<size_t>(n));
            }
        }
        double ms = std::chrono::duration<double, std::milli>(clock::now() - t0).count();
        size_t close = reply.find(']');
        if (ok && reply.size() > 1 && reply[0] == '[' && close != std::string::npos) {
            out.latency_ms.push_back(ms);
            out.by_backend[reply.substr(1, close - 1)]++;
        } else {
            out.failed++;
        }
    }
}

int main(int argc, char** argv) {
    std::string host = "127.0.0.1";
    int port = 8080, clients = 50, requests = 100;
    size_t payload = 64;
    for (int i = 1; i + 1 < argc; i += 2) {
        std::string a = argv[i];
        if (a == "--host") host = argv[i + 1];
        else if (a == "--port") port = std::atoi(argv[i + 1]);
        else if (a == "--clients") clients = std::atoi(argv[i + 1]);
        else if (a == "--requests") requests = std::atoi(argv[i + 1]);
        else if (a == "--payload") payload = static_cast<size_t>(std::atoi(argv[i + 1]));
        else { fprintf(stderr, "usage: %s [--host H] [--port N] [--clients N] [--requests N] [--payload BYTES]\n", argv[0]); return 2; }
    }
    signal(SIGPIPE, SIG_IGN);
    sockaddr_in addr{};
    try { addr = net::resolve(host, static_cast<uint16_t>(port)); }
    catch (const std::exception& e) { fprintf(stderr, "%s\n", e.what()); return 1; }

    std::vector<Result> results(static_cast<size_t>(clients));
    std::vector<std::thread> threads;
    auto t0 = std::chrono::steady_clock::now();
    for (int t = 0; t < clients; ++t)
        threads.emplace_back(worker, std::cref(addr), t, requests, payload, std::ref(results[static_cast<size_t>(t)]));
    for (auto& th : threads) th.join();
    double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();

    std::vector<double> lat;
    std::map<std::string, long> dist;
    long failed = 0;
    for (auto& r : results) {
        lat.insert(lat.end(), r.latency_ms.begin(), r.latency_ms.end());
        for (auto& kv : r.by_backend) dist[kv.first] += kv.second;
        failed += r.failed;
    }
    std::sort(lat.begin(), lat.end());
    auto pct = [&](double p) { return lat.empty() ? 0.0 : lat[std::min(lat.size() - 1, static_cast<size_t>(p * static_cast<double>(lat.size())))]; };

    printf("clients=%d requests/client=%d payload=%zuB\n", clients, requests, payload);
    printf("ok=%zu failed=%ld duration=%.2fs throughput=%.0f req/s\n", lat.size(), failed, secs, static_cast<double>(lat.size()) / secs);
    printf("latency ms: p50=%.2f p95=%.2f p99=%.2f max=%.2f\n", pct(0.50), pct(0.95), pct(0.99), lat.empty() ? 0.0 : lat.back());
    printf("distribution:\n");
    for (auto& kv : dist)
        printf("  %-16s %7ld  (%.1f%%)\n", kv.first.c_str(), kv.second, 100.0 * static_cast<double>(kv.second) / static_cast<double>(lat.size()));
    return failed == 0 ? 0 : 1;
}
