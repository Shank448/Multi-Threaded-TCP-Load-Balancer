// Tiny test backend.
//   --mode line (default): replies "[name] <line>\n" for every line received,
//                          so a client can see WHICH backend served it.
//   --mode raw           : echoes bytes back unchanged (streaming; used to test
//                          full-duplex relaying of large payloads).
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>

#include "net.hpp"

static bool send_all(int fd, const char* p, size_t n) {
    while (n > 0) {
        ssize_t w = ::send(fd, p, n, MSG_NOSIGNAL);
        if (w < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        p += w;
        n -= static_cast<size_t>(w);
    }
    return true;
}

static void serve(int raw_fd, std::string name, bool raw, int delay_ms) {
    net::Fd fd(raw_fd);
    net::set_nodelay(fd.get());
    std::string pending;
    char buf[65536];
    for (;;) {
        ssize_t n = ::recv(fd.get(), buf, sizeof buf, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return;
        if (raw) {
            if (!send_all(fd.get(), buf, static_cast<size_t>(n))) return;
            continue;
        }
        pending.append(buf, static_cast<size_t>(n));
        size_t pos;
        while ((pos = pending.find('\n')) != std::string::npos) {
            std::string reply = "[" + name + "] " + pending.substr(0, pos) + "\n";
            pending.erase(0, pos + 1);
            if (delay_ms > 0) usleep(static_cast<useconds_t>(delay_ms) * 1000);
            if (!send_all(fd.get(), reply.data(), reply.size())) return;
        }
    }
}

int main(int argc, char** argv) {
    int port = 9001, delay_ms = 0;
    std::string name, mode = "line";
    for (int i = 1; i + 1 < argc; i += 2) {
        std::string a = argv[i];
        if (a == "--port") port = std::atoi(argv[i + 1]);
        else if (a == "--name") name = argv[i + 1];
        else if (a == "--delay-ms") delay_ms = std::atoi(argv[i + 1]);
        else if (a == "--mode") mode = argv[i + 1];
        else { fprintf(stderr, "usage: %s [--port N] [--name S] [--delay-ms N] [--mode line|raw]\n", argv[0]); return 2; }
    }
    if (name.empty()) name = "backend-" + std::to_string(port);
    signal(SIGPIPE, SIG_IGN);
    try {
        net::Fd listener(net::create_listener(static_cast<uint16_t>(port)));
        fprintf(stderr, "echo_backend '%s' listening on :%d (%s mode)\n", name.c_str(), port, mode.c_str());
        for (;;) {
            int fd = ::accept(listener.get(), nullptr, nullptr);
            if (fd < 0) continue;
            std::thread(serve, fd, name, mode == "raw", delay_ms).detach();
        }
    } catch (const std::exception& e) {
        fprintf(stderr, "fatal: %s\n", e.what());
        return 1;
    }
}
