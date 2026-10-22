#pragma once
// Thin wrappers around BSD/POSIX sockets: RAII file descriptors, listener and
// connect helpers, and the non-blocking bidirectional relay that moves bytes
// between a client and a backend.

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0  // macOS: SIGPIPE is ignored process-wide in main()
#endif

namespace net {

// Owns a file descriptor and closes it exactly once (no leaks on early return).
class Fd {
public:
    Fd() = default;
    explicit Fd(int fd) : fd_(fd) {}
    ~Fd() { reset(); }
    Fd(const Fd&) = delete;
    Fd& operator=(const Fd&) = delete;
    Fd(Fd&& o) noexcept : fd_(o.release()) {}
    Fd& operator=(Fd&& o) noexcept {
        if (this != &o) reset(o.release());
        return *this;
    }
    int get() const { return fd_; }
    int release() {
        int f = fd_;
        fd_ = -1;
        return f;
    }
    void reset(int nfd = -1) {
        if (fd_ >= 0) ::close(fd_);
        fd_ = nfd;
    }
    explicit operator bool() const { return fd_ >= 0; }

private:
    int fd_ = -1;
};

inline bool set_nonblocking(int fd, bool on) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return false;
    flags = on ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK);
    return fcntl(fd, F_SETFL, flags) == 0;
}

// Disable Nagle: a proxy must not add 40ms delays to small request/response messages.
inline void set_nodelay(int fd) {
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
}

// Resolve once at start-up so the hot path never touches DNS.
inline sockaddr_in resolve(const std::string& host, uint16_t port) {
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res = nullptr;
    int rc = getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &res);
    if (rc != 0 || res == nullptr)
        throw std::runtime_error("cannot resolve '" + host + "': " + gai_strerror(rc));
    sockaddr_in addr{};
    std::memcpy(&addr, res->ai_addr, sizeof addr);
    freeaddrinfo(res);
    return addr;
}

inline int create_listener(uint16_t port, int backlog = 1024) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error(std::string("socket: ") + std::strerror(errno));
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof addr) < 0 || ::listen(fd, backlog) < 0) {
        std::string err = std::string("bind/listen on port ") + std::to_string(port) + ": " + std::strerror(errno);
        ::close(fd);
        throw std::runtime_error(err);
    }
    return fd;
}

// Non-blocking connect() bounded by poll(). Returns a blocking fd, or -1.
inline int connect_with_timeout(const sockaddr_in& addr, int timeout_ms) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    set_nonblocking(fd, true);
    int rc = ::connect(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof addr);
    if (rc < 0) {
        if (errno != EINPROGRESS) {
            ::close(fd);
            return -1;
        }
        pollfd p{fd, POLLOUT, 0};
        int n;
        do {
            n = ::poll(&p, 1, timeout_ms);
        } while (n < 0 && errno == EINTR);
        int err = 0;
        socklen_t len = sizeof err;
        if (n > 0 && getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) err = errno;
        if (n <= 0 || err != 0) {
            // Capture the real cause BEFORE close() can clobber errno.
            const int cause = (n == 0) ? ETIMEDOUT : (n < 0 ? errno : err);
            ::close(fd);
            errno = cause;
            return -1;
        }
    }
    set_nonblocking(fd, false);
    set_nodelay(fd);
    return fd;
}

// ---------------------------------------------------------------------------
// Relay
//
// One direction of a proxied connection. Each Pipe owns a small buffer and is
// driven by poll(): we only read when the buffer is empty and only write when
// it is not, so a slow receiver applies back-pressure to its sender instead of
// making us buffer without bound. Because both directions are serviced from
// ONE poll() loop with non-blocking I/O, neither direction can block the
// other => the classic "both sides blocked in send()" deadlock cannot occur.
// ---------------------------------------------------------------------------
struct Pipe {
    int from;
    int to;
    char buf[16 * 1024];
    size_t head = 0, tail = 0;
    bool eof = false;   // peer closed its write side
    bool done = false;  // EOF has been forwarded with shutdown(to, SHUT_WR)
    uint64_t bytes = 0;

    Pipe(int f, int t) : from(f), to(t) {}

    bool has_data() const { return tail > head; }
    bool wants_read() const { return !eof && !has_data(); }

    // false => hard error, tear the connection down
    bool read_some() {
        if (!wants_read()) return true;
        ssize_t n = ::recv(from, buf, sizeof buf, 0);
        if (n > 0) {
            head = 0;
            tail = static_cast<size_t>(n);
            return true;
        }
        if (n == 0) {
            eof = true;
            return true;
        }
        return errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR;
    }

    bool write_some() {
        while (has_data()) {
            ssize_t n = ::send(to, buf + head, tail - head, MSG_NOSIGNAL);
            if (n > 0) {
                head += static_cast<size_t>(n);
                bytes += static_cast<uint64_t>(n);
                continue;
            }
            if (n < 0 && errno == EINTR) continue;
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return true;
            return false;
        }
        if (eof && !done) {  // propagate half-close so request/response protocols finish cleanly
            ::shutdown(to, SHUT_WR);
            done = true;
        }
        return true;
    }
};

struct RelayStats {
    uint64_t client_to_backend = 0;
    uint64_t backend_to_client = 0;
    bool clean = false;
    const char* reason = "";
};

inline RelayStats relay(int client, int backend, int idle_timeout_ms, const std::atomic<bool>& stop) {
    constexpr int kSliceMs = 500;  // also how often we notice `stop`
    set_nonblocking(client, true);
    set_nonblocking(backend, true);

    Pipe up(client, backend);
    Pipe down(backend, client);
    RelayStats st;
    int idle_ms = 0;

    for (;;) {
        if (up.done && down.done) {
            st.clean = true;
            st.reason = "complete";
            break;
        }
        if (stop.load()) {
            st.reason = "shutdown";
            break;
        }

        pollfd fds[2] = {{client, 0, 0}, {backend, 0, 0}};
        if (up.wants_read()) fds[0].events |= POLLIN;
        if (down.wants_read()) fds[1].events |= POLLIN;
        if (up.has_data()) fds[1].events |= POLLOUT;
        if (down.has_data()) fds[0].events |= POLLOUT;

        int n = ::poll(fds, 2, kSliceMs);
        if (n < 0) {
            if (errno == EINTR) continue;
            st.reason = "poll-error";
            break;
        }
        if (n == 0) {
            idle_ms += kSliceMs;
            if (idle_ms >= idle_timeout_ms) {
                st.reason = "idle-timeout";
                break;
            }
            continue;
        }
        idle_ms = 0;

        if ((fds[0].revents | fds[1].revents) & (POLLERR | POLLNVAL)) {
            st.reason = "socket-error";
            break;
        }

        bool ok = true;
        if (fds[0].revents & (POLLIN | POLLHUP)) ok = up.read_some();
        if (ok && (fds[1].revents & (POLLIN | POLLHUP))) ok = down.read_some();
        if (ok) ok = up.write_some();    // also covers the POLLOUT case
        if (ok) ok = down.write_some();
        if (!ok) {
            st.reason = "io-error";
            break;
        }
    }
    st.client_to_backend = up.bytes;
    st.backend_to_client = down.bytes;
    return st;
}

}  // namespace net
