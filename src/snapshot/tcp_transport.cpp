#include "snapshot/tcp_transport.h"
#include "snapshot/codec.h"
#include "checkpoint/crc32c.h"
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <system_error>

namespace stormglass::snapshot {
namespace {
[[noreturn]] void Error(const char *what) {
    throw std::system_error(errno, std::generic_category(), what);
}
uint32_t Checksum(const Bytes &b) { return Crc32c(b.data(), b.size()); }
Bytes Encode(const Message &m) {
    Encoder e;
    e.U32(0x53474331);
    e.U8(static_cast<uint8_t>(m.kind));
    e.U64(m.sequence);
    e.U64(m.epoch);
    e.Blob(m.payload);
    if (e.bytes.size() + 4 > TcpTransport::kMaxFrame)
        throw std::runtime_error("TCP frame exceeds limit");
    e.U32(Checksum(e.bytes));
    Encoder frame;
    frame.U32(static_cast<uint32_t>(e.bytes.size()));
    frame.bytes.insert(frame.bytes.end(), e.bytes.begin(), e.bytes.end());
    return std::move(frame.bytes);
}
Message Decode(const Bytes &frame) {
    if (frame.size() < 4)
        throw std::runtime_error("short TCP frame");
    Decoder trailer(std::span(frame).last(4));
    if (Crc32c(frame.data(), frame.size() - 4) != trailer.U32())
        throw std::runtime_error("TCP frame CRC mismatch");
    Decoder d(std::span(frame).first(frame.size() - 4));
    if (d.U32() != 0x53474331)
        throw std::runtime_error("TCP protocol version mismatch");
    Message m;
    m.kind = static_cast<Message::Kind>(d.U8());
    m.sequence = d.U64();
    m.epoch = d.U64();
    m.payload = d.Blob();
    d.End();
    if (m.kind != Message::Kind::Data && m.kind != Message::Kind::Marker)
        throw std::runtime_error("invalid TCP message kind");
    return m;
}
int NewSocket() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        Error("socket");
    return fd;
}
sockaddr_in Address(const char *address, uint16_t port) {
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(port);
    if (::inet_pton(AF_INET, address, &a.sin_addr) != 1)
        throw std::invalid_argument("expected numeric IPv4 address");
    return a;
}
} // namespace
TcpTransport::TcpTransport(std::vector<PeerSocket> sockets, std::size_t limit)
    : queue_limit_(limit) {
    // Adopt every fd first so errors release even as-yet-unconfigured sockets.
    try {
        if (!limit)
            throw std::invalid_argument("zero TCP queue limit");
        std::set<int> adopted;
        for (auto s : sockets) {
            if (s.fd < 0 || channels_.contains(s.peer) || !adopted.insert(s.fd).second)
                throw std::invalid_argument("invalid/duplicate TCP peer");
            channels_.emplace(s.peer, Channel{s.fd, {}, {}, 0, 0});
        }
        for (auto &[peer, c] : channels_) {
            (void)peer;
            int flags = ::fcntl(c.fd, F_GETFL, 0);
            if (flags < 0 || ::fcntl(c.fd, F_SETFL, flags | O_NONBLOCK) < 0)
                Error("nonblocking socket");
#ifdef SO_NOSIGPIPE
            int yes = 1;
            if (::setsockopt(c.fd, SOL_SOCKET, SO_NOSIGPIPE, &yes, sizeof(yes)) < 0)
                Error("SO_NOSIGPIPE");
#endif
        }
    } catch (...) {
        // sockets may contain a duplicate fd; close each descriptor only once.
        std::set<int> closed;
        for (auto s : sockets)
            if (s.fd >= 0 && closed.insert(s.fd).second)
                ::close(s.fd);
        throw;
    }
}
TcpTransport::~TcpTransport() {
    for (auto &[peer, c] : channels_) {
        (void)peer;
        ::close(c.fd);
    }
}
void TcpTransport::Queue(NodeId peer, const Message &m) {
    if (failed_)
        throw std::runtime_error("TCP transport failed");
    try {
        auto frame = Encode(m);
        auto &c = channels_.at(peer);
        if (frame.size() > queue_limit_ - c.queued_bytes)
            throw std::runtime_error("TCP output queue limit exceeded");
        c.queued_bytes += frame.size();
        c.output.push_back(std::move(frame));
    } catch (...) {
        failed_ = true;
        throw;
    }
}
bool TcpTransport::Drained() const {
    for (auto &[peer, c] : channels_) {
        (void)peer;
        if (!c.output.empty())
            return false;
    }
    return true;
}
void TcpTransport::PollOnce(const std::function<void(NodeId, const Message &)> &receive,
                            int timeout_ms) {
    if (failed_)
        throw std::runtime_error("TCP transport failed");
    try {
        if (timeout_ms < 0)
            throw std::invalid_argument("TCP poll requires bounded timeout");
        std::vector<pollfd> fds;
        for (auto &[peer, c] : channels_) {
            (void)peer;
            fds.push_back({c.fd, static_cast<short>(POLLIN | (c.output.empty() ? 0 : POLLOUT)), 0});
        }
        int ready;
        do {
            ready = ::poll(fds.data(), fds.size(), timeout_ms);
        } while (ready < 0 && errno == EINTR);
        if (ready < 0)
            Error("poll");
        std::size_t index = 0;
        for (auto &[peer, c] : channels_) {
            auto events = fds[index++].revents;
            if (events & POLLNVAL)
                throw std::runtime_error("invalid TCP descriptor");
            if (events & POLLOUT) {
                std::size_t budget = 256 * 1024;
                while (!c.output.empty() && budget) {
                    auto &front = c.output.front();
#ifdef MSG_NOSIGNAL
                    constexpr int flags = MSG_NOSIGNAL;
#else
                    constexpr int flags = 0;
#endif
                    auto n = ::send(c.fd, front.data() + c.front_offset,
                                    std::min(budget, front.size() - c.front_offset), flags);
                    if (n < 0) {
                        if (errno == EINTR)
                            continue;
                        if (errno == EAGAIN || errno == EWOULDBLOCK)
                            break;
                        Error("send");
                    }
                    if (n == 0)
                        throw std::runtime_error("zero TCP send");
                    c.front_offset += n;
                    c.queued_bytes -= n;
                    budget -= n;
                    if (c.front_offset == front.size()) {
                        c.output.pop_front();
                        c.front_offset = 0;
                    }
                }
            }
            if (events & (POLLIN | POLLHUP | POLLERR)) {
                std::array<uint8_t, 16384> buffer{};
                for (int reads = 0; reads < 1; ++reads) {
                    auto n = ::recv(c.fd, buffer.data(), buffer.size(), 0);
                    if (n < 0) {
                        if (errno == EINTR)
                            continue;
                        if (errno == EAGAIN || errno == EWOULDBLOCK)
                            break;
                        Error("recv");
                    }
                    if (n == 0)
                        throw std::runtime_error(
                            "TCP peer disconnected; snapshot epoch cannot exclude it");
                    c.input.insert(c.input.end(), buffer.begin(), buffer.begin() + n);
                    while (c.input.size() >= 4) {
                        Decoder length(std::span(c.input).first(4));
                        auto size = length.U32();
                        if (size < 29 || size > kMaxFrame)
                            throw std::runtime_error("invalid TCP frame length");
                        if (c.input.size() < size + 4)
                            break;
                        Bytes body(c.input.begin() + 4, c.input.begin() + 4 + size);
                        auto m = Decode(body);
                        c.input.erase(c.input.begin(), c.input.begin() + 4 + size);
                        receive(peer, m);
                    }
                }
            }
        }
    } catch (...) {
        failed_ = true;
        throw;
    }
}
int ListenTcp(const char *address, uint16_t port, uint16_t &bound_port) {
    auto a = Address(address, port);
    int fd = NewSocket();
    if (::bind(fd, reinterpret_cast<sockaddr *>(&a), sizeof(a)) < 0 || ::listen(fd, 16) < 0) {
        int saved = errno;
        ::close(fd);
        errno = saved;
        Error("TCP listen");
    }
    socklen_t size = sizeof(a);
    if (::getsockname(fd, reinterpret_cast<sockaddr *>(&a), &size) < 0) {
        int saved = errno;
        ::close(fd);
        errno = saved;
        Error("getsockname");
    }
    bound_port = ntohs(a.sin_port);
    return fd;
}
int ConnectTcp(const char *address, uint16_t port) {
    auto a = Address(address, port);
    int fd = NewSocket();
    // Bounded connection setup; do not hang indefinitely on unreachable peers.
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        int saved = errno;
        ::close(fd);
        errno = saved;
        Error("connect flags");
    }
    int rc = ::connect(fd, reinterpret_cast<sockaddr *>(&a), sizeof(a));
    if (rc < 0 && errno != EINPROGRESS) {
        int saved = errno;
        ::close(fd);
        errno = saved;
        Error("connect");
    }
    if (rc < 0) {
        pollfd p{fd, POLLOUT, 0};
        do {
            rc = ::poll(&p, 1, 5000);
        } while (rc < 0 && errno == EINTR);
        int error = 0;
        socklen_t size = sizeof(error);
        if (rc <= 0 || ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &size) < 0 || error) {
            int saved = error ? error : (rc == 0 ? ETIMEDOUT : errno);
            ::close(fd);
            errno = saved;
            Error("TCP connect completion");
        }
    }
    return fd;
}
int AcceptTcp(int listener) {
    pollfd p{listener, POLLIN, 0};
    int rc;
    do {
        rc = ::poll(&p, 1, 5000);
    } while (rc < 0 && errno == EINTR);
    if (rc <= 0) {
        if (rc == 0)
            errno = ETIMEDOUT;
        Error("TCP accept timeout");
    }
    int fd;
    do {
        fd = ::accept(listener, nullptr, nullptr);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0)
        Error("accept");
    return fd;
}
} // namespace stormglass::snapshot
