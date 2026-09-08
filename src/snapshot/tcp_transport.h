#pragma once
#include "snapshot/chandy_lamport.h"
#include <deque>
#include <map>

namespace stormglass::snapshot {
struct PeerSocket {
    NodeId peer;
    int fd;
};
// Owns connected TCP sockets. One owner thread polls all channels concurrently;
// nonblocking writes prevent reciprocal marker sends from deadlocking. Fixed
// byte budgets bound receive frames and queued output. Overflow/EOF is a job
// failure, never a reason to silently exclude a channel from a snapshot.
class TcpTransport {
  public:
    explicit TcpTransport(std::vector<PeerSocket> sockets,
                          std::size_t max_queued_bytes_per_peer = 4 * 1024 * 1024);
    ~TcpTransport();
    TcpTransport(const TcpTransport &) = delete;
    TcpTransport &operator=(const TcpTransport &) = delete;
    void Queue(NodeId peer, const Message &message);
    void PollOnce(const std::function<void(NodeId, const Message &)> &receive, int timeout_ms = 10);
    [[nodiscard]] bool Drained() const;
    static constexpr std::size_t kMaxFrame = 1024 * 1024;

  private:
    struct Channel {
        int fd = -1;
        Bytes input;
        std::deque<Bytes> output;
        std::size_t front_offset = 0, queued_bytes = 0;
    };
    std::map<NodeId, Channel> channels_;
    std::size_t queue_limit_;
    bool failed_ = false;
};
// Numeric IPv4 endpoints, useful for separate processes/hosts. Listener owns
// the returned fd; caller transfers accepted/connected fds to TcpTransport.
int ListenTcp(const char *address, uint16_t port, uint16_t &bound_port);
int ConnectTcp(const char *address, uint16_t port);
int AcceptTcp(int listener);
} // namespace stormglass::snapshot
