#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <vector>

namespace stormglass::snapshot {
using NodeId = uint32_t;
using Bytes = std::vector<uint8_t>;

struct Message {
    enum class Kind : uint8_t { Data = 1, Marker = 2 };
    Kind kind = Kind::Data;
    uint64_t sequence = 0; // data sequence, or sender's sequence at its local cut
    uint64_t epoch = 0;
    Bytes payload;
};
struct ChannelRecord {
    NodeId peer;
    uint64_t sequence;
    Bytes payload;
};
struct LocalSnapshot {
    NodeId node = 0;
    uint64_t epoch = 0;
    Bytes state;
    std::map<NodeId, uint64_t> received; // local cut, before channel replay
    std::map<NodeId, uint64_t> sent;
    std::map<NodeId, uint64_t> marker_sequences;
    // Receive order across channels is retained, as well as FIFO within each channel.
    std::vector<ChannelRecord> channels;
};

// One owner/event-loop thread. Each duplex peer represents two reliable FIFO
// channels. Transmit must enqueue synchronously, preserving order, or throw.
// Capture/Deliver/Transmit must not call back into this participant. Any callback
// or protocol failure poisons it; discard the instance and any incomplete epoch.
class Participant {
  public:
    using Capture = std::function<Bytes()>;
    using Deliver = std::function<void(NodeId, const Bytes &)>;
    using Transmit = std::function<void(NodeId, const Message &)>;
    Participant(NodeId node, std::vector<NodeId> peers, Capture capture, Deliver deliver,
                Transmit transmit, std::size_t max_snapshot_bytes = 64 * 1024 * 1024);
    void Send(NodeId peer, Bytes payload);
    void Initiate(uint64_t epoch);
    void Receive(NodeId peer, const Message &message);
    [[nodiscard]] bool HasCompleted() const { return completed_.has_value(); }
    LocalSnapshot TakeCompleted();
    // Fresh connections and a globally validated snapshot are required. Restore
    // application state BEFORE calling this method. Replays saved channel data
    // before accepting any fresh network input. Do not use for live rollback.
    void Restore(const LocalSnapshot &snapshot);
    [[nodiscard]] bool failed() const { return failed_; }

  private:
    void CheckPeer(NodeId peer) const;
    void CheckHealthy() const;
    void Start(uint64_t epoch);
    void FinishIfReady();
    NodeId node_;
    std::map<NodeId, uint64_t> received_, sent_;
    Capture capture_;
    Deliver deliver_;
    Transmit transmit_;
    std::size_t limit_, recorded_bytes_ = 0;
    std::optional<LocalSnapshot> active_, completed_;
    std::set<NodeId> marked_;
    uint64_t last_epoch_ = 0;
    bool failed_ = false;
};

// Validate fixed, symmetric topology and the consistent-cut conservation rule:
// sender.sent == receiver.received_at_cut + exactly the FIFO channel log.
using Topology = std::map<NodeId, std::vector<NodeId>>;
void ValidateGlobal(const std::vector<LocalSnapshot> &snapshots, const Topology &topology);
} // namespace stormglass::snapshot
