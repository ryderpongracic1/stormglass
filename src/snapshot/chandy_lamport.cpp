#include "snapshot/chandy_lamport.h"
#include <limits>
#include <stdexcept>
#include <utility>

namespace stormglass::snapshot {
namespace {
void Require(bool condition, const char *message) {
    if (!condition)
        throw std::runtime_error(message);
}
} // namespace
Participant::Participant(NodeId node, std::vector<NodeId> peers, Capture capture, Deliver deliver,
                         Transmit transmit, std::size_t limit)
    : node_(node), capture_(std::move(capture)), deliver_(std::move(deliver)),
      transmit_(std::move(transmit)), limit_(limit) {
    Require(limit > 0 && capture_ && deliver_ && transmit_, "invalid snapshot callbacks/limit");
    for (auto peer : peers) {
        Require(peer != node && received_.emplace(peer, 0).second, "invalid snapshot peer");
        sent_.emplace(peer, 0);
    }
}
void Participant::CheckHealthy() const { Require(!failed_, "snapshot participant failed"); }
void Participant::CheckPeer(NodeId peer) const {
    Require(received_.contains(peer), "unknown snapshot peer");
}
void Participant::Start(uint64_t epoch) {
    Require(epoch > last_epoch_ && !active_ && !completed_, "overlapping/stale snapshot epoch");
    LocalSnapshot s;
    s.node = node_;
    s.epoch = epoch;
    s.received = received_;
    s.sent = sent_;
    s.state = capture_();
    Require(s.state.size() <= limit_, "snapshot state exceeds limit");
    recorded_bytes_ = s.state.size();
    active_ = std::move(s);
    marked_.clear();
    // All marker enqueues precede any subsequent application sends.
    for (const auto &[peer, sequence] : sent_)
        transmit_(peer, Message{Message::Kind::Marker, sequence, epoch, {}});
}
void Participant::FinishIfReady() {
    if (active_ && marked_.size() == received_.size()) {
        last_epoch_ = active_->epoch;
        completed_ = std::move(active_);
        active_.reset();
    }
}
void Participant::Initiate(uint64_t epoch) {
    CheckHealthy();
    try {
        Start(epoch);
        FinishIfReady();
    } catch (...) {
        failed_ = true;
        throw;
    }
}
void Participant::Send(NodeId peer, Bytes payload) {
    CheckHealthy();
    try {
        CheckPeer(peer);
        Require(sent_.at(peer) != std::numeric_limits<uint64_t>::max(), "send sequence overflow");
        auto sequence = sent_.at(peer) + 1;
        transmit_(peer, Message{Message::Kind::Data, sequence, 0, std::move(payload)});
        sent_.at(peer) = sequence;
    } catch (...) {
        failed_ = true;
        throw;
    }
}
void Participant::Receive(NodeId peer, const Message &m) {
    CheckHealthy();
    try {
        CheckPeer(peer);
        if (m.kind == Message::Kind::Data) {
            Require(m.epoch == 0 && received_.at(peer) != std::numeric_limits<uint64_t>::max() &&
                        m.sequence == received_.at(peer) + 1,
                    "non-FIFO/duplicate data sequence");
            if (active_ && !marked_.contains(peer)) {
                constexpr std::size_t overhead = sizeof(ChannelRecord);
                Require(overhead <= limit_ - recorded_bytes_ &&
                            m.payload.size() <= limit_ - recorded_bytes_ - overhead,
                        "in-flight channel snapshot exceeds limit");
                active_->channels.push_back({peer, m.sequence, m.payload});
                recorded_bytes_ += overhead + m.payload.size();
            }
            received_.at(peer) = m.sequence;
            deliver_(peer, m.payload);
        } else if (m.kind == Message::Kind::Marker) {
            Require(m.payload.empty() && m.epoch != 0, "invalid marker");
            // FIFO places every pre-cut data message before its marker.
            Require(m.sequence == received_.at(peer),
                    "marker sequence does not match FIFO frontier");
            if (!active_)
                Start(m.epoch);
            Require(active_->epoch == m.epoch && marked_.insert(peer).second,
                    "overlapping/duplicate marker");
            active_->marker_sequences.emplace(peer, m.sequence);
            FinishIfReady();
        } else
            throw std::runtime_error("unknown message kind");
    } catch (...) {
        failed_ = true;
        throw;
    }
}
LocalSnapshot Participant::TakeCompleted() {
    CheckHealthy();
    Require(completed_.has_value(), "snapshot is incomplete");
    auto s = std::move(*completed_);
    completed_.reset();
    return s;
}
void Participant::Restore(const LocalSnapshot &s) {
    CheckHealthy();
    try {
        Require(!active_ && !completed_ && last_epoch_ == 0 && s.node == node_ && s.epoch > 0,
                "restore requires a fresh matching participant");
        Require(s.received.size() == received_.size() && s.sent.size() == sent_.size() &&
                    s.marker_sequences.size() == received_.size(),
                "restore topology mismatch");
        for (const auto &[peer, count] : received_) {
            Require(count == 0 && sent_.at(peer) == 0 && s.received.contains(peer) &&
                        s.sent.contains(peer) && s.marker_sequences.contains(peer),
                    "restore topology/activity mismatch");
        }
        received_ = s.received;
        sent_ = s.sent;
        for (const auto &r : s.channels) {
            CheckPeer(r.peer);
            Require(received_.at(r.peer) != std::numeric_limits<uint64_t>::max() &&
                        r.sequence == received_.at(r.peer) + 1,
                    "invalid replay sequence");
            received_.at(r.peer) = r.sequence;
            deliver_(r.peer, r.payload);
        }
        Require(received_ == s.marker_sequences, "incomplete channel replay");
        last_epoch_ = s.epoch;
    } catch (...) {
        failed_ = true;
        throw;
    }
}
void ValidateGlobal(const std::vector<LocalSnapshot> &snapshots, const Topology &topology) {
    Require(!topology.empty() && snapshots.size() == topology.size(), "incomplete global snapshot");
    std::map<NodeId, const LocalSnapshot *> nodes;
    uint64_t epoch = snapshots.front().epoch;
    Require(epoch > 0, "invalid global epoch");
    for (const auto &s : snapshots) {
        Require(s.epoch == epoch && topology.contains(s.node) && nodes.emplace(s.node, &s).second,
                "mixed epoch/duplicate/unknown node");
    }
    for (const auto &[id, peers] : topology) {
        const auto &s = *nodes.at(id);
        std::set<NodeId> expected(peers.begin(), peers.end());
        Require(expected.size() == peers.size() && !expected.contains(id), "invalid topology");
        Require(s.received.size() == expected.size() && s.sent.size() == expected.size() &&
                    s.marker_sequences.size() == expected.size(),
                "snapshot topology mismatch");
        auto next = s.received;
        for (const auto &r : s.channels) {
            Require(expected.contains(r.peer) && next.contains(r.peer) &&
                        next.at(r.peer) != std::numeric_limits<uint64_t>::max() &&
                        r.sequence == next.at(r.peer) + 1,
                    "channel log is not a contiguous FIFO prefix");
            ++next.at(r.peer);
        }
        for (auto peer : expected) {
            Require(nodes.contains(peer) && s.sent.contains(peer) && next.contains(peer) &&
                        s.marker_sequences.contains(peer),
                    "snapshot peer missing");
            const auto &sender = *nodes.at(peer);
            Require(sender.sent.contains(id) && sender.received.contains(id),
                    "asymmetric topology");
            Require(sender.sent.at(id) == next.at(peer) &&
                        next.at(peer) == s.marker_sequences.at(peer),
                    "inconsistent global cut: missing/orphan channel messages");
        }
    }
}
} // namespace stormglass::snapshot
