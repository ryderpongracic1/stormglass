#pragma once
#include "snapshot/chandy_lamport.h"
#include <string>

namespace stormglass::snapshot {
// One writer per (epoch,node), one commit coordinator. Immutable local files;
// the commit manifest binds all CRCs, epoch, topology and caller's job/config ID.
// Local files alone never authorize recovery. All files must still validate on
// load. A failed/incomplete epoch leaves the previous committed epoch usable.
class Store {
  public:
    explicit Store(std::string root) : root_(std::move(root)) {}
    void SaveLocal(const LocalSnapshot &snapshot) const;
    void Commit(uint64_t epoch, const Topology &topology, const std::string &configuration) const;
    std::vector<LocalSnapshot> LoadCommitted(uint64_t epoch, const Topology &topology,
                                             const std::string &configuration) const;
    std::vector<LocalSnapshot> LoadLatestCommitted(const Topology &topology,
                                                   const std::string &configuration) const;
    static Bytes EncodeLocal(const LocalSnapshot &snapshot);
    static LocalSnapshot DecodeLocal(const Bytes &bytes);

  private:
    std::string root_;
};
} // namespace stormglass::snapshot
