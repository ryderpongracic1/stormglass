#include "window/window.h"
#include <functional>
namespace stormglass {
size_t WindowHash::operator()(const Window& w) const {
    auto h1 = std::hash<int64_t>{}(w.start.time_since_epoch().count());
    auto h2 = std::hash<int64_t>{}(w.end.time_since_epoch().count());
    return h1 ^ (h2 * 0x9e3779b97f4a7c15ULL + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
}
} // namespace stormglass
