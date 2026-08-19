#include "stream/watermark.h"
namespace stormglass {
bool WatermarkTracker::Advance(Timestamp wm) { if (wm > current_) { current_ = wm; return true; } return false; }
} // namespace stormglass
