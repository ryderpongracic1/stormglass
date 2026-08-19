.PHONY: build test bench oracle nemesis clean configure release

BUILD_DIR := build
RELEASE_DIR := build-release

configure:
	cmake -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=Debug -DSTORMGLASS_SANITIZERS=ON

build: configure
	cmake --build $(BUILD_DIR) -j$$(nproc)

test: build
	cd $(BUILD_DIR) && ctest --output-on-failure

release:
	cmake -B $(RELEASE_DIR) -DCMAKE_BUILD_TYPE=Release
	cmake --build $(RELEASE_DIR) -j$$(nproc)

bench: release
	./$(RELEASE_DIR)/app/stormglass_bench

oracle: release
	./$(RELEASE_DIR)/app/stormglass_oracle --seeds 100 --records 10000

nemesis: release
	./$(RELEASE_DIR)/app/stormglass_nemesis --seeds 20 --verbose

clean:
	rm -rf $(BUILD_DIR) $(RELEASE_DIR)
