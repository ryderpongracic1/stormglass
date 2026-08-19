.PHONY: build test bench clean configure

BUILD_DIR := build

configure:
	cmake -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=Debug -DSTORMGLASS_SANITIZERS=ON

build: configure
	cmake --build $(BUILD_DIR) -j$$(nproc)

test: build
	cd $(BUILD_DIR) && ctest --output-on-failure

bench:
	cmake -B build-release -DCMAKE_BUILD_TYPE=Release -DSTORMGLASS_BENCH=ON
	cmake --build build-release -j$$(nproc)
	./build-release/app/bench/stormglass_bench

clean:
	rm -rf $(BUILD_DIR) build-release
