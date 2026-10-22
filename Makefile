CXX      ?= g++
BUILD    ?= build
WARN     := -Wall -Wextra -Wpedantic
CXXFLAGS ?= -std=c++17 -O2 $(WARN) -pthread -Iinclude

BINS := $(BUILD)/lb $(BUILD)/echo_backend $(BUILD)/load_client
HDRS := $(wildcard include/*.hpp)

.PHONY: all clean test tsan asan demo

all: $(BINS)

$(BUILD)/lb: src/main.cpp $(HDRS) | $(BUILD)
	$(CXX) $(CXXFLAGS) $< -o $@

$(BUILD)/echo_backend: tools/echo_backend.cpp $(HDRS) | $(BUILD)
	$(CXX) $(CXXFLAGS) $< -o $@

$(BUILD)/load_client: tools/load_client.cpp $(HDRS) | $(BUILD)
	$(CXX) $(CXXFLAGS) $< -o $@

$(BUILD):
	mkdir -p $@

test: all
	BIN_DIR=$(BUILD) python3 tests/integration_test.py -v

# Same code, instrumented: ThreadSanitizer finds data races / lock misuse,
# AddressSanitizer finds leaks and memory errors.
tsan:
	$(MAKE) BUILD=build-tsan CXXFLAGS="-std=c++17 -O1 -g $(WARN) -pthread -Iinclude -fsanitize=thread" test

asan:
	$(MAKE) BUILD=build-asan CXXFLAGS="-std=c++17 -O1 -g $(WARN) -pthread -Iinclude -fsanitize=address,undefined" test

demo: all
	./scripts/demo.sh

clean:
	rm -rf build build-tsan build-asan
