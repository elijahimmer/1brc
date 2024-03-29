run: build
    time ./zig-out/bin/1brc

build:
    zig build

test:
    zig test src/main.zig
