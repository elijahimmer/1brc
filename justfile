alias r := run
alias b := build

run: build
    time ./zig-out/bin/1brz

build:
    zig build

