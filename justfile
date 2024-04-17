alias r := run
alias b := build
alias t := test

run: build
    time ./zig-out/bin/1brc

build:
    zig build

test:
    zig test src/parse.zig

