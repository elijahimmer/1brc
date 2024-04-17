/// The maximum number of threads to that are supported.
const MAX_THREADS = 16;

/// the maximum size for a read buffer
const read_buffer_size = 1 << 16;

/// the type of a read buffer index.
const readBufferIndex = meta.Int(.unsigned, math.log2(read_buffer_size));

/// The array that stores the info of every thread.
const ThreadInfoArr = [MAX_THREADS]ThreadInfo;

/// The info each thread gets.
const ThreadInfo = struct {
    should_stop: bool = false,
    should_read: ?readBufferIndex = null,
};

pub fn main() !void {
    var gpalloc = std.heap.GeneralPurposeAllocator(.{
        .thread_safe = false,
    }){};
    defer _ = gpalloc.deinit();

    var arena = std.heap.ArenaAllocator.init(gpalloc.allocator());
    const allocator = arena.allocator();
    defer arena.deinit();

    const stdout_file = std.io.getStdOut().writer();
    var bw = std.io.bufferedWriter(stdout_file);
    const stdout = bw.writer();

    var file = try fs.cwd().openFile("test.txt", .{}); // measurements.txt
    defer file.close();

    var map: HashMap = .{};
    try map.map.ensureTotalCapacity(allocator, 1 << 18);

    const threads = (std.Thread.getCpuCount() catch 2) - 1;
    assert(threads > 0);
    assert(threads <= MAX_THREADS); // If this fails, increase MAX_THREADS

    var read_buf: [MAX_THREADS][read_buffer_size]u8 = undefined;

    var thread_info: [MAX_THREADS]ThreadInfo = [_]ThreadInfo{.{}} ** MAX_THREADS;
    var thread_list: [MAX_THREADS]Thread = undefined;

    var bytes_read: usize = 0;

    { // thread scope
        for (0..threads) |idx| {
            thread_list[idx] = try Thread.spawn(.{}, thread_loop, .{ idx, &thread_info[idx], &map, &read_buf[idx] });
        }
        defer {
            for (0..threads) |idx| thread_info[idx].should_stop = true;
            for (0..threads) |idx| thread_list[idx].join();
        }

        var time = try std.time.Timer.start();
        defer {
            stdout.print("{},", .{time.read()}) catch {};
            bw.flush() catch {};
        }

        var leftover: ?[]u8 = null;
        var thread: usize = 0;
        while (true) {
            // search for a thread which is done
            while (true) : (thread += 1) {
                if (thread >= threads) thread = 0;

                // if the thread is done processing, break
                if (thread_info[thread].should_read == null) break;
            }
            defer thread += 1;

            var buffer = &read_buf[thread];

            // copy over leftover bytes
            const leftover_bytes: usize = blk: {
                if (leftover) |left| {
                    std.log.debug("leftover: '{s}'", .{left});
                    assert(left.len <= math.maxInt(readBufferIndex)); // if not, the leftover is too big

                    // leftover cannot have a newline
                    if (debug.runtime_safety) assert(mem.count(u8, left, "\n") == 0);

                    @memcpy(buffer[0..left.len], left);

                    break :blk left.len;
                }
                std.log.debug("no leftover", .{});
                break :blk 0;
            };

            const bytes = try file.read(buffer[leftover_bytes..]);
            bytes_read += bytes;

            if (bytes == 0) {
                assert(leftover == null); // when it's over, there is no leftovers if done properly
                break;
            }

            const valid_length = leftover_bytes + bytes;
            const valid_buf = buffer[0..valid_length];

            const last_newline = mem.lastIndexOfScalar(u8, valid_buf, '\n').?;
            assert(last_newline <= math.maxInt(readBufferIndex)); // shouldn't be possible

            // alert thread
            thread_info[thread].should_read = @intCast(last_newline);

            const leftover_buf = valid_buf[last_newline + 1 ..];
            // send to leftovers
            if (leftover_buf.len > 0) {
                leftover = leftover_buf;
            } else {
                leftover = null;
            }
        }

        std.log.info("waiting for threads...", .{});
    }

    for (0..threads) |thread| assert(thread_info[thread].should_read == null); // if some threads still have to read, that's bad.

    const file_size: usize = (try file.metadata()).size();
    assert(bytes_read == file_size); // didn't read the whole file if failed
}

/// The main loop of each non-reading thread
fn thread_loop(thread_id: usize, thread_info: *ThreadInfo, map: *HashMap, buffer: *[read_buffer_size]u8) void {
    Thread.yield() catch {};

    while (true) {
        const info = thread_info.*;

        if (info.should_read) |read_to| {
            defer thread_info.should_read = null; // reset the info to say it is done.

            assert(read_to <= buffer.len); // so that we don't over-read

            var valid_buf = buffer[0..read_to];

            var iter = mem.splitScalar(u8, valid_buf, '\n');
            var line_max = mem.count(u8, valid_buf, "\n");

            var line_count: usize = 0;
            while (iter.next()) |line| {
                parse.parse_line(map, line) catch |err| {
                    panic("thread {:2}, {}/{} {s}: '{s}'\n'{s}':'{s}'", .{ thread_id, line_count, line_max, @errorName(err), line, valid_buf, buffer[read_to..] });
                };
                line_count += 1;
            }
        }

        if (info.should_stop) break;

        //Thread.yield() catch {};
    }
}

const parse = @import("parse.zig");

const HashMap = parse.HashMap;
const HashMapContent = parse.HashMapContent;

const std = @import("std");
const debug = std.debug;
const fs = std.fs;
const math = std.math;
const mem = std.mem;
const meta = std.meta;
const testing = std.testing;

const assert = std.debug.assert;
const panic = std.debug.panic;

const Thread = std.Thread;
