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
    should_read: bool = false,
    read_to: readBufferIndex = 0,
};

pub fn main() !void {
    var gpalloc = std.heap.GeneralPurposeAllocator(.{
        .thread_safe = false,
    }){};

    const allocator = gpalloc.allocator();

    //// who needs to deallocate? :)
    //defer _ = gpalloc.deinit();
    //var arena = std.heap.ArenaAllocator.init(gpalloc.allocator());
    //const allocator = arena.allocator();
    //defer arena.deinit();

    const stdout_file = std.io.getStdOut().writer();
    var bw = std.io.bufferedWriter(stdout_file);
    const stdout = bw.writer();

    var file = try fs.cwd().openFile("test.txt", .{}); // measurements.txt
    defer file.close();

    var map = .{ .mutex = .{}, .map = HashMapContent.init(allocator) };
    defer map.map.deinit();

    try map.map.ensureTotalCapacity(1 << 20);

    const threads = 2; //(std.Thread.getCpuCount() catch 2) - 1;
    assert(threads > 0);
    assert(threads <= MAX_THREADS); // If this fails, increase MAX_THREADS

    var read_buf = try allocator.alignedAlloc(u8, mem.page_size, read_buffer_size * threads);

    var thread_info: [MAX_THREADS]ThreadInfo = [_]ThreadInfo{.{}} ** MAX_THREADS;
    var thread_list: [MAX_THREADS]Thread = undefined;

    var bytes_read: usize = 0;

    { // thread scope
        for (0..threads) |idx| {
            const start_idx = idx * read_buffer_size;
            const end_idx = start_idx + read_buffer_size;
            assert(end_idx <= read_buf.len);

            var buf = read_buf[start_idx..end_idx];

            thread_list[idx] = try Thread.spawn(.{ .allocator = allocator }, thread_loop, .{ idx, &thread_info[idx], &map, buf });
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
                if (!thread_info[thread].should_read) break;
            }

            const start_idx = read_buffer_size * thread;
            const end_idx = start_idx + read_buffer_size;

            assert(start_idx < read_buf.len);
            assert(end_idx <= read_buf.len); // that would be bad

            var leftover_bytes: usize = 0;
            if (leftover) |left| {
                std.log.debug("leftover: '{s}'", .{left});
                assert(left.len <= math.maxInt(u6)); // if not, the leftover is too big

                leftover_bytes = left.len;

                if (std.debug.runtime_safety) assert(mem.count(u8, left, "\n") == 0);

                @memcpy(read_buf[start_idx .. start_idx + left.len], left);
            }

            var buf = read_buf[start_idx..end_idx];

            const bytes = try file.read(buf[leftover_bytes..]);
            bytes_read += bytes;

            if (bytes == 0) break;

            assert(buf[0] != '\n'); // if a buf ever starts with a newline, that's not right.

            const valid_length = leftover_bytes + bytes;
            const valid_buf = buf[0..valid_length];

            const last_newline = mem.lastIndexOfLinear(u8, valid_buf, "\n").?; // linear search as it should be very close
            assert(last_newline <= math.maxInt(readBufferIndex));

            thread_info[thread] = .{
                .should_read = true,
                .read_to = @intCast(last_newline),
            };

            if (last_newline < valid_buf.len) {
                leftover = valid_buf[last_newline + 1 ..];
            } else {
                leftover = null;
            }
        }

        std.log.info("waiting for threads...", .{});
    }

    for (0..threads) |thread| assert(thread_info[thread].should_read == false); // if some threads still have to read, that's bad.

    //try stdout.print("bytes read: {}\n", .{bytes_read});
    //try bw.flush();

    const file_size: usize = (try file.metadata()).size();
    assert(bytes_read == file_size); // didn't read the whole file if failed
}

/// The main loop of each non-reading thread
fn thread_loop(thread_id: usize, thread_info: *ThreadInfo, map: *HashMap, buffer: []const u8) void {
    Thread.yield() catch {};

    while (true) {
        const info = thread_info.*;

        // if the thread should read.
        if (info.should_read) { // process data
            defer thread_info.*.should_read = false; // reset the info to say it is done.

            const read_to = info.read_to;
            if (info.read_to == 0) break;

            assert(read_to < buffer.len); // so that we don't over-read

            var valid_buf = buffer[0..read_to];

            if (std.debug.runtime_safety) {
                const last_semi = mem.lastIndexOfLinear(u8, valid_buf, ";").?;
                const last_newline = mem.lastIndexOfLinear(u8, valid_buf, "\n").?;

                assert(last_semi > last_newline);
            }

            var iter = mem.splitScalar(u8, valid_buf, '\n');
            while (iter.next()) |line| {
                if (line.len == 0) break;

                if (std.debug.runtime_safety) assert(mem.count(u8, line, ";") == 1);

                parse.parse_line(map, line) catch |err| {
                    std.log.err("thread {:2}, Failed to parse line: '{s}' with {s}", .{ thread_id, line, @errorName(err) });
                };
            }
        }

        if (info.should_stop) break;

        Thread.yield() catch {};
    }
}

const parse = @import("parse.zig");

const HashMap = parse.HashMap;
const HashMapContent = parse.HashMapContent;

const std = @import("std");
const fs = std.fs;
const math = std.math;
const mem = std.mem;
const meta = std.meta;
const testing = std.testing;

const assert = std.debug.assert;

const Thread = std.Thread;
