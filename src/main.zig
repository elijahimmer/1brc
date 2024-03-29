/// The maximum number of threads to that are supported.
const MAX_THREADS = 16;

/// The info each thread gets.
/// The first 6 bits are for how many bytes of the read buffer to not use.
/// The second to last is the keep-alive bit, if it's reset, stop the thread.
/// The last bit is the read bit. If it's set, then process the buffer, then reset it.
const ThreadInfo = u8;

/// If this bit is set, then process the buffer, if not then the data will be replaced to then be processed.
const read_mask: ThreadInfo = 0b01000000;

/// Keep the thread alive as long as this bit is set.
const stop_mask: ThreadInfo = 0b10000000;

/// These bits are how many bytes at the end of the  buffer to not parse.
const leftover_mask: ThreadInfo = 0b00111111;
const leftover_bit_shift: std.math.Log2Int(@typeInfo(ThreadInfo).Int.bits) = 2;

/// The array that stores the info of every thread.
const ThreadInfoArr = [MAX_THREADS]ThreadInfo;

/// ThreadInfoArr but as a integer to quickly set bits on every thread at once.
const ThreadInfoInt = std.meta.Int(.unsigned, @typeInfo(ThreadInfo).Int.bits * MAX_THREADS);

/// The array that holds the starting value, where all threads are set to be alive.
const full_stop_mask = [_]ThreadInfo{stop_mask} ** MAX_THREADS;

/// The stop_mask array, but as a int that you can kill all threads at once.
const full_stop_mask_int: ThreadInfoInt = @as(*ThreadInfoInt, @constCast(@alignCast(@ptrCast(&full_stop_mask)))).*;

/// The basic length of a read buffer. The thread might be told to use less because the whole buffer may not have been used.
const READ_BUFFER_SIZE = 1 << 16;

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

    var file = try fs.cwd().openFile("measurements.txt", .{}); // measurements.txt
    defer file.close();

    const threads = (std.Thread.getCpuCount() catch 1) - 1;
    assert(threads <= MAX_THREADS); // If this fails, increase MAX_THREADS

    var read_buf = try allocator.alloc(u8, READ_BUFFER_SIZE * threads);

    var threads_info: [MAX_THREADS]ThreadInfo = full_stop_mask;
    var threads_info_int: *ThreadInfoInt = @alignCast(@ptrCast(&threads_info));

    var thread_list: [MAX_THREADS]Thread = undefined;

    var bytes_read: usize = 0;

    { // thread scope
        for (0..threads) |idx| {
            const start_idx = idx * READ_BUFFER_SIZE;
            const end_idx = start_idx + READ_BUFFER_SIZE;
            assert(end_idx <= read_buf.len);

            var buf = read_buf[start_idx..end_idx];

            thread_list[idx] = try Thread.spawn(.{ .allocator = allocator }, thread_loop, .{ idx, &threads_info[idx], buf });
        }
        defer {
            threads_info_int.* ^= full_stop_mask_int;
            for (0..threads) |idx| thread_list[idx].join();
        }

        var leftover: usize = 0;
        while (true) {
            var thread: usize = 0;

            while (true) : (thread += 1) {
                if (thread > threads) thread = 0;

                // if the thread is done processing, break
                if (threads_info[thread] & read_mask == 0) break;
            }

            assert(thread <= threads); // shouldn't be possible

            const start_idx = READ_BUFFER_SIZE * thread;
            var end_idx = start_idx + READ_BUFFER_SIZE;

            std.log.info("buf len: {}, start: {}, end: {}", .{ read_buf.len, start_idx, end_idx });

            assert(start_idx < read_buf.len);
            assert(end_idx <= read_buf.len); // that would be bad

            var buf = read_buf[start_idx..end_idx];

            const bytes = try file.read(buf[leftover..]);
            bytes_read += bytes;

            if (bytes == 0) {
                threads_info[thread] = stop_mask | read_mask;
                break;
            }

            const valid_length = leftover + bytes;

            leftover = valid_length - mem.lastIndexOfScalar(u8, buf[0..valid_length], '\n').? - 1;

            assert(leftover <= std.math.maxInt(u6)); // if not, the leftover is too bit

            threads_info[thread] = @as(u8, @intCast(leftover)) | stop_mask | read_mask;

            if (leftover > 1) {
                const leftover_buf = buf[buf.len - leftover ..];

                assert(mem.count(u8, leftover_buf, "\n") == 0);

                if (end_idx >= read_buf.len) end_idx = 0;

                @memcpy(read_buf[end_idx .. end_idx + leftover], leftover_buf);
            }
        }

        std.log.info("waiting for threads...", .{});
    }

    std.log.info("threads_info: {b}", .{threads_info_int.*});
    assert(threads_info_int.* == 0); // nothing good, some threads didn't read

    try stdout.print("bytes read: {}\n", .{bytes_read});
    try bw.flush();

    const file_bytes = (try file.metadata()).size();
    assert(bytes_read == file_bytes); // didn't read the whole file if failed
}

/// The main loop of each non-reading thread
fn thread_loop(thread_id: usize, thread_info: *ThreadInfo, buffer: []const u8) void {
    Thread.yield() catch {};

    while (true) {
        // if the thread should read.
        if (thread_info.* & read_mask == 1) { // process data

            const leftover_bits = @as(usize, thread_info.* & leftover_mask);

            const valid_buf = buffer[0 .. READ_BUFFER_SIZE - leftover_bits];

            _ = valid_buf;

            thread_info.* ^= read_mask; // reset the read bit to say the processing is over.
        }
        if (thread_info.* & stop_mask == 0) break; // stop the thread

        Thread.yield() catch {};
    }

    std.log.info("thread {} finished", .{thread_id});
}

const parse = @import("parse.zig");

const HashMap = parse.HashMap;

const std = @import("std");
const fs = std.fs;
const mem = std.mem;
const testing = std.testing;

const assert = std.debug.assert;

const Thread = std.Thread;
