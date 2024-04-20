var line_count: usize = 0;

pub fn main() !void {
    var gpalloc = std.heap.GeneralPurposeAllocator(.{
        .thread_safe = false,
    }){};
    defer _ = gpalloc.deinit();
    const gp_allocator = gpalloc.allocator();

    const stdout_file = std.io.getStdOut().writer();
    var bw = std.io.bufferedWriter(stdout_file);
    const stdout = bw.writer();

    var args = std.process.args();
    _ = args.next(); // discard program name
    const file_name = args.next() orelse "measurements.txt";

    //// Setup file

    var file = try std.fs.cwd().openFile(file_name, .{ .mode = .read_only });
    defer file.close();

    const file_size = (try file.metadata()).size();

    log.info("File Size: {}", .{file_size});

    // got the mmap from https://github.com/candrewlee14/1brc-zig, it makes is farrrr simplier
    const file_contents = try os.mmap(null, file_size, os.PROT.READ, os.MAP.SHARED, file.handle, 0);
    defer os.munmap(file_contents);

    // same here, from got https://github.com/candrewlee14/1brc-zig
    if (builtin.os.tag == .linux) try std.os.madvise(file_contents.ptr, file_size, os.MADV.HUGEPAGE);

    assert(file_contents.len == file_size); // should be OS guaranteed

    const spawn_count = try std.Thread.getCpuCount();

    //// Pre Alloc Memory

    const expected_alloc_size = blk: {
        const thread_list_size = @sizeOf(Thread) * spawn_count;
        const map_list_size = @sizeOf(HashMap) * spawn_count;

        const one_hash_map = (@sizeOf(HashMap.KV) + 75) * HASH_MAP_SIZE; // approximately what each hash map needs

        const hash_map_size = one_hash_map * (spawn_count + 1);
        const name_list_size = @sizeOf([]const u8) * MAX_UNIQUE_NAMES;

        break :blk thread_list_size + map_list_size + hash_map_size + name_list_size;
    };

    log.info("fixed_buffer length: {}", .{expected_alloc_size});

    var fixed_buffer = try gp_allocator.alloc(u8, expected_alloc_size);
    defer gp_allocator.free(fixed_buffer);

    var fixed_alloc = std.heap.FixedBufferAllocator{ .end_index = 0, .buffer = fixed_buffer };

    const allocator = if (runtime_safety)
        (std.heap.ScopedLoggingAllocator(.@"1brz-alloc", .info, .err){ .parent_allocator = fixed_alloc.allocator() }).allocator()
    else
        fixed_alloc.allocator();

    //// Make Lists

    var thread_list: []Thread = try allocator.alloc(Thread, spawn_count);
    defer allocator.free(thread_list);

    var map_list = try allocator.alloc(HashMap, spawn_count);
    defer allocator.free(map_list);

    for (map_list) |*map| {
        map.* = .{};
        try map.ensureTotalCapacity(allocator, HASH_MAP_SIZE);
    }
    defer for (map_list) |*map| map.deinit(allocator);

    const thread_offset = file_size / spawn_count;
    var end_prev: usize = 0;

    for (0..spawn_count) |idx| {
        const end = mem.indexOfScalarPos(u8, file_contents, end_prev + thread_offset, '\n') orelse file_contents.len;

        var buffer = file_contents[end_prev..end];
        assert(buffer[0] != '\n'); // make sure newline isn't included

        thread_list[idx] = try Thread.spawn(.{ .allocator = allocator }, process, .{ idx, &map_list[idx], buffer });

        end_prev = end + 1;
    }

    var final_map = HashMap{};
    try final_map.ensureTotalCapacity(allocator, HASH_MAP_SIZE);
    defer final_map.deinit(allocator);

    for (0..spawn_count) |idx| {
        thread_list[idx].join();
        var iter = map_list[idx].iterator();
        while (iter.next()) |*item| {
            var got = final_map.getOrPutAssumeCapacity(item.key_ptr.*);

            if (got.found_existing) {
                got.value_ptr.merge(item.value_ptr);
            } else {
                got.value_ptr.* = item.value_ptr.*;
            }
        }
    }

    if (runtime_safety) {
        try stdout.print("Lines Processed: {}\nUnique Cities: {}\n", .{ line_count, final_map.count() });
        try bw.flush();
    }

    var map_names = try allocator.alloc([]const u8, MAX_UNIQUE_NAMES);

    {
        var map_iter = final_map.iterator();

        var idx: usize = 0;
        while (map_iter.next()) |entry| {
            map_names[idx] = entry.key_ptr.*;
            idx += 1;
        }

        map_names.len = idx;
    }

    std.mem.sortUnstable([]const u8, map_names, {}, strLessThan);

    try stdout.print("{{", .{});
    {
        try stdout.print("{s}=", .{map_names[0]});

        try final_map.get(map_names[0]).?.print(stdout);

        for (1..map_names.len) |idx| {
            try stdout.print(", {s}=", .{map_names[idx]});
            try final_map.get(map_names[idx]).?.print(stdout);
        }
    }
    try stdout.print("}}\n", .{});
    try bw.flush();
}

fn strLessThan(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.order(u8, a, b) == std.math.Order.lt;
}

pub fn process(
    thread_id: usize,
    map: *HashMap,
    buffer: []const u8,
) void {
    log.info("thread: {:2}, len: {}", .{ thread_id, buffer.len });

    if (runtime_safety) {
        const lines_processed = process_buffer(map, buffer) catch |err| {
            std.debug.panic("thread: {:2} with {s}", .{ thread_id, @errorName(err) });
        };

        log.info("thread: {:2}, lines processed: {}", .{ thread_id, lines_processed });
        _ = @atomicRmw(usize, &line_count, .Add, lines_processed, .Monotonic);
    } else process_buffer(map, buffer);
}

const ParseLineError = error{
    @"Line Too Short",
    @"No Semicolon",
    @"Too Many Semicolons",
    @"No Name",
    @"Line Contains Newline",
};

pub fn process_buffer(map: *HashMap, buffer: []const u8) if (runtime_safety) ParseLineError!usize else void {
    var line_no: usize = 0;
    var start_idx: usize = 0;

    @prefetch(map, .{});
    while (true) : (line_no += 1) {
        if (start_idx >= buffer.len) break;

        const end_of_line = mem.indexOfScalarPos(u8, buffer, start_idx + 7, '\n') orelse buffer.len;
        defer start_idx = end_of_line + 1;

        const line = buffer[start_idx..end_of_line];

        if (runtime_safety) {
            if (line.len < 6) return error.@"Line Too Short";

            const semi_count = mem.count(u8, line, ";");
            if (semi_count < 1) return error.@"No Semicolon";
            if (semi_count > 1) return error.@"Too Many Semicolons";

            if (mem.count(u8, line, "\n") > 0) return error.@"Line Contains Newline";
        }

        const semi_pos = mem.indexOfScalar(u8, line, ';').?;

        if (runtime_safety and semi_pos == 0) return error.@"No Name";

        const name = line[0..semi_pos];
        const num_buf = line[semi_pos + 1 ..];

        const num = if (runtime_safety)
            parse_number(num_buf) catch |err| {
                std.debug.panic("Failed with {s}, num_buf: '{s}'", .{ @errorName(err), num_buf });
            }
        else
            parse_number(num_buf);

        const get = map.getOrPutAssumeCapacity(name);

        if (get.found_existing) {
            get.value_ptr.addMeasurement(num);
        } else {
            get.value_ptr.* = Station{ .min = num, .max = num, .sum = num, .count = 1 };
        }
    }

    if (runtime_safety) return line_no;
}

const ParseNumberError = error{
    @"Number Too Short",
    @"Number Too Long",
    @"Number Without Period",
    @"Non-Number Characters",
};

pub fn parse_number(str: []const u8) if (runtime_safety) ParseNumberError!i32 else i32 {
    if (runtime_safety) {
        if (str.len < 3) return error.@"Number Too Short";
        if (str.len > 5) return error.@"Number Too Long";
        if (mem.count(u8, str, ".") != 1) return error.@"Number Without Period";
        for (str) |char| {
            switch (char) {
                '0'...'9', '.', '-' => {},
                else => return error.@"Non-Number Characters",
            }
        }
    }

    const negative = str[0] == '-';

    var res = @as(i32, str[str.len - 1] - '0');
    res += @as(i32, str[str.len - 3] - '0') * 10;

    if ((str.len == 4 and !negative) or (str.len == 5)) {
        res += @as(i32, str[str.len - 4] - '0') * 100;
    }

    if (negative) res *= -1;

    return res;
}

test parse_number {
    const input = [_][]const u8{ "5.9", "-8.2", "99.0", "-96.1" };
    const output = [_]i64{ 59, -82, 990, -961 };

    for (input, output) |in, out| {
        if (runtime_safety) {
            try std.testing.expect(try parse_number(in) == out);
        } else {
            try std.testing.expect(parse_number(in) == out);
        }
    }
}

const Station = struct {
    min: i32,
    max: i32,
    sum: i64,
    count: u64,

    pub fn merge(self: *@This(), other: *const @This()) void {
        self.min = @min(self.min, other.min);
        self.max = @max(self.max, other.max);
        self.sum += other.sum;
        self.count += other.count;
    }

    pub fn addMeasurement(self: *@This(), num: i32) void {
        self.min = @min(self.min, num);
        self.max = @max(self.max, num);
        self.sum += num;
        self.count += 1;
    }

    pub fn print(self: @This(), stdout: anytype) !void {
        const min_base = @divTrunc(self.min, 10);
        const min_frac: u4 = @intCast(try std.math.absInt(@rem(self.min, 10)));

        try stdout.print("{}.{}/", .{ min_base, min_frac });

        // average is multiplied by 10 to round the last digit off later on
        const average = @divTrunc(self.sum * 10, @as(i64, @intCast(self.count)));

        var average_base = @divTrunc(average, 100);

        // print negative if the base wouldn't print it out
        if (average_base == 0 and average < 0) try stdout.print("-", .{});

        const average_frac: u7 = blk: {
            const whole_frac = try std.math.absInt(@rem(average, 100));

            var base = @as(u7, @intCast(@divTrunc(whole_frac, 10)));
            const round_up = @rem(whole_frac, 10) >= 5;

            base += @intFromBool(round_up);
            assert(base <= 10);

            if (base == 10) {
                average_base += 1;
                break :blk 0;
            }

            break :blk base;
        };
        try stdout.print("{}.{}/", .{ average_base, average_frac });

        const max_base = @divTrunc(self.max, 10);
        const max_frac: u4 = @intCast(try std.math.absInt(@rem(self.max, 10)));

        try stdout.print("{}.{}", .{ max_base, max_frac });
    }
};

//// Options
const runtime_safety = std.debug.runtime_safety;
const alloc_ahead = true;

const HashMap = std.StringHashMapUnmanaged(Station);
/// the number of unique possible names
const MAX_UNIQUE_NAMES = 10_000;
/// MAX_UNIQUE_NAMES + 33%, so that the hashmaps only fill up to 80% at most
const HASH_MAP_SIZE = (MAX_UNIQUE_NAMES * 4) / 3;

const builtin = @import("builtin");
const std = @import("std");
const os = std.os;
const mem = std.mem;

const Thread = std.Thread;

const log = std.log.scoped(.@"1brz");
const assert = std.debug.assert;
