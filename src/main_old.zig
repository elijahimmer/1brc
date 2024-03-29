pub fn main() !void {
    var gpalloc = std.heap.GeneralPurposeAllocator(.{
        .thread_safe = false,
    }){};
    defer _ = gpalloc.deinit();

    const allocator = gpalloc.allocator();

    const stdout_file = std.io.getStdOut().writer();
    var bw = std.io.bufferedWriter(stdout_file);
    const stdout = bw.writer();

    var file = try fs.cwd().openFile("test.txt", .{}); // measurements.txt
    defer file.close();

    var map = HashMap.init(allocator);
    defer map.deinit();

    try map.ensureTotalCapacity(4096); // should be enough storage

    const THREADS = Thread.getCpuCount() catch 1;
    const EACH_BUF_SIZE = 1 << 16;

    var buf_mutex: []Mutex = try allocator.alloc(Mutex, THREADS);
    defer allocator.free(buf_mutex);

    var read_buf: []u8 = try allocator.alloc(u8, EACH_BUF_SIZE * THREADS);
    defer allocator.free(read_buf);

    var pool: Thread.Pool = undefined;

    try Thread.Pool.init(&pool, .{
        .allocator = allocator,
    });
    defer pool.deinit();

    var bytes_read: usize = 0;
    var buf_start: usize = 0;
    var buf_end: usize = buf_start + EACH_BUF_SIZE;
    var leftover: usize = 0;

    while (true) {
        var buf = read_buf[buf_start..buf_end];

        const bytes = try file.read(buf[leftover..]);

        if (bytes == 0 and leftover == 0) break;

        const last_newline = mem.lastIndexOfScalar(u8, buf, '\n').?;

        assert(buf[0..last_newline]);

        try pool.spawn(parse_lines, .{ &map, buf[0..last_newline] });

        bytes_read += bytes;

        buf_start = buf_end;
        buf_end += EACH_BUF_SIZE;

        assert(buf_end - buf_start > 0);
        assert(buf_end <= read_buf.len);

        @memcpy(read_buf[buf_start .. buf_end - last_newline], buf[last_newline..]);
        leftover = buf.len - last_newline;
    }

    const file_bytes = (try file.metadata()).size();

    try stdout.print("file: {}, bytes: {}, map: {}\n", .{ file_bytes, bytes_read, map.count() });
    try bw.flush();

    assert(file_bytes == bytes_read);
}

fn parse_lines(map: *HashMap, str: []const u8) void {
    std.log.info("thread: {}, size: {}", .{ Thread.getCurrentId(), str.len });
    var iter = mem.splitScalar(u8, str, '\n');
    var i: usize = 0;
    while (iter.next()) |line| {
        if (line.len == 0) break;
        i += 1;
        assert(mem.count(u8, line, ";") == 1);
        parse_line(map, line) catch {
            std.log.err("Thread: {}, Failed to parse line: '{}'\n", .{ Thread.getCurrentId(), line });
        };
    }
}

fn parse_number(str: []const u8) i64 {
    assert(str.len >= 3); // smallest number is 0.0
    assert(str.len <= 5); // largest number is -00.0
    assert(mem.count(u8, str, ".") == 1);

    const negative = str[0] == '-';

    var res: i64 = 0;

    const start = @intFromBool(negative); // skip the negative sign if it exists.

    for (str[start..]) |char| {
        if (char == '.') continue;
        assert(char >= '0');

        res *= 10;
        res += char - '0';
    }

    if (negative) res *= -1;

    return res;
}

test parse_number {
    const input = [_][]const u8{ "5.9", "-8.2", "99.0", "-96.1" };
    const output = [_]i64{ 59, -82, 990, -961 };

    for (input, output) |in, out| {
        try std.testing.expect(parse_number(in) == out);
    }
}

fn parse_line(map: *HashMap, line: []const u8) !void {
    assert(line.len >= 0); // assert the string exists

    const semi = mem.indexOfScalar(u8, line, ';').?;

    const name = line[0..semi];
    const num = parse_number(line[semi + 1 ..]);

    const get = map.getOrPutAssumeCapacity(name);

    {
        if (get.found_existing) {
            get.value_ptr.min = @min(get.value_ptr.min, num);
            get.value_ptr.max = @max(get.value_ptr.max, num);
            get.value_ptr.sum += num;
            get.value_ptr.count += 1;
        } else {
            get.value_ptr.min = num;
            get.value_ptr.max = num;
            get.value_ptr.sum = num;
            get.value_ptr.count = 1;
        }
    }
}

test parse_line {
    const input = [_][]const u8{ "test;10.0", "test;20.0", "aaa;-19.0", "bela;-9.0", "lel'ob;2.5", "aaa;20.1", "test;-10.0" };

    var correct_map = HashMap.init(std.testing.allocator);
    defer correct_map.deinit();

    try correct_map.put("test", .{
        .min = -100,
        .max = 200,
        .sum = 200,
        .count = 3,
    });
    try correct_map.put("aaa", .{
        .min = -190,
        .max = 201,
        .sum = 11,
        .count = 2,
    });
    try correct_map.put("bela", .{
        .min = -90,
        .max = -90,
        .sum = -90,
        .count = 1,
    });
    try correct_map.put("lel'ob", .{
        .min = 25,
        .max = 25,
        .sum = 25,
        .count = 1,
    });

    var map = HashMap.init(std.testing.allocator);
    defer map.deinit();

    for (input) |in| {
        try parse_line(&map, in);
    }

    try std.testing.expect(correct_map.count() == map.count());

    var iter = correct_map.iterator();

    while (iter.next()) |entry| {
        const en = map.getPtr(entry.key_ptr.*);
        try std.testing.expect(en != null);
        try std.testing.expect(en.?.min == entry.value_ptr.min);
        try std.testing.expect(en.?.max == entry.value_ptr.max);
        try std.testing.expect(en.?.sum == entry.value_ptr.sum);
        try std.testing.expect(en.?.count == entry.value_ptr.count);
    }
}

const HashMap = std.StringArrayHashMap(Station);

const Station = struct {
    min: i64,
    max: i64,
    sum: i64,
    count: u64,
};

const std = @import("std");
const fs = std.fs;
const mem = std.mem;

const assert = std.debug.assert;

const Thread = std.Thread;
const Mutex = std.Thread.Mutex;
