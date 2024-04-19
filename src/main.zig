var line_count: usize = 0;

pub fn main() !void {
    var gpalloc = std.heap.GeneralPurposeAllocator(.{
        .thread_safe = false,
    }){};
    defer _ = gpalloc.deinit();

    var arena = std.heap.ArenaAllocator.init(gpalloc.allocator());
    defer arena.deinit();
    const allocator = arena.allocator();

    //const stdout_file = std.io.getStdOut().writer();
    //var bw = std.io.bufferedWriter(stdout_file);
    //const stdout = bw.writer();
    //_ = stdout;

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    _ = args.next(); // discard program name
    const file_name = args.next() orelse "measurements.txt";

    var file = try std.fs.cwd().openFile(file_name, .{ .mode = .read_only }); // measurements.txt
    defer file.close();

    const file_size = (try file.metadata()).size();

    log.info("File Size: {}", .{file_size});

    const file_contents = try os.mmap(null, file_size, os.PROT.READ, os.MAP.SHARED, file.handle, 0);
    defer os.munmap(file_contents);

    if (runtime_safety) assert(file_contents.len == file_size); // should be OS guaranteed

    const thread_count = (std.Thread.getCpuCount() catch unreachable);

    var thread_list: []Thread = try allocator.alloc(Thread, thread_count - 1);
    var map_list = try allocator.alloc(HashMap, thread_count - 1);
    for (map_list) |*map| {
        map.* = .{};
        try map.ensureTotalCapacity(allocator, HASH_MAP_SIZE);
    }

    const thread_offset = file_size / thread_count;
    var end_prev: usize = 0;

    for (0..thread_count - 1) |idx| {
        const end = mem.indexOfScalarPos(u8, file_contents, end_prev + thread_offset, '\n').?;

        var buffer = file_contents[end_prev..end];
        assert(buffer[0] != '\n'); // make sure newline isn't included
        assert(buffer[buffer.len - 1] != '\n'); // no newline there either

        thread_list[idx] = try Thread.spawn(.{ .allocator = allocator }, process, .{ idx, &map_list[idx], buffer });

        end_prev = end + 1;
    }

    var final_map = HashMap{};
    try final_map.ensureTotalCapacity(allocator, HASH_MAP_SIZE);

    process(thread_count - 1, &final_map, file_contents[end_prev..]);

    for (0..thread_count - 1) |idx| thread_list[idx].join();

    log.info("Lines Processed {}", .{line_count});

    for (map_list) |*map| {
        var iter = map.iterator();
        while (iter.next()) |*item| {
            var got = final_map.getOrPutAssumeCapacity(item.key_ptr.*);

            if (got.found_existing) {
                got.value_ptr.merge(item.value_ptr);
            } else {
                got.value_ptr.* = item.value_ptr.*;
            }
        }
    }
}

pub fn process(
    thread_id: usize,
    map: *HashMap,
    buffer: []const u8,
) void {
    log.info("thread: {:2}, len: {}", .{ thread_id, buffer.len });

    if (runtime_safety) {
        process_buffer(thread_id, map, buffer) catch |err| {
            std.debug.panic("thread: {:2} with {s}", .{ thread_id, @errorName(err) });
        };
    } else {
        process_buffer(thread_id, map, buffer);
    }
}

const ParseLineError = error{
    @"Line Too Short",
    @"No Semicolon",
    @"Too Many Semicolons",
    @"No Name",
    @"Line Contains Newline",
};

pub fn process_buffer(thread_id: usize, map: *HashMap, buffer: []const u8) if (runtime_safety) ParseLineError!void else void {
    var line_no: usize = 1;
    var start_idx: usize = 0;
    while (true) : (line_no += 1) {
        const end_of_line = mem.indexOfScalarPos(u8, buffer, start_idx + 7, '\n') orelse buffer.len;

        const line = buffer[start_idx..end_of_line];

        if (runtime_safety) {
            if (line.len < 6) return error.@"Line Too Short";

            const semi_count = mem.count(u8, line, ";");
            if (semi_count < 1) return error.@"No Semicolon";
            if (semi_count > 1) return error.@"Too Many Semicolons";

            if (mem.count(u8, line, "\n") > 0) return error.@"Line Contains Newline";
        }

        const semi_pos = mem.indexOfScalarPos(u8, line, 3, ';').?;

        if (runtime_safety and semi_pos == 0) return error.@"No Name";

        const name = line[0..semi_pos];
        const num_buf = line[semi_pos + 1 ..];

        const num = parse_number(num_buf) catch |err| {
            std.debug.panic("thread: {:2} with {s}, num_buf: '{s}'", .{ thread_id, @errorName(err), num_buf });
        };

        const get = map.getOrPutAssumeCapacity(name);

        if (get.found_existing) {
            get.value_ptr.addMeasurement(num);
        } else {
            get.value_ptr.* = Station{ .min = num, .max = num, .sum = num, .count = 1 };
        }

        start_idx = end_of_line + 1;
        if (start_idx >= buffer.len) break;
    }

    log.info("lines processed: {}", .{line_no});
    line_count += line_no;
}

const Station = struct {
    min: i32,
    max: i32,
    sum: i64,
    count: usize,

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
};

const ParseNumberError = error{
    @"Number Too Short",
    @"Number Too Long",
    @"Number without Period",
    @"Non-Number Characters",
};

pub fn parse_number(str: []const u8) ParseNumberError!i32 {
    if (runtime_safety) {
        if (str.len < 3) return error.@"Number Too Short";
        if (str.len > 5) return error.@"Number Too Long";
        if (mem.count(u8, str, ".") != 1) return error.@"Number without Period";
        for (str) |char| {
            switch (char) {
                '0'...'9', '.', '-' => {},
                else => return error.@"Non-Number Characters",
            }
        }
    }

    const negative = str[0] == '-';

    var res: i32 = str[str.len - 1] - '0';
    res += (str[str.len - 3] - '0') * 10;

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
        try std.testing.expect(try parse_number(in) == out);
    }
}

const HashMap = std.StringArrayHashMapUnmanaged(Station);
const HASH_MAP_SIZE = 1 << 16;

const std = @import("std");
const os = std.os;
const mem = std.mem;

const Thread = std.Thread;

const log = std.log.scoped(.@"1brz");
const assert = std.debug.assert;
const runtime_safety = std.debug.runtime_safety;
