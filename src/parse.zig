const ParseNumberError = error{
    @"Number Too Short",
    @"Number Too Long",
    @"Number without Period",
    @"Non-Number Characters",
};

pub fn parse_number(str: []const u8) ParseNumberError!i64 {
    if (runtime_safety) {
        if (str.len < 3) return error.@"Number Too Short";
        if (str.len > 5) return error.@"Number Too Long";
        if (mem.count(u8, str, ".") != 1) return error.@"Number without Period";
    }

    const negative = str[0] == '-';

    var res: i64 = 0;

    const start = @intFromBool(negative); // skip the negative sign if it exists.

    for (str[start..]) |char| {
        if (char == '.') continue;

        if (runtime_safety and char < '0' or char > '9') return error.@"Non-Number Characters";

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
        try testing.expect(try parse_number(in) == out);
    }
}

const ParseLineError = error{
    @"Line Too Short",
    @"No Semicolon",
    @"Too Many Semicolons",
    @"No Name",
} || ParseNumberError;

pub fn parse_line(map: *HashMap, line: []const u8) ParseLineError!void {
    if (runtime_safety) {
        if (line.len < 6) return error.@"Line Too Short";

        const semi_count = mem.count(u8, line, ";");
        if (semi_count > 1) return error.@"Too Many Semicolons";
        if (semi_count < 1) return error.@"No Semicolon";
    }

    const semi_pos = mem.indexOfScalar(u8, line, ';').?;

    if (runtime_safety and semi_pos == 0) return error.@"No Name";

    const name = line[0..semi_pos];
    const num = try parse_number(line[semi_pos + 1 ..]);

    map.mutex.lock();
    defer map.mutex.unlock();
    const get = map.map.getOrPutAssumeCapacity(name);

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

test parse_line {
    const input = [_][]const u8{ "test;10.0", "test;20.0", "aaa;-19.0", "bela;-9.0", "lel'ob;2.5", "aaa;20.1", "test;-10.0" };

    var names = [_][]const u8{ "test", "aaa", "bela", "lel'ob" };
    var values = [_]Station{ .{
        .min = -100,
        .max = 200,
        .sum = 200,
        .count = 3,
    }, .{
        .min = -190,
        .max = 201,
        .sum = 11,
        .count = 2,
    }, .{
        .min = -90,
        .max = -90,
        .sum = -90,
        .count = 1,
    }, .{
        .min = 25,
        .max = 25,
        .sum = 25,
        .count = 1,
    } };

    var map = HashMap{};
    defer map.map.deinit(testing.allocator);
    try map.map.ensureTotalCapacity(testing.allocator, 4);

    for (input) |in| try parse_line(&map, in);

    try testing.expect(map.map.count() == names.len);

    for (names, values) |name, value| {
        const en = map.map.getPtr(name) orelse return error.@"Name not found in map";
        try testing.expect(std.meta.eql(value, en.*));
    }
}

pub const HashMap = struct {
    mutex: std.Thread.Mutex = .{},
    map: HashMapContent = .{},
};
pub const HashMapContent = std.StringArrayHashMapUnmanaged(Station);

pub const Station = struct {
    min: i64,
    max: i64,
    sum: i64,
    count: u64,
};

const std = @import("std");
const debug = std.debug;
const mem = std.mem;
const testing = std.testing;

const assert = std.debug.assert;
const panic = std.debug.panic;
const runtime_safety = std.debug.runtime_safety;
