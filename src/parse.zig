pub fn parse_number(str: []const u8) i64 {
    assert(str.len >= 3); // smallest number is 0.0
    assert(str.len <= 5); // largest number is -00.0
    if (std.debug.runtime_safety) assert(mem.count(u8, str, ".") == 1);

    const negative = str[0] == '-';

    var res: i64 = 0;

    const start = @intFromBool(negative); // skip the negative sign if it exists.

    for (str[start..]) |char| {
        if (char == '.') continue;
        assert(char >= '0');
        assert(char <= '9');

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
        try testing.expect(parse_number(in) == out);
    }
}

pub fn parse_line(map: *HashMap, line: []const u8) !void {
    assert(line.len > 5); // assert the string exists

    const semi = mem.indexOfScalar(u8, line, ';').?;
    assert(semi > 1); // if not, the name is too short

    const name = line[0..semi];
    const num = parse_number(line[semi + 1 ..]);

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

    var correct_map = HashMapContent.init(testing.allocator);
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

    var map = HashMapContent.init(testing.allocator);
    defer map.deinit();

    for (input) |in| {
        try parse_line(&map, in);
    }

    try testing.expect(correct_map.count() == map.count());

    var iter = correct_map.iterator();

    while (iter.next()) |entry| {
        const en = map.getPtr(entry.key_ptr.*) orelse try testing.expect(false);
        try testing.expect(en.min == entry.value_ptr.min);
        try testing.expect(en.max == entry.value_ptr.max);
        try testing.expect(en.sum == entry.value_ptr.sum);
        try testing.expect(en.count == entry.value_ptr.count);
    }
}

pub const HashMap = struct {
    mutex: std.Thread.Mutex,
    map: HashMapContent,
};
pub const HashMapContent = std.StringArrayHashMap(Station);

pub const Station = struct {
    min: i64,
    max: i64,
    sum: i64,
    count: u64,
};

const std = @import("std");
const mem = std.mem;
const testing = std.testing;
const assert = std.debug.assert;
