//! Io.Select() Pattern - Subscription with Timeout
//!
//! Demonstrates io.select() to race a subscription receive against a timeout.
//! This is the correct use case for io.select() with NATS - racing ONE
//! subscription against a non-resource operation like sleep.
//!
//! NOTE: Do NOT use io.select() to race multiple subscriptions - cancelling
//! a subscription future discards any message it received. Use polling or
//! io.concurrent() + Io.Queue instead (see multi_sub.zig, multi_sub_async.zig).
//!
//! Run with: zig build example-select
//!
//! Prerequisites: nats-server running on localhost:4222

const std = @import("std");
const nats = @import("nats");

const Io = std.Io;
const Sub = nats.Client.Sub;
const Message = nats.Client.Message;

/// Sleep function compatible with io.async()
fn sleepMs(io: Io, ms: i64) void {
    io.sleep(.fromMilliseconds(ms), .awake) catch {};
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    const client = try nats.Client.connect(
        allocator,
        io,
        "nats://localhost:4222",
        .{ .name = "select-example" },
    );
    defer client.deinit();

    std.debug.print("Connected to NATS!\n", .{});

    const sub = try client.subscribeSync("demo.select");
    defer sub.deinit();

    std.debug.print("Subscribed to 'demo.select'\n", .{});
    std.debug.print("\nPublishing 3 messages with 200ms gaps...\n", .{});
    std.debug.print("Using 500ms timeout - should receive all 3.\n\n", .{});

    // Spawn publisher in background
    var publisher = io.async(publishMessages, .{ client, io });
    defer publisher.cancel(io);

    // Receive with timeout using io.select()
    var received: u32 = 0;
    const max_attempts = 5;

    for (0..max_attempts) |attempt| {
        const Result = union(enum) {
            message: anyerror!Message,
            timeout: void,
        };
        var result_buf: [2]Result = undefined;
        var select = Io.Select(Result).init(io, &result_buf);
        defer select.cancelDiscard();

        _ = select.async(.message, Sub.nextMsg, .{sub});
        _ = select.async(.timeout, sleepMs, .{ io, 500 });

        const completed = select.await() catch |err| {
            if (err == error.Canceled) break;
            std.debug.print("  Select error: {}\n", .{err});
            break;
        };

        switch (completed) {
            .message => |msg_result| {
                if (msg_result) |msg| {
                    defer msg.deinit();
                    received += 1;
                    std.debug.print("  [{d}] Received: {s}\n", .{ attempt + 1, msg.data });
                } else |err| {
                    std.debug.print("  [{d}] Receive error: {}\n", .{ attempt + 1, err });
                }
            },
            .timeout => {
                std.debug.print("  [{d}] Timeout - no message\n", .{attempt + 1});
            },
        }
    }

    std.debug.print("\nReceived {d} messages in {d} attempts.\n", .{
        received,
        max_attempts,
    });
    std.debug.print("Done!\n", .{});
}

fn publishMessages(
    client: *nats.Client,
    io: Io,
) void {
    io.sleep(.fromMilliseconds(100), .awake) catch {};

    for (1..4) |i| {
        var buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "Message {d}", .{i}) catch "Msg";
        client.publish("demo.select", msg) catch return;
        io.sleep(.fromMilliseconds(200), .awake) catch {};
    }
}
