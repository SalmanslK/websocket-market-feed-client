using System.Text.Json.Nodes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace MarketFeed.Tests;

public class MarketFeedTests
{
    [Fact]
    public async Task The_first_request_subscribes_and_returns_the_first_snapshot()
    {
        await using var server = await FakeFeedServer.StartAsync();
        using var feed = await StartFeedAsync(server.Url);
        await server.WaitForConnectionAsync(1);

        var request = feed.GetAsync("A");
        var subscribe = await server.NextMessageAsync("subscribe");
        await server.SendAsync(Update("""{"id":"A","last":10,"status":"open"}"""));
        var snapshot = await request;

        Assert.Equal("""["A"]""", subscribe["keys"]!.ToJsonString());
        Assert.Equal(10, (int)snapshot!["last"]!);
        Assert.Equal(["A"], feed.SubscribedKeys);
    }

    [Fact]
    public async Task Later_updates_are_merged_into_the_snapshot()
    {
        await using var server = await FakeFeedServer.StartAsync();
        using var feed = await StartFeedAsync(server.Url);
        await server.WaitForConnectionAsync(1);
        var first = feed.GetAsync("A");
        await server.NextMessageAsync("subscribe");
        await server.SendAsync(Update("""{"id":"A","last":10,"status":"open","levels":[{"id":1,"bid":9}]}"""));
        await first;

        await server.SendAsync(Update("""{"id":"A","last":11,"levels":[{"id":1,"bid":10}]}"""));
        var snapshot = await EventuallyAsync(() => feed.GetAsync("A"), s => (int?)s?["last"] == 11);

        Assert.Equal("open", (string?)snapshot!["status"]);
        Assert.Equal(10, (int)snapshot["levels"]![0]!["bid"]!);
    }

    [Fact]
    public async Task A_request_answers_null_when_no_snapshot_arrives_in_time()
    {
        await using var server = await FakeFeedServer.StartAsync();
        using var feed = await StartFeedAsync(server.Url, options => options.FirstSnapshotTimeout = TimeSpan.FromMilliseconds(200));
        await server.WaitForConnectionAsync(1);

        Assert.Null(await feed.GetAsync("A"));
    }

    [Fact]
    public async Task After_a_dropped_connection_it_reconnects_and_resubscribes()
    {
        await using var server = await FakeFeedServer.StartAsync();
        using var feed = await StartFeedAsync(server.Url);
        await server.WaitForConnectionAsync(1);
        var first = feed.GetAsync("A");
        await server.NextMessageAsync("subscribe");
        await server.SendAsync(Update("""{"id":"A","last":10}"""));
        await first;

        server.DropConnection();
        await server.WaitForConnectionAsync(2);
        var resubscribe = await server.NextMessageAsync("subscribe");
        await server.SendAsync(Update("""{"id":"A","last":12}"""));
        var snapshot = await EventuallyAsync(() => feed.GetAsync("A"), s => (int?)s?["last"] == 12);

        Assert.Equal("""["A"]""", resubscribe["keys"]!.ToJsonString());
        Assert.Equal(12, (int)snapshot!["last"]!);
    }

    [Fact]
    public async Task Keys_nobody_asks_for_are_unsubscribed_and_dropped()
    {
        await using var server = await FakeFeedServer.StartAsync();
        using var feed = await StartFeedAsync(server.Url, options =>
        {
            options.IdleTimeout = TimeSpan.FromMilliseconds(300);
            options.SweepInterval = TimeSpan.FromMilliseconds(50);
        });
        await server.WaitForConnectionAsync(1);
        var first = feed.GetAsync("A");
        await server.NextMessageAsync("subscribe");
        await server.SendAsync(Update("""{"id":"A","last":10}"""));
        await first;

        var unsubscribe = await server.NextMessageAsync("unsubscribe");

        Assert.Equal("""["A"]""", unsubscribe["keys"]!.ToJsonString());
        Assert.Empty(feed.SubscribedKeys);
    }

    private static async Task<MarketFeed<JsonObject>> StartFeedAsync(Uri url, Action<FeedOptions>? configure = null)
    {
        var options = new FeedOptions
        {
            Url = url,
            FirstSnapshotTimeout = TimeSpan.FromSeconds(10),
            ReconnectDelay = TimeSpan.FromMilliseconds(50),
        };
        configure?.Invoke(options);

        var feed = new MarketFeed<JsonObject>(
            new TestProtocol(), new JsonDeltaMerger(), Options.Create(options), TimeProvider.System,
            NullLogger<MarketFeed<JsonObject>>.Instance);
        await feed.StartAsync(CancellationToken.None);
        return feed;
    }

    private static async Task<JsonObject?> EventuallyAsync(Func<Task<JsonObject?>> read, Func<JsonObject?, bool> done)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (true)
        {
            var value = await read();
            if (done(value) || DateTime.UtcNow > deadline)
            {
                return value;
            }
            await Task.Delay(20);
        }
    }

    private static string Update(string item) => $$"""{"type":"update","items":[{{item}}]}""";

    private sealed class TestProtocol : IFeedProtocol<JsonObject>
    {
        public string SubscribeMessage(IReadOnlyCollection<string> keys, string? token) => Keys("subscribe", keys);

        public string? UnsubscribeMessage(IReadOnlyCollection<string> keys, string? token) => Keys("unsubscribe", keys);

        public IEnumerable<(string Key, JsonObject Update)> ParseMessage(string message)
        {
            var envelope = JsonNode.Parse(message)!;
            foreach (var item in envelope["items"]!.AsArray().OfType<JsonObject>())
            {
                yield return ((string)item["id"]!, (JsonObject)item.DeepClone());
            }
        }

        private static string Keys(string type, IReadOnlyCollection<string> keys) =>
            new JsonObject { ["type"] = type, ["keys"] = new JsonArray([.. keys.Select(key => JsonValue.Create(key))]) }.ToJsonString();
    }
}
