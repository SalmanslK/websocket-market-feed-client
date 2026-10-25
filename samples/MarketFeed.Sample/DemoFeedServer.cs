using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json.Nodes;

namespace MarketFeed.Sample;

/// <summary>
/// A fake price feed speaking <see cref="DemoProtocol"/>: a full snapshot for each newly subscribed market,
/// then a small random price move every second. Lets the sample run with no external provider.
/// </summary>
public static class DemoFeedServer
{
    public static async Task HandleAsync(HttpContext context)
    {
        if (!context.WebSockets.IsWebSocketRequest)
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            return;
        }

        using var socket = await context.WebSockets.AcceptWebSocketAsync();
        using var connection = CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted);
        var markets = new ConcurrentDictionary<string, Market>();
        var pushing = PushPricesAsync(socket, markets, connection.Token);

        var buffer = new byte[8 * 1024];
        try
        {
            while (socket.State == WebSocketState.Open)
            {
                var frame = await socket.ReceiveAsync(buffer, connection.Token);
                if (frame.MessageType == WebSocketMessageType.Close)
                {
                    break;
                }

                var message = JsonNode.Parse(buffer.AsSpan(0, frame.Count))!;
                var keys = message["keys"]?.AsArray().Select(key => (string)key!) ?? [];
                foreach (var key in keys)
                {
                    if ((string?)message["type"] == "subscribe")
                    {
                        markets.TryAdd(key, new Market(50 + Random.Shared.NextDouble() * 150));
                    }
                    else if ((string?)message["type"] == "unsubscribe")
                    {
                        markets.TryRemove(key, out _);
                    }
                }
            }
        }
        catch (Exception ex) when (ex is WebSocketException or OperationCanceledException)
        {
        }
        finally
        {
            await connection.CancelAsync();
            await pushing;
        }

        if (socket.State == WebSocketState.CloseReceived)
        {
            await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, null, CancellationToken.None);
        }
    }

    private static async Task PushPricesAsync(WebSocket socket, ConcurrentDictionary<string, Market> markets, CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                var items = new JsonArray();
                foreach (var (id, market) in markets)
                {
                    items.Add(market.NextUpdate(id));
                }
                if (items.Count > 0)
                {
                    var message = new JsonObject { ["type"] = "update", ["items"] = items }.ToJsonString();
                    await socket.SendAsync(Encoding.UTF8.GetBytes(message), WebSocketMessageType.Text, true, cancellationToken);
                }
            }
        }
        catch (Exception ex) when (ex is WebSocketException or OperationCanceledException)
        {
        }
    }

    private sealed class Market(double price)
    {
        private bool _snapshotSent;

        public JsonObject NextUpdate(string id)
        {
            price = Math.Round(price * (1 + (Random.Shared.NextDouble() - 0.5) / 500), 4);
            var update = new JsonObject
            {
                ["id"] = id,
                ["last"] = price,
                ["updated"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                ["levels"] = new JsonArray(
                    new JsonObject { ["id"] = 1, ["bid"] = Math.Round(price * 0.999, 4), ["ask"] = Math.Round(price * 1.001, 4) },
                    new JsonObject { ["id"] = 2, ["bid"] = Math.Round(price * 0.998, 4), ["ask"] = Math.Round(price * 1.002, 4) }),
            };

            // The first message is a full snapshot; later ones leave out what has not changed.
            if (!_snapshotSent)
            {
                update["status"] = "open";
                update["currency"] = "USD";
                _snapshotSent = true;
            }
            return update;
        }
    }
}
