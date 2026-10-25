using System.Net.WebSockets;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Channels;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace MarketFeed.Tests;

/// <summary>A real WebSocket server on a free local port that records what the client sends and lets a test reply.</summary>
internal sealed class FakeFeedServer : IAsyncDisposable
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    private readonly WebApplication _app;
    private readonly Channel<JsonObject> _received = Channel.CreateUnbounded<JsonObject>();
    private readonly Channel<int> _connections = Channel.CreateUnbounded<int>();
    private int _connectionCount;
    private volatile WebSocket? _socket;

    private FakeFeedServer(WebApplication app)
    {
        _app = app;
        _app.UseWebSockets();
        _app.Map("/feed", HandleAsync);
    }

    public Uri Url { get; private set; } = null!;

    public static async Task<FakeFeedServer> StartAsync()
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();

        var server = new FakeFeedServer(builder.Build());
        await server._app.StartAsync();
        server.Url = new Uri(server._app.Urls.First().Replace("http://", "ws://") + "/feed");
        return server;
    }

    public async Task WaitForConnectionAsync(int number)
    {
        using var timeout = new CancellationTokenSource(Patience);
        while (await _connections.Reader.ReadAsync(timeout.Token) < number)
        {
        }
    }

    /// <summary>The next message of the given type the client sent, skipping others.</summary>
    public async Task<JsonObject> NextMessageAsync(string type)
    {
        using var timeout = new CancellationTokenSource(Patience);
        while (true)
        {
            var message = await _received.Reader.ReadAsync(timeout.Token);
            if ((string?)message["type"] == type)
            {
                return message;
            }
        }
    }

    public Task SendAsync(string json) =>
        _socket!.SendAsync(Encoding.UTF8.GetBytes(json), WebSocketMessageType.Text, true, CancellationToken.None);

    public void DropConnection() => _socket?.Abort();

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }

    private async Task HandleAsync(HttpContext context)
    {
        using var socket = await context.WebSockets.AcceptWebSocketAsync();
        _socket = socket;
        _connections.Writer.TryWrite(Interlocked.Increment(ref _connectionCount));

        var buffer = new byte[64 * 1024];
        try
        {
            while (socket.State == WebSocketState.Open)
            {
                var frame = await socket.ReceiveAsync(buffer, context.RequestAborted);
                if (frame.MessageType == WebSocketMessageType.Close)
                {
                    break;
                }
                _received.Writer.TryWrite(JsonNode.Parse(buffer.AsSpan(0, frame.Count))!.AsObject());
            }
        }
        catch (Exception ex) when (ex is WebSocketException or OperationCanceledException)
        {
        }
    }
}
