using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MarketFeed;

/// <summary>
/// Keeps one WebSocket connection to a market-data feed and an in-memory snapshot per subscribed key.
/// Keys are subscribed on first request and dropped once nobody asks for them; the connection is
/// re-established with backoff and every live key is resubscribed after a reconnect.
/// </summary>
public sealed class MarketFeed<T> : BackgroundService, IMarketFeed<T> where T : class
{
    private readonly IFeedProtocol<T> _protocol;
    private readonly IUpdateMerger<T> _merger;
    private readonly FeedOptions _options;
    private readonly Uri _url;
    private readonly TimeProvider _time;
    private readonly ILogger<MarketFeed<T>> _logger;
    private readonly ConcurrentDictionary<string, Entry> _entries = new();
    private readonly SemaphoreSlim _sendLock = new(1, 1);

    // Set only once the handshake is sent, so requests never subscribe ahead of authentication.
    private volatile ClientWebSocket? _socket;
    private volatile string? _token;

    public MarketFeed(
        IFeedProtocol<T> protocol,
        IUpdateMerger<T> merger,
        IOptions<FeedOptions> options,
        TimeProvider time,
        ILogger<MarketFeed<T>> logger)
    {
        _protocol = protocol;
        _merger = merger;
        _options = options.Value;
        _url = _options.Url ?? throw new InvalidOperationException("The feed URL (FeedOptions.Url) is not configured.");
        _time = time;
        _logger = logger;
    }

    public IReadOnlyCollection<string> SubscribedKeys => _entries.Keys.ToArray();

    public bool IsConnected => _socket?.State == WebSocketState.Open;

    public async Task<T?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);

        var created = new Entry();
        var entry = _entries.GetOrAdd(key, created);
        entry.Touch(_time.GetUtcNow());

        if (ReferenceEquals(entry, created))
        {
            // When disconnected, the reconnect subscribes every key in the cache, this one included.
            await SendIfConnectedAsync(_protocol.SubscribeMessage([key], _token), cancellationToken);
        }

        if (entry.Value is { } value)
        {
            return value;
        }

        try
        {
            return await entry.FirstValue.Task.WaitAsync(_options.FirstSnapshotTimeout, _time, cancellationToken);
        }
        catch (TimeoutException)
        {
            return null;
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var sweeping = SweepIdleKeysAsync(stoppingToken);
        var delay = _options.ReconnectDelay;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var socket = await ConnectAsync(stoppingToken);
                delay = _options.ReconnectDelay;
                await ReceiveAsync(socket, stoppingToken);
                _logger.LogWarning("The feed closed the connection; reconnecting in {Delay}.", delay);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "The feed connection failed; reconnecting in {Delay}.", delay);
            }
            finally
            {
                _socket = null;
            }

            try
            {
                await Task.Delay(delay, _time, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, _options.MaxReconnectDelay.Ticks));
        }

        await sweeping;
    }

    public override void Dispose()
    {
        base.Dispose();
        _sendLock.Dispose();
    }

    private async Task<ClientWebSocket> ConnectAsync(CancellationToken cancellationToken)
    {
        _token = await _protocol.AuthenticateAsync(cancellationToken);

        var socket = new ClientWebSocket();
        try
        {
            _protocol.ConfigureSocket(socket.Options, _token);
            await socket.ConnectAsync(_url, cancellationToken);

            if (_protocol.HandshakeMessage(_token) is { } handshake)
            {
                await SendAsync(socket, handshake, cancellationToken);
            }

            _socket = socket;
            var keys = _entries.Keys.ToArray();
            if (keys.Length > 0)
            {
                await SendAsync(socket, _protocol.SubscribeMessage(keys, _token), cancellationToken);
            }

            _logger.LogInformation("Connected to {Url}; {Count} keys subscribed.", _url, keys.Length);
            return socket;
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    private async Task ReceiveAsync(ClientWebSocket socket, CancellationToken cancellationToken)
    {
        var buffer = new byte[16 * 1024];
        using var message = new MemoryStream();

        while (socket.State == WebSocketState.Open)
        {
            // A message can arrive in several frames; collect them all before parsing.
            message.SetLength(0);
            ValueWebSocketReceiveResult frame;
            do
            {
                frame = await socket.ReceiveAsync(buffer.AsMemory(), cancellationToken);
                if (frame.MessageType == WebSocketMessageType.Close)
                {
                    await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, null, CancellationToken.None);
                    return;
                }
                message.Write(buffer, 0, frame.Count);
            }
            while (!frame.EndOfMessage);

            Dispatch(Encoding.UTF8.GetString(message.GetBuffer(), 0, (int)message.Length));
        }
    }

    private void Dispatch(string message)
    {
        List<(string Key, T Update)> updates;
        try
        {
            updates = _protocol.ParseMessage(message).ToList();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Skipped a feed message that could not be parsed.");
            return;
        }

        foreach (var (key, update) in updates)
        {
            // Updates for keys already dropped are late arrivals; ignore them.
            if (_entries.TryGetValue(key, out var entry))
            {
                entry.Apply(update, _merger);
            }
        }
    }

    private async Task SweepIdleKeysAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(_options.SweepInterval, _time);
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                var cutoff = _time.GetUtcNow() - _options.IdleTimeout;
                var idle = _entries.Where(pair => pair.Value.LastAccess < cutoff).Select(pair => pair.Key).ToArray();
                if (idle.Length == 0)
                {
                    continue;
                }

                foreach (var key in idle)
                {
                    _entries.TryRemove(key, out _);
                }
                if (_protocol.UnsubscribeMessage(idle, _token) is { } unsubscribe)
                {
                    await SendIfConnectedAsync(unsubscribe, cancellationToken);
                }
                _logger.LogInformation("Dropped {Count} idle keys.", idle.Length);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task SendIfConnectedAsync(string message, CancellationToken cancellationToken)
    {
        var socket = _socket;
        if (socket?.State != WebSocketState.Open)
        {
            return;
        }

        try
        {
            await SendAsync(socket, message, cancellationToken);
        }
        catch (WebSocketException ex)
        {
            // The receive loop sees the same failure and reconnects, resubscribing everything.
            _logger.LogDebug(ex, "A send failed on a closing connection.");
        }
    }

    private async Task SendAsync(ClientWebSocket socket, string message, CancellationToken cancellationToken)
    {
        // A WebSocket allows one send at a time.
        await _sendLock.WaitAsync(cancellationToken);
        try
        {
            await socket.SendAsync(Encoding.UTF8.GetBytes(message), WebSocketMessageType.Text, endOfMessage: true, cancellationToken);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    private sealed class Entry
    {
        private readonly Lock _gate = new();
        private T? _value;
        private long _lastAccessTicks;

        public TaskCompletionSource<T> FirstValue { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public T? Value => Volatile.Read(ref _value);

        public DateTimeOffset LastAccess => new(Volatile.Read(ref _lastAccessTicks), TimeSpan.Zero);

        public void Touch(DateTimeOffset now) => Volatile.Write(ref _lastAccessTicks, now.UtcTicks);

        public void Apply(T update, IUpdateMerger<T> merger)
        {
            T merged;
            lock (_gate)
            {
                merged = _value is null ? update : merger.Merge(_value, update);
                Volatile.Write(ref _value, merged);
            }
            FirstValue.TrySetResult(merged);
        }
    }
}
