namespace MarketFeed;

/// <summary>Connection and caching settings, bound from configuration (for example the <c>Feed</c> section).</summary>
public sealed class FeedOptions
{
    /// <summary>The feed's WebSocket address, such as <c>wss://stream.example.com/ws</c>.</summary>
    public Uri? Url { get; set; }

    /// <summary>How long a request for a newly subscribed key waits for its first snapshot.</summary>
    public TimeSpan FirstSnapshotTimeout { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>A key nobody has asked for in this long is unsubscribed and dropped from the cache.</summary>
    public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>How often idle keys are looked for.</summary>
    public TimeSpan SweepInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>The first wait before reconnecting; it doubles after each failed attempt.</summary>
    public TimeSpan ReconnectDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>The longest wait between reconnection attempts.</summary>
    public TimeSpan MaxReconnectDelay { get; set; } = TimeSpan.FromSeconds(30);
}
