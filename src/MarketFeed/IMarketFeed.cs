namespace MarketFeed;

/// <summary>Reads the latest snapshot for a key, subscribing to it on first use.</summary>
public interface IMarketFeed<T> where T : class
{
    /// <summary>
    /// The latest snapshot for <paramref name="key"/>. The first request for a key subscribes to it and waits
    /// up to <see cref="FeedOptions.FirstSnapshotTimeout"/>; returns <see langword="null"/> if nothing arrived.
    /// </summary>
    Task<T?> GetAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>The keys currently subscribed.</summary>
    IReadOnlyCollection<string> SubscribedKeys { get; }

    /// <summary>Whether the feed's socket is open right now.</summary>
    bool IsConnected { get; }
}
