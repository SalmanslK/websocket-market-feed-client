using System.Net.WebSockets;

namespace MarketFeed;

/// <summary>
/// Everything that differs between one feed provider and another: how to log in, what to send,
/// and how to read what comes back. The connection, caching and reconnection are generic.
/// </summary>
/// <typeparam name="T">The snapshot type kept per key (an instrument, market or symbol).</typeparam>
public interface IFeedProtocol<T>
{
    /// <summary>
    /// Called before every connection attempt. Returns the session token the other members receive,
    /// or <see langword="null"/> when the feed needs none.
    /// </summary>
    Task<string?> AuthenticateAsync(CancellationToken cancellationToken) => Task.FromResult<string?>(null);

    /// <summary>Sets request headers or other socket options before connecting.</summary>
    void ConfigureSocket(ClientWebSocketOptions options, string? token)
    {
    }

    /// <summary>The first message sent after connecting, or <see langword="null"/> for none.</summary>
    string? HandshakeMessage(string? token) => null;

    /// <summary>The message that subscribes to <paramref name="keys"/>.</summary>
    string SubscribeMessage(IReadOnlyCollection<string> keys, string? token);

    /// <summary>The message that unsubscribes from <paramref name="keys"/>, or <see langword="null"/> if the feed has none.</summary>
    string? UnsubscribeMessage(IReadOnlyCollection<string> keys, string? token) => null;

    /// <summary>
    /// The updates carried by one incoming message, each with the key it belongs to.
    /// Heartbeats and acknowledgements yield nothing.
    /// </summary>
    IEnumerable<(string Key, T Update)> ParseMessage(string message);
}
