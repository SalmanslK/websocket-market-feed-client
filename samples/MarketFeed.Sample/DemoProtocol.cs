using System.Net.WebSockets;
using System.Text.Json.Nodes;
using Microsoft.Extensions.Options;

namespace MarketFeed.Sample;

/// <summary>Login settings for feeds that hand out a session token. Keep the password in user secrets.</summary>
public sealed class FeedAuthOptions
{
    /// <summary>The login endpoint; leave empty for feeds without authentication.</summary>
    public string? LoginUrl { get; set; }

    public string? Username { get; set; }

    public string? Password { get; set; }

    /// <summary>Extra headers sent on login and on the WebSocket request, such as an application key.</summary>
    public Dictionary<string, string> Headers { get; set; } = [];
}

/// <summary>
/// A simple JSON protocol: the client sends <c>{"type":"subscribe","keys":[...]}</c> (and <c>unsubscribe</c>),
/// the feed answers with <c>{"type":"update","items":[{"id":...}, ...]}</c> — a full snapshot first, deltas after.
/// Adapt these few methods to a real provider's message format.
/// </summary>
public sealed class DemoProtocol(IHttpClientFactory httpClients, IOptions<FeedAuthOptions> authOptions) : IFeedProtocol<JsonObject>
{
    private readonly FeedAuthOptions _auth = authOptions.Value;

    public async Task<string?> AuthenticateAsync(CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(_auth.LoginUrl))
        {
            return null;
        }

        using var client = httpClients.CreateClient();
        using var request = new HttpRequestMessage(HttpMethod.Post, _auth.LoginUrl)
        {
            Content = JsonContent.Create(new { username = _auth.Username, password = _auth.Password }),
        };
        foreach (var (name, value) in _auth.Headers)
        {
            request.Headers.TryAddWithoutValidation(name, value);
        }

        using var response = await client.SendAsync(request, cancellationToken);
        response.EnsureSuccessStatusCode();
        var body = await response.Content.ReadFromJsonAsync<JsonObject>(cancellationToken);
        return (string?)body?["token"] ?? throw new InvalidOperationException("The login response did not include a token.");
    }

    public void ConfigureSocket(ClientWebSocketOptions options, string? token)
    {
        foreach (var (name, value) in _auth.Headers)
        {
            options.SetRequestHeader(name, value);
        }
    }

    public string? HandshakeMessage(string? token) =>
        token is null ? null : new JsonObject { ["type"] = "auth", ["token"] = token }.ToJsonString();

    public string SubscribeMessage(IReadOnlyCollection<string> keys, string? token) => KeysMessage("subscribe", keys);

    public string? UnsubscribeMessage(IReadOnlyCollection<string> keys, string? token) => KeysMessage("unsubscribe", keys);

    public IEnumerable<(string Key, JsonObject Update)> ParseMessage(string message)
    {
        if (JsonNode.Parse(message) is not JsonObject envelope
            || (string?)envelope["type"] != "update"
            || envelope["items"] is not JsonArray items)
        {
            yield break;
        }

        foreach (var item in items.OfType<JsonObject>())
        {
            if ((string?)item["id"] is { } id)
            {
                yield return (id, (JsonObject)item.DeepClone());
            }
        }
    }

    private static string KeysMessage(string type, IReadOnlyCollection<string> keys) =>
        new JsonObject { ["type"] = type, ["keys"] = new JsonArray([.. keys.Select(key => JsonValue.Create(key))]) }.ToJsonString();
}
