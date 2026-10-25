# WebSocket Market Feed Client

A small .NET 10 library for serving live market data from a WebSocket feed through your own API,
without holding a subscription to everything the provider offers.

```
GET /markets/EURUSD   →  first call subscribes, waits for the snapshot, returns it
GET /markets/EURUSD   →  later calls return the cached snapshot, kept current by the feed
(10 min unused)       →  unsubscribed and dropped from the cache
```

## What it does

- **Subscribes on demand.** The first request for a key subscribes to it and waits briefly for its first snapshot.
- **Merges deltas.** Feeds usually send a full snapshot once and then only what changed; each update is
  folded into the cached snapshot (`JsonDeltaMerger` handles the common JSON shape: missing/`null` = unchanged,
  arrays of objects merged by `id`).
- **Drops idle keys.** Keys nobody has asked for within `IdleTimeout` are unsubscribed and evicted.
- **Heals itself.** On a dropped connection it re-authenticates, reconnects with exponential backoff
  and resubscribes every live key.
- **Is provider-neutral.** Everything specific to a provider — login, headers, message formats — sits in one
  small `IFeedProtocol<T>` implementation.

## Use it

```csharp
builder.Services.AddMarketFeed<JsonObject, MyProviderProtocol, JsonDeltaMerger>(builder.Configuration.GetSection("Feed"));

app.MapGet("/markets/{id}", async (string id, IMarketFeed<JsonObject> feed, CancellationToken ct) =>
    await feed.GetAsync(id, ct) is { } snapshot ? Results.Ok(snapshot) : Results.NotFound());
```

```json
"Feed": {
  "Url": "wss://stream.example.com/ws",
  "FirstSnapshotTimeout": "00:00:03",
  "IdleTimeout": "00:10:00"
}
```

Implement `IFeedProtocol<T>` for your provider: `SubscribeMessage` and `ParseMessage` are required;
`AuthenticateAsync`, `ConfigureSocket`, `HandshakeMessage` and `UnsubscribeMessage` are optional.
Use your own snapshot type `T` with an `IUpdateMerger<T>` if you prefer typed models over `JsonObject`.

## Run the sample

The sample API includes a fake price feed, so it runs with nothing else installed:

```
dotnet run --project samples/MarketFeed.Sample
curl http://localhost:5080/markets/EURUSD
curl http://localhost:5080/status
```

For a real provider, set `Feed:Url`, adapt `DemoProtocol`, and keep credentials out of the repository:

```
dotnet user-secrets set "FeedAuth:Password" "<password>" --project samples/MarketFeed.Sample
dotnet user-secrets set "FeedAuth:Headers:X-Application" "<app key>" --project samples/MarketFeed.Sample
```

## Layout

| Path | What it is |
|---|---|
| `src/MarketFeed` | The library: `MarketFeed<T>` (connection, cache, reconnect, idle sweep), `IFeedProtocol<T>`, `JsonDeltaMerger` |
| `samples/MarketFeed.Sample` | Minimal API, a sample protocol with optional token login, and a fake feed |
| `tests/MarketFeed.Tests` | Merge rules, plus end-to-end tests against a real local WebSocket server (subscribe, merge, timeout, reconnect, idle drop) |

```
dotnet test
```
