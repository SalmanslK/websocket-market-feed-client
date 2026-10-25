using System.Text.Json.Nodes;
using MarketFeed;
using MarketFeed.Sample;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddHttpClient();
builder.Services.Configure<FeedAuthOptions>(builder.Configuration.GetSection("FeedAuth"));
builder.Services.AddMarketFeed<JsonObject, DemoProtocol, JsonDeltaMerger>(builder.Configuration.GetSection("Feed"));

var app = builder.Build();
app.UseWebSockets();

// The first request for a market subscribes to it; later ones read the cached, merged snapshot.
app.MapGet("/markets/{id}", async (string id, IMarketFeed<JsonObject> feed, CancellationToken cancellationToken) =>
    await feed.GetAsync(id, cancellationToken) is { } snapshot ? Results.Ok(snapshot) : Results.NotFound());

app.MapGet("/status", (IMarketFeed<JsonObject> feed) => new { feed.IsConnected, Subscribed = feed.SubscribedKeys });

// A stand-in feed so the sample runs on its own. Point Feed:Url at a real provider instead.
app.Map("/demo-feed", DemoFeedServer.HandleAsync);

app.Run();
