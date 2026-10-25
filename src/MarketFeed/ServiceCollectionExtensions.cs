using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace MarketFeed;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers a feed of <typeparamref name="T"/> snapshots: <see cref="IMarketFeed{T}"/> for readers,
    /// and the hosted service that keeps the connection alive.
    /// </summary>
    public static IServiceCollection AddMarketFeed<T, TProtocol, TMerger>(this IServiceCollection services, IConfiguration section)
        where T : class
        where TProtocol : class, IFeedProtocol<T>
        where TMerger : class, IUpdateMerger<T>
    {
        services.Configure<FeedOptions>(section);
        services.TryAddSingleton(TimeProvider.System);
        services.AddSingleton<IFeedProtocol<T>, TProtocol>();
        services.AddSingleton<IUpdateMerger<T>, TMerger>();
        services.AddSingleton<MarketFeed<T>>();
        services.AddSingleton<IMarketFeed<T>>(provider => provider.GetRequiredService<MarketFeed<T>>());
        services.AddHostedService(provider => provider.GetRequiredService<MarketFeed<T>>());
        return services;
    }
}
