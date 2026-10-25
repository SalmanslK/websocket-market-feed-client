namespace MarketFeed;

/// <summary>Folds a partial update (a delta) into the cached snapshot.</summary>
public interface IUpdateMerger<T>
{
    /// <summary>
    /// Returns the new snapshot. Must not modify <paramref name="current"/>: readers may be holding it.
    /// </summary>
    T Merge(T current, T update);
}
