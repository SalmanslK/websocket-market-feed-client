using System.Text.Json.Nodes;

namespace MarketFeed;

/// <summary>
/// Merges JSON deltas the way most market feeds send them:
/// a missing or <c>null</c> property means "unchanged", nested objects merge recursively,
/// and arrays of objects that carry an id merge item by item (new ids are appended).
/// Any other value replaces the old one.
/// </summary>
public sealed class JsonDeltaMerger : IUpdateMerger<JsonObject>
{
    private readonly string _idProperty;

    public JsonDeltaMerger()
        : this("id")
    {
    }

    public JsonDeltaMerger(string idProperty) => _idProperty = idProperty;

    public JsonObject Merge(JsonObject current, JsonObject update)
    {
        var merged = (JsonObject)current.DeepClone();
        MergeInto(merged, update);
        return merged;
    }

    private void MergeInto(JsonObject target, JsonObject update)
    {
        foreach (var (name, value) in update)
        {
            if (value is null)
            {
                continue;
            }

            if (value is JsonObject updateObject && target[name] is JsonObject targetObject)
            {
                MergeInto(targetObject, updateObject);
            }
            else if (value is JsonArray updateArray && target[name] is JsonArray targetArray
                     && IsKeyed(updateArray) && IsKeyed(targetArray))
            {
                MergeKeyed(targetArray, updateArray);
            }
            else
            {
                target[name] = value.DeepClone();
            }
        }
    }

    private void MergeKeyed(JsonArray target, JsonArray update)
    {
        foreach (var item in update.Cast<JsonObject>())
        {
            var id = item[_idProperty]!.ToJsonString();
            var existing = target.Cast<JsonObject>().FirstOrDefault(candidate => candidate[_idProperty]!.ToJsonString() == id);
            if (existing is null)
            {
                target.Add(item.DeepClone());
            }
            else
            {
                MergeInto(existing, item);
            }
        }
    }

    private bool IsKeyed(JsonArray array) =>
        array.All(item => item is JsonObject obj && obj[_idProperty] is not null);
}
