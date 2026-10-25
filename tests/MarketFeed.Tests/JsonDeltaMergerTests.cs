using System.Text.Json.Nodes;

namespace MarketFeed.Tests;

public class JsonDeltaMergerTests
{
    private readonly JsonDeltaMerger _merger = new();

    [Fact]
    public void A_missing_or_null_property_leaves_the_old_value()
    {
        var current = Parse("""{"id":"A","last":10,"status":"open"}""");

        var merged = _merger.Merge(current, Parse("""{"id":"A","last":11,"status":null}"""));

        Assert.Equal(11, (int)merged["last"]!);
        Assert.Equal("open", (string?)merged["status"]);
    }

    [Fact]
    public void Arrays_of_objects_with_ids_merge_item_by_item()
    {
        var current = Parse("""{"levels":[{"id":1,"bid":9,"size":5},{"id":2,"bid":8,"size":7}]}""");

        var merged = _merger.Merge(current, Parse("""{"levels":[{"id":2,"bid":8.5},{"id":3,"bid":7}]}"""));

        var levels = merged["levels"]!.AsArray();
        Assert.Equal(3, levels.Count);
        Assert.Equal(9, (int)levels[0]!["bid"]!);
        Assert.Equal(8.5, (double)levels[1]!["bid"]!);
        Assert.Equal(7, (int)levels[1]!["size"]!);
        Assert.Equal(3, (int)levels[2]!["id"]!);
    }

    [Fact]
    public void Nested_objects_merge_and_plain_arrays_are_replaced()
    {
        var current = Parse("""{"quote":{"bid":1,"ask":2},"history":[1,2,3]}""");

        var merged = _merger.Merge(current, Parse("""{"quote":{"ask":3},"history":[4]}"""));

        Assert.Equal(1, (int)merged["quote"]!["bid"]!);
        Assert.Equal(3, (int)merged["quote"]!["ask"]!);
        Assert.Equal("[4]", merged["history"]!.ToJsonString());
    }

    [Fact]
    public void The_current_snapshot_is_never_modified()
    {
        var current = Parse("""{"last":10,"levels":[{"id":1,"bid":9}]}""");
        var before = current.ToJsonString();

        _merger.Merge(current, Parse("""{"last":11,"levels":[{"id":1,"bid":10}]}"""));

        Assert.Equal(before, current.ToJsonString());
    }

    private static JsonObject Parse(string json) => JsonNode.Parse(json)!.AsObject();
}
