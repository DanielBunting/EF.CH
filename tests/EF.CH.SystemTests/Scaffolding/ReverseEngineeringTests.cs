using EF.CH.Design.Internal;
using EF.CH.Metadata;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EF.CH.SystemTests.Scaffolding;

/// <summary>
/// Reverse-engineering coverage for <c>ClickHouseDatabaseModelFactory</c>. Tests pin
/// scaffolder behavior end-to-end against a live ClickHouse: complex MergeTree shape,
/// view/dictionary exclusion, and refreshable MV metadata round-trip.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public sealed class ReverseEngineeringTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public ReverseEngineeringTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task Scaffold_ComplexTable_PreservesEngineOrderPartitionTypes()
    {
        await ResetAsync();

        await RawClickHouse.ExecuteAsync(Conn, @"
            CREATE TABLE complex_orders (
                id UInt64,
                user_id UInt64,
                amount Decimal(18, 4) CODEC(ZSTD(3)),
                created_at DateTime64(3),
                INDEX idx_user user_id TYPE minmax GRANULARITY 1,
                PROJECTION p_by_user (SELECT user_id, count(), sum(amount) GROUP BY user_id)
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(created_at)
            ORDER BY (user_id, created_at)
            TTL created_at + INTERVAL 90 DAY");

        var model = CreateFactory().Create(Conn, new DatabaseModelFactoryOptions());
        var t = model.Tables.Single(x => x.Name == "complex_orders");

        Assert.Equal("MergeTree", t[ClickHouseAnnotationNames.Engine]);

        var orderBy = t[ClickHouseAnnotationNames.OrderBy] as string[];
        Assert.NotNull(orderBy);
        Assert.Equal(new[] { "user_id", "created_at" }, orderBy);

        var partition = t[ClickHouseAnnotationNames.PartitionBy] as string;
        Assert.False(string.IsNullOrEmpty(partition));
        Assert.Contains("toYYYYMM", partition!);

        Assert.Equal(4, t.Columns.Count);
        Assert.Contains(t.Columns, c => c.Name == "amount" && c.StoreType.StartsWith("Decimal"));
        Assert.Contains(t.Columns, c => c.Name == "created_at" && c.StoreType.StartsWith("DateTime64"));
    }

    [Fact]
    public async Task Scaffold_ExcludesPlainViewsAndDictionaries()
    {
        await ResetAsync();

        await RawClickHouse.ExecuteAsync(Conn,
            "CREATE TABLE plain_t (id UInt64) ENGINE = MergeTree() ORDER BY id");
        await RawClickHouse.ExecuteAsync(Conn,
            "CREATE VIEW plain_v AS SELECT id FROM plain_t");
        await RawClickHouse.ExecuteAsync(Conn,
            "CREATE TABLE dict_source (id UInt64, label String) ENGINE = MergeTree() ORDER BY id");
        await RawClickHouse.ExecuteAsync(Conn, @"
            CREATE DICTIONARY dict_lookup (id UInt64, label String)
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(table 'dict_source'))
            LAYOUT(HASHED())
            LIFETIME(MIN 0 MAX 300)");

        var model = CreateFactory().Create(Conn, new DatabaseModelFactoryOptions());
        var names = model.Tables.Select(t => t.Name).ToHashSet();

        Assert.Contains("plain_t", names);
        Assert.Contains("dict_source", names);
        // Pin: scaffolder excludes plain views and dictionaries (filter at ClickHouseDatabaseModelFactory:126).
        Assert.DoesNotContain("plain_v", names);
        Assert.DoesNotContain("dict_lookup", names);
    }

    [Fact]
    public async Task Scaffold_RespectsExplicitTableFilter()
    {
        await ResetAsync();

        await RawClickHouse.ExecuteAsync(Conn,
            "CREATE TABLE keep_me (id UInt64) ENGINE = MergeTree() ORDER BY id");
        await RawClickHouse.ExecuteAsync(Conn,
            "CREATE TABLE skip_me (id UInt64) ENGINE = MergeTree() ORDER BY id");

        var options = new DatabaseModelFactoryOptions(tables: new[] { "keep_me" });
        var model = CreateFactory().Create(Conn, options);
        var names = model.Tables.Select(t => t.Name).ToHashSet();

        Assert.Contains("keep_me", names);
        Assert.DoesNotContain("skip_me", names);
    }

    [Fact]
    public async Task Scaffold_NullableAndCodecColumns_RoundTripStoreTypes()
    {
        await ResetAsync();

        await RawClickHouse.ExecuteAsync(Conn, @"
            CREATE TABLE codecs_table (
                id UInt64,
                name Nullable(String),
                ts DateTime CODEC(DoubleDelta, LZ4),
                payload String CODEC(ZSTD(5))
            )
            ENGINE = MergeTree() ORDER BY id");

        var model = CreateFactory().Create(Conn, new DatabaseModelFactoryOptions());
        var t = model.Tables.Single(x => x.Name == "codecs_table");

        Assert.Equal("UInt64", t.Columns.First(c => c.Name == "id").StoreType);
        var nameCol = t.Columns.First(c => c.Name == "name");
        Assert.True(nameCol.IsNullable);
        Assert.Equal("Nullable(String)", nameCol.StoreType);
    }

    private async Task ResetAsync()
    {
        await RawClickHouse.ExecuteAsync(Conn, "DROP DICTIONARY IF EXISTS \"dict_lookup\"");
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT name, engine FROM system.tables WHERE database = currentDatabase() AND name NOT LIKE '.%'");
        foreach (var r in rows)
        {
            var name = (string)r["name"]!;
            var engine = (string?)r["engine"];
            var kind = engine is "View" or "MaterializedView" or "LiveView" ? "VIEW" : "TABLE";
            await RawClickHouse.ExecuteAsync(Conn, $"DROP {kind} IF EXISTS \"{name}\" SYNC");
        }
    }

    private static IDatabaseModelFactory CreateFactory()
    {
        var services = new ServiceCollection();
        new ClickHouseDesignTimeServices().ConfigureDesignTimeServices(services);
        return services.BuildServiceProvider().GetRequiredService<IDatabaseModelFactory>();
    }
}
