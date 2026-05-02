using EF.CH.Extensions;
using EF.CH.Metadata.Conventions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Xunit;

namespace EF.CH.Tests.Metadata.Conventions;

/// <summary>
/// <see cref="ClickHouseConventionSetBuilder"/> inserts and appends a small
/// number of conventions on top of EF Core's relational set. The ordering is
/// load-bearing — for example, <see cref="ClickHouseKeylessConvention"/> must
/// run before EF Core's automatic key discovery — and reordering today is
/// silent. These tests pin the current insertion positions so a future edit
/// that drops or moves one of them surfaces as a test failure.
/// </summary>
public class ConventionSetBuilderOrderTests
{
    [Fact]
    public void KeylessConvention_IsAtIndexZero()
    {
        var set = BuildConventionSet(useKeylessEntitiesByDefault: true);
        // The keyless convention is inserted at index 0 only when
        // UseKeylessEntitiesByDefault is on; assert it lives there.
        Assert.NotEmpty(set.EntityTypeAddedConventions);
        Assert.IsType<ClickHouseKeylessConvention>(set.EntityTypeAddedConventions[0]);
    }

    [Fact]
    public void KeylessConvention_IsAbsent_WhenOptOutDefault()
    {
        var set = BuildConventionSet(useKeylessEntitiesByDefault: false);
        Assert.DoesNotContain(set.EntityTypeAddedConventions,
            c => c is ClickHouseKeylessConvention);
    }

    [Fact]
    public void JsonValueConverterPair_IsRegisteredAdjacentInOrder()
    {
        var set = BuildConventionSet(useKeylessEntitiesByDefault: false);

        // The two JSON conventions must coexist: the property-added one
        // discovers [ClickHouseJson] attributes; the model-finalizing one
        // applies the value converters. If either is missing the JSON
        // surface silently regresses.
        Assert.Contains(set.PropertyAddedConventions, c => c is ClickHouseJsonAttributeConvention);
        Assert.Contains(set.ModelFinalizingConventions, c => c is ClickHouseJsonValueConverterConvention);

        // The model-finalising JSON converter convention runs after the
        // ValueGenerated convention so that integer ID rewriting completes
        // before JSON property converters apply.
        var modelFinalizingTypes = set.ModelFinalizingConventions.Select(c => c.GetType()).ToList();
        var valueGenIdx = modelFinalizingTypes.IndexOf(typeof(ClickHouseValueGeneratedConvention));
        var jsonConvIdx = modelFinalizingTypes.IndexOf(typeof(ClickHouseJsonValueConverterConvention));
        Assert.True(valueGenIdx >= 0, "ClickHouseValueGeneratedConvention is missing");
        Assert.True(jsonConvIdx >= 0, "ClickHouseJsonValueConverterConvention is missing");
        Assert.True(valueGenIdx < jsonConvIdx,
            "ValueGenerated must run before JsonValueConverter so server-default ID conversion settles first");
    }

    [Fact]
    public void ValueGeneratedConvention_IsRegistered()
    {
        var set = BuildConventionSet(useKeylessEntitiesByDefault: false);
        Assert.Contains(set.ModelFinalizingConventions, c => c is ClickHouseValueGeneratedConvention);
    }

    private static ConventionSet BuildConventionSet(bool useKeylessEntitiesByDefault)
    {
        var ob = new DbContextOptionsBuilder<EmptyCtx>()
            .UseClickHouse("Host=localhost;Database=test", o =>
            {
                if (useKeylessEntitiesByDefault) o.UseKeylessEntitiesByDefault();
            });

        using var ctx = new EmptyCtx(ob.Options);
        var builder = ctx.GetService<IConventionSetBuilder>();
        return builder.CreateConventionSet();
    }

    public sealed class EmptyCtx(DbContextOptions<EmptyCtx> o) : DbContext(o);
}
