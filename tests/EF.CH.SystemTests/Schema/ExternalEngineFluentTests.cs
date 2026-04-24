using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Xunit;

namespace EF.CH.SystemTests.Schema;

/// <summary>
/// Gap #6 — fluent engine builders for the integration-engine family
/// (PostgreSQL / MySQL / Redis / ODBC / KeeperMap) are not implemented.
/// <c>UseDistributed</c> exists; the rest must be declared today via raw
/// CREATE TABLE … ENGINE = Xyz(...). See .tmp/notes/feature-gaps.md §6.
///
/// These tests only check that the fluent method exists on
/// <c>EntityTypeBuilder&lt;T&gt;</c> — they don't need a live remote DB.
/// </summary>
public class ExternalEngineFluentTests
{
    private static readonly Type EntityTypeBuilderType = typeof(EntityTypeBuilder<>);

    private static bool HasFluentEngine(string methodName)
    {
        // Check across the ClickHouseEntityTypeBuilderExtensions and its partial files
        // for a method with this name that's callable on EntityTypeBuilder<T>.
        var assembly = typeof(ClickHouseEntityTypeBuilderExtensions).Assembly;
        return assembly.GetTypes()
            .SelectMany(t => t.GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static))
            .Any(m => m.Name == methodName
                && m.GetParameters().Length > 0
                && m.GetParameters()[0].ParameterType.IsGenericType
                && m.GetParameters()[0].ParameterType.GetGenericTypeDefinition() == typeof(EntityTypeBuilder<>));
    }

    [Fact]
    public void UsePostgreSqlEngine_ShouldBeDefined()
    {
        Assert.True(HasFluentEngine("UsePostgreSqlEngine"),
            "UsePostgreSqlEngine fluent method is not defined. See feature-gaps.md §6.");
    }

    [Fact]
    public void UseMySqlEngine_ShouldBeDefined()
    {
        Assert.True(HasFluentEngine("UseMySqlEngine"),
            "UseMySqlEngine fluent method is not defined. See feature-gaps.md §6.");
    }

    [Fact]
    public void UseRedisEngine_ShouldBeDefined()
    {
        Assert.True(HasFluentEngine("UseRedisEngine"),
            "UseRedisEngine fluent method is not defined. See feature-gaps.md §6.");
    }

    [Fact]
    public void UseOdbcEngine_ShouldBeDefined()
    {
        Assert.True(HasFluentEngine("UseOdbcEngine"),
            "UseOdbcEngine fluent method is not defined. See feature-gaps.md §6.");
    }

    // UseKeeperMapEngine IS already defined on EntityTypeBuilder<T>, so this is
    // not a gap. (Left as documentation — original §6 grouped KeeperMap with
    // the external engines but it's actually shipped.)
}
