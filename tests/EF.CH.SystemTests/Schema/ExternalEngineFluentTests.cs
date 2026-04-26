using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Xunit;

namespace EF.CH.SystemTests.Schema;

/// <summary>
/// API-surface guards for fluent engine builders in the integration-engine
/// family. These tests only check that the fluent method exists on
/// <c>EntityTypeBuilder&lt;T&gt;</c>; live remote DB workflows are outside this
/// system-test slice.
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
            "UsePostgreSqlEngine fluent method is not defined.");
    }

    [Fact]
    public void UseMySqlEngine_ShouldBeDefined()
    {
        Assert.True(HasFluentEngine("UseMySqlEngine"),
            "UseMySqlEngine fluent method is not defined.");
    }

    [Fact]
    public void UseRedisEngine_ShouldBeDefined()
    {
        Assert.True(HasFluentEngine("UseRedisEngine"),
            "UseRedisEngine fluent method is not defined.");
    }

    [Fact]
    public void UseOdbcEngine_ShouldBeDefined()
    {
        Assert.True(HasFluentEngine("UseOdbcEngine"),
            "UseOdbcEngine fluent method is not defined.");
    }

    // UseKeeperMapEngine is already covered separately; it ships on
    // EntityTypeBuilder<T> but is not part of this external-engine guard list.
}
