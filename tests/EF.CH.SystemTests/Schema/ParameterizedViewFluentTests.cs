using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Xunit;

namespace EF.CH.SystemTests.Schema;

/// <summary>
/// API-surface guards for ClickHouse parameterized view mapping and query-time
/// parameter binding.
/// </summary>
public class ParameterizedViewFluentTests
{
    [Fact]
    public void ToParameterizedView_ShouldBeDefined()
    {
        var found = typeof(ClickHouseEntityTypeBuilderExtensions).Assembly
            .GetTypes()
            .SelectMany(t => t.GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static))
            .Any(m => m.Name == "ToParameterizedView"
                && m.GetParameters().Length > 0
                && m.GetParameters()[0].ParameterType.IsGenericType
                && m.GetParameters()[0].ParameterType.GetGenericTypeDefinition() == typeof(EntityTypeBuilder<>));
        Assert.True(found,
            "Expected a fluent ToParameterizedView<T>(name) method on EntityTypeBuilder<T>.");
    }

    [Fact]
    public void DbSet_WithParameter_ShouldBeDefined()
    {
        var found = typeof(ClickHouseEntityTypeBuilderExtensions).Assembly
            .GetTypes()
            .SelectMany(t => t.GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static))
            .Any(m => m.Name == "WithParameter");
        Assert.True(found,
            "Expected a WithParameter(name, value) extension on IQueryable<T>.");
    }
}
