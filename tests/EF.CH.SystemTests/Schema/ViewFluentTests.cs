using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Xunit;

namespace EF.CH.SystemTests.Schema;

/// <summary>
/// API-surface guards for the plain (non-parameterized, non-materialized) ClickHouse
/// view fluent API. Mirrors <see cref="ParameterizedViewFluentTests"/>.
/// </summary>
public class ViewFluentTests
{
    [Fact]
    public void HasView_ShouldBeDefined()
    {
        var found = typeof(ClickHouseEntityTypeBuilderExtensions).Assembly
            .GetTypes()
            .SelectMany(t => t.GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static))
            .Any(m => m.Name == "HasView"
                && m.GetParameters().Length > 0
                && m.GetParameters()[0].ParameterType.IsGenericType
                && m.GetParameters()[0].ParameterType.GetGenericTypeDefinition() == typeof(EntityTypeBuilder<>));
        Assert.True(found, "Expected a fluent HasView<T>(name) method on EntityTypeBuilder<T>.");
    }

    [Fact]
    public void AsView_ShouldBeDefined()
    {
        var found = typeof(ClickHouseEntityTypeBuilderExtensions).Assembly
            .GetTypes()
            .SelectMany(t => t.GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static))
            .Any(m => m.Name == "AsView"
                && m.IsGenericMethodDefinition
                && m.GetGenericArguments().Length == 2);
        Assert.True(found, "Expected an AsView<TView, TSource>(cfg => ...) method.");
    }

    [Fact]
    public void AsViewRaw_ShouldBeDefined()
    {
        var found = typeof(ClickHouseEntityTypeBuilderExtensions).Assembly
            .GetTypes()
            .SelectMany(t => t.GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static))
            .Any(m => m.Name == "AsViewRaw");
        Assert.True(found, "Expected an AsViewRaw<T>(name, selectSql, ...) method.");
    }

    [Fact]
    public void FromView_ShouldBeDefined()
    {
        var found = typeof(ClickHouseViewExtensions).Assembly
            .GetTypes()
            .SelectMany(t => t.GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static))
            .Any(m => m.Name == "FromView");
        Assert.True(found, "Expected a FromView<T>(viewName) extension on DbContext.");
    }

    [Fact]
    public void CreateViewAndDropView_ShouldBeDefined()
    {
        var methods = typeof(ClickHouseDatabaseExtensions).GetMethods(
            System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
        Assert.Contains(methods, m => m.Name == "CreateViewAsync");
        Assert.Contains(methods, m => m.Name == "DropViewAsync");
        Assert.Contains(methods, m => m.Name == "EnsureViewsAsync");
    }

    [Fact]
    public void CreateViewMigrationBuilder_ShouldBeDefined()
    {
        var methods = typeof(ClickHouseMigrationBuilderExtensions).GetMethods(
            System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
        Assert.Contains(methods, m => m.Name == "CreateView");
        Assert.Contains(methods, m => m.Name == "DropView");
    }
}
