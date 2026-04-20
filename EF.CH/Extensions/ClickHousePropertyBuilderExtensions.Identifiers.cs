using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods for configuring server-generated identifier defaults
/// (<c>generateSerialID</c>, <c>generateUUIDv4</c>, <c>generateUUIDv7</c>,
/// <c>generateULID</c>, <c>generateSnowflakeID</c>) on a column.
/// </summary>
/// <remarks>
/// Each helper sets a ClickHouse <c>DEFAULT</c> expression and marks the
/// property as <c>ValueGeneratedOnAdd</c> so EF Core omits the column from
/// INSERT statements and the server populates it.
/// </remarks>
public static class ClickHousePropertyBuilderIdentifierExtensions
{
    /// <summary>
    /// Configures the column to default to <c>generateSerialID('counterName')</c>,
    /// a Keeper-backed monotonically increasing UInt64. Requires ClickHouse Keeper.
    /// </summary>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <param name="counterName">The Keeper counter name. Single quotes are escaped.</param>
    public static PropertyBuilder<TProperty> HasSerialIDDefault<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder,
        string counterName)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(counterName);

        var expression = BuildSerialIDExpression(counterName);
        return propertyBuilder
            .HasDefaultExpression(expression)
            .ValueGeneratedOnAdd();
    }

    /// <summary>
    /// Configures the column to default to <c>generateSerialID('counterName')</c>.
    /// </summary>
    public static PropertyBuilder HasSerialIDDefault(
        this PropertyBuilder propertyBuilder,
        string counterName)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(counterName);

        var expression = BuildSerialIDExpression(counterName);
        propertyBuilder.HasDefaultExpression(expression);
        propertyBuilder.ValueGeneratedOnAdd();
        return propertyBuilder;
    }

    /// <summary>
    /// Configures the column to default to <c>generateUUIDv4()</c>.
    /// </summary>
    public static PropertyBuilder<TProperty> HasUuidV4Default<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder)
        => SetGeneratorDefault(propertyBuilder, "generateUUIDv4()");

    /// <summary>
    /// Configures the column to default to <c>generateUUIDv4()</c>.
    /// </summary>
    public static PropertyBuilder HasUuidV4Default(this PropertyBuilder propertyBuilder)
        => SetGeneratorDefault(propertyBuilder, "generateUUIDv4()");

    /// <summary>
    /// Configures the column to default to <c>generateUUIDv7()</c> — a time-sortable UUID v7.
    /// </summary>
    public static PropertyBuilder<TProperty> HasUuidV7Default<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder)
        => SetGeneratorDefault(propertyBuilder, "generateUUIDv7()");

    /// <summary>
    /// Configures the column to default to <c>generateUUIDv7()</c>.
    /// </summary>
    public static PropertyBuilder HasUuidV7Default(this PropertyBuilder propertyBuilder)
        => SetGeneratorDefault(propertyBuilder, "generateUUIDv7()");

    /// <summary>
    /// Configures the column to default to <c>generateULID()</c> — a 26-char
    /// lexicographically-sortable identifier. Column should be a String.
    /// </summary>
    public static PropertyBuilder<TProperty> HasUlidDefault<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder)
        => SetGeneratorDefault(propertyBuilder, "generateULID()");

    /// <summary>
    /// Configures the column to default to <c>generateULID()</c>.
    /// </summary>
    public static PropertyBuilder HasUlidDefault(this PropertyBuilder propertyBuilder)
        => SetGeneratorDefault(propertyBuilder, "generateULID()");

    /// <summary>
    /// Configures the column to default to <c>generateSnowflakeID()</c> — a
    /// Twitter-style Int64 ID (timestamp + machine + sequence). Unique without
    /// Keeper coordination.
    /// </summary>
    public static PropertyBuilder<TProperty> HasSnowflakeIDDefault<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder)
        => SetGeneratorDefault(propertyBuilder, "generateSnowflakeID()");

    /// <summary>
    /// Configures the column to default to <c>generateSnowflakeID()</c>.
    /// </summary>
    public static PropertyBuilder HasSnowflakeIDDefault(this PropertyBuilder propertyBuilder)
        => SetGeneratorDefault(propertyBuilder, "generateSnowflakeID()");

    private static PropertyBuilder<TProperty> SetGeneratorDefault<TProperty>(
        PropertyBuilder<TProperty> propertyBuilder,
        string expression)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        return propertyBuilder
            .HasDefaultExpression(expression)
            .ValueGeneratedOnAdd();
    }

    private static PropertyBuilder SetGeneratorDefault(
        PropertyBuilder propertyBuilder,
        string expression)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        propertyBuilder.HasDefaultExpression(expression);
        propertyBuilder.ValueGeneratedOnAdd();
        return propertyBuilder;
    }

    // ClickHouse single-quoted string literals escape ' as ''
    private static string BuildSerialIDExpression(string counterName)
        => $"generateSerialID('{counterName.Replace("'", "''")}')";
}
