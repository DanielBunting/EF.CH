using System.Linq.Expressions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods for configuring ClickHouse-specific entity type options.
/// </summary>
public static class ClickHouseEntityTypeBuilderExtensions
{
    /// <summary>
    /// Configures the entity to use a MergeTree engine with the specified ORDER BY columns.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseMergeTree(
        this EntityTypeBuilder builder,
        params string[] orderByColumns)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(orderByColumns);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for MergeTree.", nameof(orderByColumns));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use a MergeTree engine with the specified ORDER BY columns.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> UseMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        params string[] orderByColumns)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).UseMergeTree(orderByColumns);
        return builder;
    }

    /// <summary>
    /// Configures the entity to use a MergeTree engine with the specified ORDER BY columns.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// entity.UseMergeTree(x => new { x.OrderDate, x.Id });
    /// // or
    /// entity.UseMergeTree(x => x.Id);
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> UseMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(orderByExpression);
        var columns = ExpressionExtensions.GetPropertyNames(orderByExpression);
        return builder.UseMergeTree(columns);
    }

    /// <summary>
    /// Configures the entity to use a ReplacingMergeTree engine.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseReplacingMergeTree(
        this EntityTypeBuilder builder,
        params string[] orderByColumns)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(orderByColumns);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for ReplacingMergeTree.", nameof(orderByColumns));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "ReplacingMergeTree");
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use a ReplacingMergeTree engine.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> UseReplacingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        params string[] orderByColumns)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).UseReplacingMergeTree(orderByColumns);
        return builder;
    }

    /// <summary>
    /// Configures the entity to use a ReplacingMergeTree engine with a version column.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="versionColumn">The version column for deduplication.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseReplacingMergeTree(
        this EntityTypeBuilder builder,
        string versionColumn,
        params string[] orderByColumns)
    {
        builder.UseReplacingMergeTree(orderByColumns);
        builder.HasAnnotation(ClickHouseAnnotationNames.VersionColumn, versionColumn);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use a ReplacingMergeTree engine with a version column.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="versionColumn">The version column for deduplication.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> UseReplacingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string versionColumn,
        params string[] orderByColumns)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).UseReplacingMergeTree(versionColumn, orderByColumns);
        return builder;
    }

    /// <summary>
    /// Configures the entity to use a ReplacingMergeTree engine with the specified ORDER BY columns.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// entity.UseReplacingMergeTree(x => new { x.UserId, x.Timestamp });
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> UseReplacingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(orderByExpression);
        var columns = ExpressionExtensions.GetPropertyNames(orderByExpression);
        return builder.UseReplacingMergeTree(columns);
    }

    /// <summary>
    /// Configures the entity to use a ReplacingMergeTree engine with a version column.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="versionColumnExpression">Expression selecting the version column for deduplication.</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// entity.UseReplacingMergeTree(x => x.UpdatedAt, x => new { x.Id });
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> UseReplacingMergeTree<TEntity, TVersion>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, TVersion>> versionColumnExpression,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(versionColumnExpression);
        ArgumentNullException.ThrowIfNull(orderByExpression);
        var versionColumn = ExpressionExtensions.GetPropertyName(versionColumnExpression);
        var columns = ExpressionExtensions.GetPropertyNames(orderByExpression);
        return builder.UseReplacingMergeTree(versionColumn, columns);
    }

    /// <summary>
    /// Configures the entity to use a SummingMergeTree engine.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> UseSummingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        params string[] orderByColumns)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(orderByColumns);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for SummingMergeTree.", nameof(orderByColumns));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "SummingMergeTree");
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use an AggregatingMergeTree engine.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> UseAggregatingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        params string[] orderByColumns)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(orderByColumns);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for AggregatingMergeTree.", nameof(orderByColumns));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "AggregatingMergeTree");
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use a SummingMergeTree engine with the specified ORDER BY columns.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> UseSummingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(orderByExpression);
        var columns = ExpressionExtensions.GetPropertyNames(orderByExpression);
        return builder.UseSummingMergeTree(columns);
    }

    /// <summary>
    /// Configures the entity to use an AggregatingMergeTree engine with the specified ORDER BY columns.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> UseAggregatingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(orderByExpression);
        var columns = ExpressionExtensions.GetPropertyNames(orderByExpression);
        return builder.UseAggregatingMergeTree(columns);
    }

    #region Partition By

    /// <summary>
    /// Configures the PARTITION BY expression for the table.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="partitionExpression">The partition expression (e.g., "toYYYYMM(created_at)").</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder HasPartitionBy(
        this EntityTypeBuilder builder,
        string partitionExpression)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(partitionExpression);

        builder.HasAnnotation(ClickHouseAnnotationNames.PartitionBy, partitionExpression);

        return builder;
    }

    /// <summary>
    /// Configures the PARTITION BY expression for the table.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="partitionExpression">The partition expression (e.g., "toYYYYMM(created_at)").</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> HasPartitionBy<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string partitionExpression)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).HasPartitionBy(partitionExpression);
        return builder;
    }

    /// <summary>
    /// Configures PARTITION BY using a monthly granularity on the specified column.
    /// Generates: PARTITION BY toYYYYMM(column)
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="columnExpression">Expression selecting the date/datetime column to partition by.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// entity.HasPartitionByMonth(x => x.CreatedAt);
    /// // Generates: PARTITION BY toYYYYMM("CreatedAt")
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> HasPartitionByMonth<TEntity, TProperty>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, TProperty>> columnExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(columnExpression);
        var columnName = ExpressionExtensions.GetPropertyName(columnExpression);
        return builder.HasPartitionBy($"toYYYYMM(\"{columnName}\")");
    }

    /// <summary>
    /// Configures PARTITION BY using a daily granularity on the specified column.
    /// Generates: PARTITION BY toYYYYMMDD(column)
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="columnExpression">Expression selecting the date/datetime column to partition by.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// entity.HasPartitionByDay(x => x.EventDate);
    /// // Generates: PARTITION BY toYYYYMMDD("EventDate")
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> HasPartitionByDay<TEntity, TProperty>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, TProperty>> columnExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(columnExpression);
        var columnName = ExpressionExtensions.GetPropertyName(columnExpression);
        return builder.HasPartitionBy($"toYYYYMMDD(\"{columnName}\")");
    }

    /// <summary>
    /// Configures PARTITION BY using a yearly granularity on the specified column.
    /// Generates: PARTITION BY toYear(column)
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="columnExpression">Expression selecting the date/datetime column to partition by.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// entity.HasPartitionByYear(x => x.OrderDate);
    /// // Generates: PARTITION BY toYear("OrderDate")
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> HasPartitionByYear<TEntity, TProperty>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, TProperty>> columnExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(columnExpression);
        var columnName = ExpressionExtensions.GetPropertyName(columnExpression);
        return builder.HasPartitionBy($"toYear(\"{columnName}\")");
    }

    #endregion

    /// <summary>
    /// Configures the SAMPLE BY expression for the table.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="sampleExpression">The sample expression.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder HasSampleBy(
        this EntityTypeBuilder builder,
        string sampleExpression)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(sampleExpression);

        builder.HasAnnotation(ClickHouseAnnotationNames.SampleBy, sampleExpression);

        return builder;
    }

    /// <summary>
    /// Configures the SAMPLE BY expression for the table.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="sampleExpression">The sample expression.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> HasSampleBy<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string sampleExpression)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).HasSampleBy(sampleExpression);
        return builder;
    }

    /// <summary>
    /// Configures the TTL expression for the table.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="ttlExpression">The TTL expression (e.g., "created_at + INTERVAL 1 MONTH").</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder HasTtl(
        this EntityTypeBuilder builder,
        string ttlExpression)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(ttlExpression);

        builder.HasAnnotation(ClickHouseAnnotationNames.Ttl, ttlExpression);

        return builder;
    }

    /// <summary>
    /// Configures the TTL expression for the table.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="ttlExpression">The TTL expression (e.g., "created_at + INTERVAL 1 MONTH").</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> HasTtl<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string ttlExpression)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).HasTtl(ttlExpression);
        return builder;
    }

    /// <summary>
    /// Configures additional engine settings.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="settings">The settings dictionary.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> HasEngineSettings<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        IDictionary<string, string> settings)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(settings);

        builder.HasAnnotation(ClickHouseAnnotationNames.Settings, settings);

        return builder;
    }
}
