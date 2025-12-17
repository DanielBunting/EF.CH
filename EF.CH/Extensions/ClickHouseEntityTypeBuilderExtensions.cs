using System.Linq.Expressions;
using EF.CH.Dictionaries;
using EF.CH.Metadata;
using EF.CH.Query.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
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
    /// Configures the entity to use a ReplacingMergeTree engine with version and is_deleted columns.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TVersion">The version column type.</typeparam>
    /// <typeparam name="TIsDeleted">The is_deleted column type (should be byte/UInt8).</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="versionColumnExpression">Expression selecting the version column.</param>
    /// <param name="isDeletedColumnExpression">Expression selecting the is_deleted column (UInt8).</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <remarks>
    /// <para>
    /// Requires ClickHouse 23.2+. When is_deleted is specified, rows where the winning version
    /// has is_deleted=1 are physically removed during background merges.
    /// </para>
    /// <para>
    /// With FINAL, deleted rows are automatically excluded from query results.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// entity.UseReplacingMergeTree(
    ///     x => x.Version,      // Version column
    ///     x => x.IsDeleted,    // IsDeleted column (UInt8)
    ///     x => new { x.Id });  // ORDER BY
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> UseReplacingMergeTree<TEntity, TVersion, TIsDeleted>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, TVersion>> versionColumnExpression,
        Expression<Func<TEntity, TIsDeleted>> isDeletedColumnExpression,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(versionColumnExpression);
        ArgumentNullException.ThrowIfNull(isDeletedColumnExpression);
        ArgumentNullException.ThrowIfNull(orderByExpression);

        var versionColumn = ExpressionExtensions.GetPropertyName(versionColumnExpression);
        var isDeletedColumn = ExpressionExtensions.GetPropertyName(isDeletedColumnExpression);
        var columns = ExpressionExtensions.GetPropertyNames(orderByExpression);

        builder.UseReplacingMergeTree(versionColumn, columns);
        builder.HasAnnotation(ClickHouseAnnotationNames.IsDeletedColumn, isDeletedColumn);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use a SummingMergeTree engine.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseSummingMergeTree(
        this EntityTypeBuilder builder,
        params string[] orderByColumns)
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
        ((EntityTypeBuilder)builder).UseSummingMergeTree(orderByColumns);
        return builder;
    }

    /// <summary>
    /// Configures the entity to use an AggregatingMergeTree engine.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseAggregatingMergeTree(
        this EntityTypeBuilder builder,
        params string[] orderByColumns)
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
        ((EntityTypeBuilder)builder).UseAggregatingMergeTree(orderByColumns);
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

    #region CollapsingMergeTree

    /// <summary>
    /// Configures the entity to use a CollapsingMergeTree engine.
    /// </summary>
    /// <remarks>
    /// CollapsingMergeTree is designed to track state changes where rows can be "cancelled"
    /// by inserting a row with the opposite sign. During background merges, ClickHouse
    /// removes pairs of rows with opposite signs and matching ORDER BY keys.
    ///
    /// The sign column must be Int8 with values +1 (state row) or -1 (cancel row).
    /// </remarks>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="signColumnExpression">Expression selecting the sign column (Int8/sbyte with +1 or -1).</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// entity.UseCollapsingMergeTree(
    ///     signColumn: x => x.Sign,
    ///     orderBy: x => new { x.UserId, x.EventTime });
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> UseCollapsingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, sbyte>> signColumnExpression,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(signColumnExpression);
        ArgumentNullException.ThrowIfNull(orderByExpression);

        var signColumn = ExpressionExtensions.GetPropertyName(signColumnExpression);
        var orderByColumns = ExpressionExtensions.GetPropertyNames(orderByExpression);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for CollapsingMergeTree.", nameof(orderByExpression));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "CollapsingMergeTree");
        builder.HasAnnotation(ClickHouseAnnotationNames.SignColumn, signColumn);
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use a CollapsingMergeTree engine with string column names.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="signColumn">The name of the sign column.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseCollapsingMergeTree(
        this EntityTypeBuilder builder,
        string signColumn,
        params string[] orderByColumns)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(signColumn);
        ArgumentNullException.ThrowIfNull(orderByColumns);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for CollapsingMergeTree.", nameof(orderByColumns));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "CollapsingMergeTree");
        builder.HasAnnotation(ClickHouseAnnotationNames.SignColumn, signColumn);
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use a VersionedCollapsingMergeTree engine.
    /// </summary>
    /// <remarks>
    /// VersionedCollapsingMergeTree extends CollapsingMergeTree to handle out-of-order inserts.
    /// It uses a version column to correctly collapse rows even when they arrive in different order.
    ///
    /// The sign column must be Int8 with values +1 (state row) or -1 (cancel row).
    /// The version column should be an incrementing value (typically UInt8, UInt16, UInt32, UInt64).
    /// </remarks>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TVersion">The version column type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="signColumnExpression">Expression selecting the sign column (Int8/sbyte with +1 or -1).</param>
    /// <param name="versionColumnExpression">Expression selecting the version column.</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// entity.UseVersionedCollapsingMergeTree(
    ///     signColumn: x => x.Sign,
    ///     versionColumn: x => x.Version,
    ///     orderBy: x => new { x.UserId, x.EventTime });
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> UseVersionedCollapsingMergeTree<TEntity, TVersion>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, sbyte>> signColumnExpression,
        Expression<Func<TEntity, TVersion>> versionColumnExpression,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(signColumnExpression);
        ArgumentNullException.ThrowIfNull(versionColumnExpression);
        ArgumentNullException.ThrowIfNull(orderByExpression);

        var signColumn = ExpressionExtensions.GetPropertyName(signColumnExpression);
        var versionColumn = ExpressionExtensions.GetPropertyName(versionColumnExpression);
        var orderByColumns = ExpressionExtensions.GetPropertyNames(orderByExpression);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for VersionedCollapsingMergeTree.", nameof(orderByExpression));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "VersionedCollapsingMergeTree");
        builder.HasAnnotation(ClickHouseAnnotationNames.SignColumn, signColumn);
        builder.HasAnnotation(ClickHouseAnnotationNames.VersionColumn, versionColumn);
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use a VersionedCollapsingMergeTree engine with string column names.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="signColumn">The name of the sign column.</param>
    /// <param name="versionColumn">The name of the version column.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseVersionedCollapsingMergeTree(
        this EntityTypeBuilder builder,
        string signColumn,
        string versionColumn,
        params string[] orderByColumns)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(signColumn);
        ArgumentException.ThrowIfNullOrWhiteSpace(versionColumn);
        ArgumentNullException.ThrowIfNull(orderByColumns);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for VersionedCollapsingMergeTree.", nameof(orderByColumns));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "VersionedCollapsingMergeTree");
        builder.HasAnnotation(ClickHouseAnnotationNames.SignColumn, signColumn);
        builder.HasAnnotation(ClickHouseAnnotationNames.VersionColumn, versionColumn);
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);

        return builder;
    }

    #endregion

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

    #region Materialized Views

    /// <summary>
    /// Configures the entity as a materialized view with a raw SQL SELECT query.
    /// This is the escape hatch for complex ClickHouse-specific queries that cannot be expressed in LINQ.
    /// </summary>
    /// <typeparam name="TEntity">The entity type representing the view's output schema.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="sourceTable">The name of the source table the view reads from.</param>
    /// <param name="selectSql">The raw SQL SELECT query for the view (without CREATE MATERIALIZED VIEW prefix).</param>
    /// <param name="populate">Whether to backfill existing data when creating the view.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <remarks>
    /// <para>
    /// ClickHouse materialized views are INSERT triggers, not cached query results.
    /// When data is inserted into the source table, the view's SELECT query transforms it
    /// and inserts the result into the view's storage.
    /// </para>
    /// <para>
    /// The entity should be configured as keyless using <c>HasNoKey()</c> since materialized views
    /// are typically append-only. Engine configuration (UseMergeTree, etc.) still applies.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// modelBuilder.Entity&lt;DailySummary&gt;(entity =>
    /// {
    ///     entity.ToTable("DailySummary_MV");
    ///     entity.HasNoKey();
    ///     entity.UseSummingMergeTree(x => new { x.Date, x.ProductId });
    ///     entity.AsMaterializedViewRaw(
    ///         sourceTable: "Orders",
    ///         selectSql: @"
    ///             SELECT
    ///                 toDate(OrderDate) AS Date,
    ///                 ProductId,
    ///                 sum(Quantity) AS TotalQuantity,
    ///                 sum(Revenue) AS TotalRevenue
    ///             FROM Orders
    ///             GROUP BY Date, ProductId
    ///         ",
    ///         populate: false
    ///     );
    /// });
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> AsMaterializedViewRaw<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string sourceTable,
        string selectSql,
        bool populate = false)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(sourceTable);
        ArgumentException.ThrowIfNullOrEmpty(selectSql);

        // Mark as keyless - materialized views are typically append-only
        builder.HasNoKey();

        // Store view configuration as annotations
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, sourceTable);
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery, selectSql.Trim());
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedViewPopulate, populate);

        return builder;
    }

    /// <summary>
    /// Configures the entity as a materialized view with a raw SQL SELECT query.
    /// Non-generic overload.
    /// </summary>
    public static EntityTypeBuilder AsMaterializedViewRaw(
        this EntityTypeBuilder builder,
        string sourceTable,
        string selectSql,
        bool populate = false)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(sourceTable);
        ArgumentException.ThrowIfNullOrEmpty(selectSql);

        builder.HasNoKey();
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, sourceTable);
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery, selectSql.Trim());
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedViewPopulate, populate);

        return builder;
    }

    /// <summary>
    /// Configures the entity as a materialized view using a type-safe LINQ query expression.
    /// The LINQ expression is translated to ClickHouse SQL at configuration time.
    /// </summary>
    /// <typeparam name="TEntity">The entity type representing the view's output schema.</typeparam>
    /// <typeparam name="TSource">The entity type representing the source table.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="query">
    /// A LINQ expression that defines the view's transformation.
    /// Supports GroupBy, Select, and aggregate functions (Sum, Count, Min, Max, Average).
    /// </param>
    /// <param name="populate">Whether to backfill existing data when creating the view.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <remarks>
    /// <para>
    /// ClickHouse materialized views are INSERT triggers. When data is inserted into the source table,
    /// the view's query transforms it and inserts the result into the view's storage.
    /// </para>
    /// <para>
    /// The query expression is translated to ClickHouse SQL at configuration time, not at runtime.
    /// This provides compile-time type checking while generating optimized ClickHouse SQL.
    /// </para>
    /// <para>
    /// Engine configuration (UseMergeTree, UseSummingMergeTree, etc.) should be applied before
    /// calling this method.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// modelBuilder.Entity&lt;OrderDailySummary&gt;(entity =&gt;
    /// {
    ///     entity.ToTable("OrderDailySummary_MV");
    ///     entity.UseSummingMergeTree(x =&gt; new { x.Date, x.ProductId });
    ///     entity.AsMaterializedView&lt;OrderDailySummary, Order&gt;(
    ///         query: orders =&gt; orders
    ///             .GroupBy(o =&gt; new { Date = o.OrderDate.Date, o.ProductId })
    ///             .Select(g =&gt; new OrderDailySummary
    ///             {
    ///                 Date = g.Key.Date,
    ///                 ProductId = g.Key.ProductId,
    ///                 TotalQuantity = g.Sum(o =&gt; o.Quantity),
    ///                 TotalRevenue = g.Sum(o =&gt; o.Revenue)
    ///             }),
    ///         populate: false);
    /// });
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> AsMaterializedView<TEntity, TSource>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<IQueryable<TSource>, IQueryable<TEntity>>> query,
        bool populate = false)
        where TEntity : class
        where TSource : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(query);

        // Get source table name from model or entity type name
        var sourceEntityType = builder.Metadata.Model.FindEntityType(typeof(TSource));
        var sourceTableName = sourceEntityType?.GetTableName() ?? typeof(TSource).Name;

        // Translate LINQ expression to SQL immediately
        // This allows the SQL to be serialized in model snapshots
        var translator = new Query.Internal.MaterializedViewSqlTranslator(
            (IModel)builder.Metadata.Model,
            sourceTableName);
        var selectSql = translator.Translate<TSource, TEntity>(query);

        builder.HasNoKey();
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, sourceTableName);
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery, selectSql);
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedViewPopulate, populate);

        return builder;
    }

    #endregion

    #region Dictionaries

    /// <summary>
    /// Configures this entity as a ClickHouse dictionary sourced from another table.
    /// </summary>
    /// <typeparam name="TDictionary">The dictionary entity type.</typeparam>
    /// <typeparam name="TSource">The source entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="configure">Action to configure the dictionary.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <remarks>
    /// <para>
    /// This method configures an entity to be created as a ClickHouse dictionary rather than a table.
    /// Dictionaries are in-memory key-value stores that provide fast lookups and are ideal for
    /// reference data like countries, currencies, or product categories.
    /// </para>
    /// <para>
    /// The dictionary entity should implement <see cref="Dictionaries.IClickHouseDictionary"/> as a marker interface.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// // Define dictionary entity
    /// public class CountryLookup : IClickHouseDictionary
    /// {
    ///     public ulong Id { get; set; }
    ///     public string Name { get; set; } = string.Empty;
    ///     public string IsoCode { get; set; } = string.Empty;
    /// }
    ///
    /// // Configure in OnModelCreating
    /// modelBuilder.Entity&lt;CountryLookup&gt;(entity =>
    /// {
    ///     entity.AsDictionary&lt;CountryLookup, Country&gt;(cfg => cfg
    ///         .HasKey(x => x.Id)
    ///         .FromTable(
    ///             projection: c => new CountryLookup
    ///             {
    ///                 Id = c.Id,
    ///                 Name = c.Name,
    ///                 IsoCode = c.IsoCode
    ///             },
    ///             filter: q => q.Where(c => c.IsActive))
    ///         .UseHashedLayout()
    ///         .HasLifetime(minSeconds: 60, maxSeconds: 300)
    ///         .HasDefault(x => x.Name, "Unknown"));
    /// });
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TDictionary> AsDictionary<TDictionary, TSource>(
        this EntityTypeBuilder<TDictionary> builder,
        Action<Dictionaries.DictionaryConfiguration<TDictionary, TSource>> configure)
        where TDictionary : class
        where TSource : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var configuration = new Dictionaries.DictionaryConfiguration<TDictionary, TSource>(builder);
        configure(configuration);
        configuration.Apply();

        return builder;
    }

    #endregion
}
