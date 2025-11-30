using System.Linq.Expressions;
using EF.CH.Metadata;
using EF.CH.Query.Internal;
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

        // Store query expression for later translation during migration
        // We defer translation until the model is finalized
        builder.HasNoKey();
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, sourceTableName);
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedViewPopulate, populate);

        // Store the expression for later translation
        // The translator will be invoked when the model is finalized
        builder.HasAnnotation("ClickHouse:MaterializedViewExpression", query);

        return builder;
    }

    #endregion
}
