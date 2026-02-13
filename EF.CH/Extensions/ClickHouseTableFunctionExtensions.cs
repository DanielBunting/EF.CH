using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods for querying ClickHouse table functions (s3, url, remote, file, cluster).
/// These wrap <c>FromSqlRaw</c> to provide ergonomic access to external data sources.
/// </summary>
public static class ClickHouseTableFunctionExtensions
{
    /// <summary>
    /// Queries data from an S3-compatible object store using the <c>s3()</c> table function.
    /// </summary>
    /// <typeparam name="T">A registered entity type.</typeparam>
    /// <param name="context">The DbContext.</param>
    /// <param name="path">S3 path (e.g. <c>https://bucket.s3.amazonaws.com/data/*.parquet</c>).</param>
    /// <param name="format">Data format (e.g. Parquet, CSVWithNames, JSONEachRow).</param>
    /// <param name="accessKeyId">Optional AWS access key ID.</param>
    /// <param name="secretAccessKey">Optional AWS secret access key.</param>
    /// <param name="structure">Optional column structure. When null, inferred from the EF model.</param>
    /// <returns>An <see cref="IQueryable{T}"/> over the S3 data.</returns>
    public static IQueryable<T> FromS3<T>(
        this DbContext context,
        string path,
        string format,
        string? accessKeyId = null,
        string? secretAccessKey = null,
        string? structure = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(format);

        structure ??= InferStructure<T>(context);

        var args = new List<string> { Escape(path) };

        if (accessKeyId != null && secretAccessKey != null)
        {
            args.Add(Escape(accessKeyId));
            args.Add(Escape(secretAccessKey));
        }

        args.Add(Escape(format));
        args.Add(Escape(structure));

        var sql = $"SELECT * FROM s3({string.Join(", ", args)})";
        return context.Set<T>().FromSqlRaw(sql);
    }

    /// <summary>
    /// Queries data from a URL using the <c>url()</c> table function.
    /// </summary>
    /// <typeparam name="T">A registered entity type.</typeparam>
    /// <param name="context">The DbContext.</param>
    /// <param name="url">The URL to fetch data from.</param>
    /// <param name="format">Data format (e.g. CSVWithNames, JSONEachRow).</param>
    /// <param name="structure">Optional column structure. When null, inferred from the EF model.</param>
    /// <returns>An <see cref="IQueryable{T}"/> over the URL data.</returns>
    public static IQueryable<T> FromUrl<T>(
        this DbContext context,
        string url,
        string format,
        string? structure = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(url);
        ArgumentNullException.ThrowIfNull(format);

        structure ??= InferStructure<T>(context);

        var sql = $"SELECT * FROM url({Escape(url)}, {Escape(format)}, {Escape(structure)})";
        return context.Set<T>().FromSqlRaw(sql);
    }

    /// <summary>
    /// Queries data from a remote ClickHouse server using the <c>remote()</c> table function.
    /// </summary>
    /// <typeparam name="T">A registered entity type.</typeparam>
    /// <param name="context">The DbContext.</param>
    /// <param name="addresses">Remote server addresses (e.g. <c>remote-host:9000</c>).</param>
    /// <param name="database">Remote database name.</param>
    /// <param name="table">Remote table name.</param>
    /// <param name="user">Optional remote user.</param>
    /// <param name="password">Optional remote password.</param>
    /// <returns>An <see cref="IQueryable{T}"/> over the remote data.</returns>
    public static IQueryable<T> FromRemote<T>(
        this DbContext context,
        string addresses,
        string database,
        string table,
        string? user = null,
        string? password = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(addresses);
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(table);

        var args = new List<string>
        {
            Escape(addresses),
            Escape(database),
            Escape(table)
        };

        if (user != null)
        {
            args.Add(Escape(user));
            if (password != null)
            {
                args.Add(Escape(password));
            }
        }

        var sql = $"SELECT * FROM remote({string.Join(", ", args)})";
        return context.Set<T>().FromSqlRaw(sql);
    }

    /// <summary>
    /// Queries data from a local file using the <c>file()</c> table function.
    /// </summary>
    /// <typeparam name="T">A registered entity type.</typeparam>
    /// <param name="context">The DbContext.</param>
    /// <param name="path">Path to the file.</param>
    /// <param name="format">Data format (e.g. CSVWithNames, Parquet).</param>
    /// <param name="structure">Optional column structure. When null, inferred from the EF model.</param>
    /// <returns>An <see cref="IQueryable{T}"/> over the file data.</returns>
    public static IQueryable<T> FromFile<T>(
        this DbContext context,
        string path,
        string format,
        string? structure = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(format);

        structure ??= InferStructure<T>(context);

        var sql = $"SELECT * FROM file({Escape(path)}, {Escape(format)}, {Escape(structure)})";
        return context.Set<T>().FromSqlRaw(sql);
    }

    /// <summary>
    /// Queries data from a ClickHouse cluster using the <c>cluster()</c> table function.
    /// </summary>
    /// <typeparam name="T">A registered entity type.</typeparam>
    /// <param name="context">The DbContext.</param>
    /// <param name="clusterName">The cluster name.</param>
    /// <param name="database">The database name on the cluster.</param>
    /// <param name="table">The table name on the cluster.</param>
    /// <returns>An <see cref="IQueryable{T}"/> over the cluster data.</returns>
    public static IQueryable<T> FromCluster<T>(
        this DbContext context,
        string clusterName,
        string database,
        string table) where T : class
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(clusterName);
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(table);

        var sql = $"SELECT * FROM cluster({Escape(clusterName)}, {Escape(database)}, {Escape(table)})";
        return context.Set<T>().FromSqlRaw(sql);
    }

    /// <summary>
    /// Infers ClickHouse column structure from the EF model for the given entity type.
    /// </summary>
    private static string InferStructure<T>(DbContext context) where T : class
    {
        var entityType = context.Model.FindEntityType(typeof(T))
            ?? throw new InvalidOperationException(
                $"Entity type '{typeof(T).Name}' is not registered in the model. " +
                "Table function extensions require a registered entity type.");

        var columns = new List<string>();
        foreach (var property in entityType.GetProperties())
        {
            var columnName = property.GetColumnName() ?? property.Name;
            var storeType = property.GetColumnType();

            if (storeType == null)
            {
                var mapping = property.FindRelationalTypeMapping();
                storeType = mapping?.StoreType ?? "String";
            }

            columns.Add($"{columnName} {storeType}");
        }

        return string.Join(", ", columns);
    }

    /// <summary>
    /// Escapes a string value for use in a ClickHouse table function argument.
    /// </summary>
    private static string Escape(string value)
    {
        return $"'{value.Replace("'", "\\'")}'";
    }
}
