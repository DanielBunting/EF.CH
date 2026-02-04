using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Export;

/// <summary>
/// Extension methods for exporting query results in various ClickHouse formats.
/// </summary>
public static class ClickHouseExportExtensions
{
    /// <summary>
    /// Exports the query results in the specified format.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="query">The query to export.</param>
    /// <param name="format">The export format.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The exported data as a string (for text formats) or base64 encoded (for binary formats).</returns>
    public static async Task<string> ToFormatAsync<T>(
        this IQueryable<T> query,
        ClickHouseExportFormat format,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);

        var context = GetDbContext(query);
        var connection = context.GetService<IRelationalConnection>();

        var sql = query.ToQueryString();
        var formatName = GetFormatName(format);
        var exportSql = $"{sql}\nFORMAT {formatName}";

        await connection.OpenAsync(cancellationToken);
        try
        {
            await using var command = connection.DbConnection.CreateCommand();
            command.CommandText = exportSql;

            if (IsBinaryFormat(format))
            {
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                using var memoryStream = new MemoryStream();

                while (await reader.ReadAsync(cancellationToken))
                {
                    // For binary formats, read the entire blob
                    if (reader.FieldCount > 0)
                    {
                        var bytes = (byte[])reader.GetValue(0);
                        await memoryStream.WriteAsync(bytes, cancellationToken);
                    }
                }

                return Convert.ToBase64String(memoryStream.ToArray());
            }
            else
            {
                var result = new StringBuilder();
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);

                while (await reader.ReadAsync(cancellationToken))
                {
                    if (reader.FieldCount > 0)
                    {
                        result.Append(reader.GetString(0));
                    }
                }

                return result.ToString();
            }
        }
        finally
        {
            await connection.CloseAsync();
        }
    }

    /// <summary>
    /// Exports the query results to a stream.
    /// Best for binary formats (Parquet, Arrow, etc.) or large text outputs.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="query">The query to export.</param>
    /// <param name="format">The export format.</param>
    /// <param name="stream">The stream to write to.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static async Task ToFormatStreamAsync<T>(
        this IQueryable<T> query,
        ClickHouseExportFormat format,
        Stream stream,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(stream);

        var context = GetDbContext(query);
        var connection = context.GetService<IRelationalConnection>();

        var sql = query.ToQueryString();
        var formatName = GetFormatName(format);
        var exportSql = $"{sql}\nFORMAT {formatName}";

        await connection.OpenAsync(cancellationToken);
        try
        {
            await using var command = connection.DbConnection.CreateCommand();
            command.CommandText = exportSql;

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync(cancellationToken))
            {
                if (reader.FieldCount > 0)
                {
                    var value = reader.GetValue(0);
                    if (value is byte[] bytes)
                    {
                        await stream.WriteAsync(bytes, cancellationToken);
                    }
                    else if (value is string str)
                    {
                        var stringBytes = Encoding.UTF8.GetBytes(str);
                        await stream.WriteAsync(stringBytes, cancellationToken);
                    }
                }
            }
        }
        finally
        {
            await connection.CloseAsync();
        }
    }

    /// <summary>
    /// Exports the query results as CSV with column names.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="query">The query to export.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The CSV data as a string.</returns>
    public static Task<string> ToCsvAsync<T>(
        this IQueryable<T> query,
        CancellationToken cancellationToken = default)
    {
        return query.ToFormatAsync(ClickHouseExportFormat.CSVWithNames, cancellationToken);
    }

    /// <summary>
    /// Exports the query results as CSV to a stream.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="query">The query to export.</param>
    /// <param name="stream">The stream to write to.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static Task ToCsvStreamAsync<T>(
        this IQueryable<T> query,
        Stream stream,
        CancellationToken cancellationToken = default)
    {
        return query.ToFormatStreamAsync(ClickHouseExportFormat.CSVWithNames, stream, cancellationToken);
    }

    /// <summary>
    /// Exports the query results as JSON (one object per line).
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="query">The query to export.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The JSON data as a string (newline-delimited JSON).</returns>
    public static Task<string> ToJsonAsync<T>(
        this IQueryable<T> query,
        CancellationToken cancellationToken = default)
    {
        return query.ToFormatAsync(ClickHouseExportFormat.JSONEachRow, cancellationToken);
    }

    /// <summary>
    /// Exports the query results as JSON to a stream.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="query">The query to export.</param>
    /// <param name="stream">The stream to write to.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static Task ToJsonStreamAsync<T>(
        this IQueryable<T> query,
        Stream stream,
        CancellationToken cancellationToken = default)
    {
        return query.ToFormatStreamAsync(ClickHouseExportFormat.JSONEachRow, stream, cancellationToken);
    }

    /// <summary>
    /// Exports the query results as Parquet to a stream.
    /// Parquet is a binary columnar format, ideal for analytics.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="query">The query to export.</param>
    /// <param name="stream">The stream to write to.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static Task ToParquetAsync<T>(
        this IQueryable<T> query,
        Stream stream,
        CancellationToken cancellationToken = default)
    {
        return query.ToFormatStreamAsync(ClickHouseExportFormat.Parquet, stream, cancellationToken);
    }

    /// <summary>
    /// Exports the query results as Arrow to a stream.
    /// Arrow is a binary columnar format, ideal for in-memory analytics.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="query">The query to export.</param>
    /// <param name="stream">The stream to write to.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static Task ToArrowAsync<T>(
        this IQueryable<T> query,
        Stream stream,
        CancellationToken cancellationToken = default)
    {
        return query.ToFormatStreamAsync(ClickHouseExportFormat.Arrow, stream, cancellationToken);
    }

    /// <summary>
    /// Exports the query results as a Markdown table.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="query">The query to export.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The Markdown table as a string.</returns>
    public static Task<string> ToMarkdownAsync<T>(
        this IQueryable<T> query,
        CancellationToken cancellationToken = default)
    {
        return query.ToFormatAsync(ClickHouseExportFormat.Markdown, cancellationToken);
    }

    /// <summary>
    /// Exports the query results as XML.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="query">The query to export.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The XML data as a string.</returns>
    public static Task<string> ToXmlAsync<T>(
        this IQueryable<T> query,
        CancellationToken cancellationToken = default)
    {
        return query.ToFormatAsync(ClickHouseExportFormat.XML, cancellationToken);
    }

    /// <summary>
    /// Exports the query results in the specified format synchronously.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="query">The query to export.</param>
    /// <param name="format">The export format.</param>
    /// <returns>The exported data as a string (for text formats) or base64 encoded (for binary formats).</returns>
    public static string ToFormat<T>(this IQueryable<T> query, ClickHouseExportFormat format)
    {
        return query.ToFormatAsync(format).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Exports the query results as CSV with column names synchronously.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="query">The query to export.</param>
    /// <returns>The CSV data as a string.</returns>
    public static string ToCsv<T>(this IQueryable<T> query)
    {
        return query.ToCsvAsync().GetAwaiter().GetResult();
    }

    /// <summary>
    /// Exports the query results as JSON (one object per line) synchronously.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="query">The query to export.</param>
    /// <returns>The JSON data as a string (newline-delimited JSON).</returns>
    public static string ToJson<T>(this IQueryable<T> query)
    {
        return query.ToJsonAsync().GetAwaiter().GetResult();
    }

    /// <summary>
    /// Gets the ClickHouse format name string.
    /// </summary>
    private static string GetFormatName(ClickHouseExportFormat format)
    {
        return format switch
        {
            ClickHouseExportFormat.CSVWithNames => "CSVWithNames",
            ClickHouseExportFormat.TabSeparatedWithNames => "TabSeparatedWithNames",
            ClickHouseExportFormat.JSONEachRow => "JSONEachRow",
            ClickHouseExportFormat.JSON => "JSON",
            ClickHouseExportFormat.JSONCompact => "JSONCompact",
            ClickHouseExportFormat.JSONCompactWithNames => "JSONCompactWithNames",
            ClickHouseExportFormat.JSONStrings => "JSONStrings",
            ClickHouseExportFormat.Parquet => "Parquet",
            ClickHouseExportFormat.Arrow => "Arrow",
            ClickHouseExportFormat.ORC => "ORC",
            ClickHouseExportFormat.Avro => "Avro",
            ClickHouseExportFormat.Native => "Native",
            ClickHouseExportFormat.Pretty => "Pretty",
            ClickHouseExportFormat.PrettyCompact => "PrettyCompact",
            ClickHouseExportFormat.Markdown => "Markdown",
            ClickHouseExportFormat.TabSeparatedRaw => "TabSeparatedRaw",
            ClickHouseExportFormat.XML => "XML",
            ClickHouseExportFormat.RowBinary => "RowBinary",
            ClickHouseExportFormat.RowBinaryWithNames => "RowBinaryWithNames",
            ClickHouseExportFormat.Values => "Values",
            ClickHouseExportFormat.Vertical => "Vertical",
            ClickHouseExportFormat.CustomSeparated => "CustomSeparated",
            _ => throw new ArgumentOutOfRangeException(nameof(format), format, "Unsupported export format")
        };
    }

    /// <summary>
    /// Determines if the format produces binary output.
    /// </summary>
    private static bool IsBinaryFormat(ClickHouseExportFormat format)
    {
        return format switch
        {
            ClickHouseExportFormat.Parquet => true,
            ClickHouseExportFormat.Arrow => true,
            ClickHouseExportFormat.ORC => true,
            ClickHouseExportFormat.Avro => true,
            ClickHouseExportFormat.Native => true,
            ClickHouseExportFormat.RowBinary => true,
            ClickHouseExportFormat.RowBinaryWithNames => true,
            _ => false
        };
    }

    /// <summary>
    /// Extracts the DbContext from an IQueryable.
    /// </summary>
    private static DbContext GetDbContext<T>(IQueryable<T> query)
    {
        var contextProperty = query.Provider.GetType()
            .GetProperty("Context", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
            ?? query.Provider.GetType()
                .GetProperty("Context", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);

        if (contextProperty != null)
        {
            var context = contextProperty.GetValue(query.Provider) as DbContext;
            if (context != null)
            {
                return context;
            }
        }

        // Try via internal dependencies (EF Core 8+)
        var dependenciesProperty = query.Provider.GetType()
            .GetProperty("Dependencies", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        if (dependenciesProperty != null)
        {
            var dependencies = dependenciesProperty.GetValue(query.Provider);
            if (dependencies != null)
            {
                var currentContextProperty = dependencies.GetType()
                    .GetProperty("CurrentContext");

                if (currentContextProperty != null)
                {
                    var currentContext = currentContextProperty.GetValue(dependencies);
                    if (currentContext != null)
                    {
                        var contextProp = currentContext.GetType()
                            .GetProperty("Context");

                        if (contextProp?.GetValue(currentContext) is DbContext ctx)
                        {
                            return ctx;
                        }
                    }
                }
            }
        }

        throw new InvalidOperationException(
            "Unable to get DbContext from query. Ensure the query is from an EF Core DbContext.");
    }
}
