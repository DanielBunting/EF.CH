using System.Text;
using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods for exporting ClickHouse query results in various formats.
/// Uses direct HTTP requests to ClickHouse, bypassing the ADO.NET driver's binary protocol
/// so that non-native formats (CSV, JSON, etc.) can be retrieved as raw text.
/// </summary>
public static class ClickHouseExportExtensions
{
    /// <summary>
    /// Executes the query and returns results in the specified ClickHouse format.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="query">The LINQ query to export.</param>
    /// <param name="context">The DbContext associated with the query.</param>
    /// <param name="format">The ClickHouse format (e.g. CSVWithNames, JSON, JSONEachRow, Parquet).</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The formatted output as a string.</returns>
    public static async Task<string> ToFormatAsync<T>(
        this IQueryable<T> query,
        DbContext context,
        string format,
        CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(format);

        var sql = query.ToQueryString() + $" FORMAT {format}";

        using var client = CreateHttpClient(context);
        var requestUrl = BuildRequestUrl(context);
        var content = new StringContent(sql, Encoding.UTF8, "text/plain");

        var response = await client.PostAsync(requestUrl, content, cancellationToken);
        response.EnsureSuccessStatusCode();

        return await response.Content.ReadAsStringAsync(cancellationToken);
    }

    /// <summary>
    /// Executes the query and returns results in CSV format with column headers.
    /// </summary>
    public static Task<string> ToCsvAsync<T>(
        this IQueryable<T> query,
        DbContext context,
        CancellationToken cancellationToken = default) where T : class
    {
        return query.ToFormatAsync(context, "CSVWithNames", cancellationToken);
    }

    /// <summary>
    /// Executes the query and returns results in JSON format.
    /// </summary>
    public static Task<string> ToJsonAsync<T>(
        this IQueryable<T> query,
        DbContext context,
        CancellationToken cancellationToken = default) where T : class
    {
        return query.ToFormatAsync(context, "JSON", cancellationToken);
    }

    /// <summary>
    /// Executes the query and returns results in JSON Lines format (one JSON object per line).
    /// </summary>
    public static Task<string> ToJsonLinesAsync<T>(
        this IQueryable<T> query,
        DbContext context,
        CancellationToken cancellationToken = default) where T : class
    {
        return query.ToFormatAsync(context, "JSONEachRow", cancellationToken);
    }

    /// <summary>
    /// Executes the query and streams the formatted output to the provided stream.
    /// Useful for large exports (Parquet, CSV) where buffering in memory is impractical.
    /// </summary>
    public static async Task ToFormatStreamAsync<T>(
        this IQueryable<T> query,
        DbContext context,
        string format,
        Stream stream,
        CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(format);
        ArgumentNullException.ThrowIfNull(stream);

        var sql = query.ToQueryString() + $" FORMAT {format}";

        using var client = CreateHttpClient(context);
        var requestUrl = BuildRequestUrl(context);
        var content = new StringContent(sql, Encoding.UTF8, "text/plain");

        var response = await client.PostAsync(requestUrl, content, cancellationToken);
        response.EnsureSuccessStatusCode();

        await using var responseStream = await response.Content.ReadAsStreamAsync(cancellationToken);
        await responseStream.CopyToAsync(stream, cancellationToken);
    }

    /// <summary>
    /// Parses a ClickHouse connection string to extract the HTTP base URL and database name.
    /// Supports both <c>Host=...;Port=...;Database=...</c> format and URI format.
    /// </summary>
    private static HttpClient CreateHttpClient(DbContext context)
    {
        var connStr = context.Database.GetConnectionString()
            ?? throw new InvalidOperationException("No connection string configured.");
        var (_, _, username, password) = ParseConnectionString(connStr);

        var client = new HttpClient();
        if (username != null)
        {
            var credentials = Convert.ToBase64String(
                Encoding.UTF8.GetBytes($"{username}:{password ?? ""}"));
            client.DefaultRequestHeaders.Authorization =
                new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", credentials);
        }

        return client;
    }

    private static string BuildRequestUrl(DbContext context)
    {
        var connStr = context.Database.GetConnectionString()
            ?? throw new InvalidOperationException("No connection string configured.");
        var (baseUrl, database, _, _) = ParseConnectionString(connStr);
        return $"{baseUrl}/?database={Uri.EscapeDataString(database)}";
    }

    private static (string BaseUrl, string Database, string? Username, string? Password) ParseConnectionString(string connectionString)
    {
        var parts = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries);
        string host = "localhost";
        int port = 8123;
        string database = "default";
        string? username = null;
        string? password = null;

        foreach (var part in parts)
        {
            var kv = part.Split('=', 2);
            if (kv.Length != 2) continue;

            var key = kv[0].Trim();
            var value = kv[1].Trim();

            if (key.Equals("Host", StringComparison.OrdinalIgnoreCase))
            {
                host = value;
            }
            else if (key.Equals("Port", StringComparison.OrdinalIgnoreCase) &&
                     int.TryParse(value, out var p))
            {
                port = p;
            }
            else if (key.Equals("Database", StringComparison.OrdinalIgnoreCase))
            {
                database = value;
            }
            else if (key.Equals("Username", StringComparison.OrdinalIgnoreCase))
            {
                username = value;
            }
            else if (key.Equals("Password", StringComparison.OrdinalIgnoreCase))
            {
                password = value;
            }
        }

        return ($"http://{host}:{port}", database, username, password);
    }
}
