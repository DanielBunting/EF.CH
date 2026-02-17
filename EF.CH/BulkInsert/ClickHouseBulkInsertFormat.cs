namespace EF.CH.BulkInsert;

/// <summary>
/// Specifies the format to use for bulk insert operations.
/// </summary>
public enum ClickHouseBulkInsertFormat
{
    /// <summary>
    /// Standard SQL VALUES format: INSERT INTO table VALUES (...), (...), ...
    /// This is the default format and works with all ClickHouse versions.
    /// </summary>
    Values,

    /// <summary>
    /// JSONEachRow format: INSERT INTO table FORMAT JSONEachRow followed by one JSON object per line.
    /// Useful when data contains complex types or when JSON serialization is preferred.
    /// </summary>
    JsonEachRow
}
