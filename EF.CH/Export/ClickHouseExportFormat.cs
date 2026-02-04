namespace EF.CH.Export;

/// <summary>
/// ClickHouse output formats for exporting query results.
/// </summary>
public enum ClickHouseExportFormat
{
    /// <summary>
    /// CSV format with column names in the first row.
    /// Good for spreadsheet compatibility.
    /// </summary>
    CSVWithNames,

    /// <summary>
    /// Tab-separated values with column names in the first row.
    /// Good for simple parsing.
    /// </summary>
    TabSeparatedWithNames,

    /// <summary>
    /// One JSON object per line (newline-delimited JSON).
    /// Good for streaming and log processing.
    /// </summary>
    JSONEachRow,

    /// <summary>
    /// Standard JSON with rows as objects.
    /// Includes metadata about the query.
    /// </summary>
    JSON,

    /// <summary>
    /// Compact JSON with rows as arrays instead of objects.
    /// Smaller output size than JSON.
    /// </summary>
    JSONCompact,

    /// <summary>
    /// JSON with column names in the first array.
    /// Alternative to JSONCompact with embedded schema.
    /// </summary>
    JSONCompactWithNames,

    /// <summary>
    /// JSON as an array of string values.
    /// All values are stringified.
    /// </summary>
    JSONStrings,

    /// <summary>
    /// Apache Parquet format (binary columnar format).
    /// Best for analytics and interoperability with data tools.
    /// </summary>
    Parquet,

    /// <summary>
    /// Apache Arrow format (binary columnar format).
    /// Good for in-memory analytics.
    /// </summary>
    Arrow,

    /// <summary>
    /// Apache ORC format (binary columnar format).
    /// Alternative to Parquet for Hadoop ecosystem.
    /// </summary>
    ORC,

    /// <summary>
    /// Apache Avro format (binary row-based format).
    /// Good for schema evolution.
    /// </summary>
    Avro,

    /// <summary>
    /// ClickHouse native binary format.
    /// Most efficient for ClickHouse-to-ClickHouse data transfer.
    /// </summary>
    Native,

    /// <summary>
    /// Human-readable table format for terminal output.
    /// </summary>
    Pretty,

    /// <summary>
    /// Compact human-readable table format.
    /// </summary>
    PrettyCompact,

    /// <summary>
    /// Markdown table format.
    /// Good for documentation.
    /// </summary>
    Markdown,

    /// <summary>
    /// Raw values separated by tabs, no escaping.
    /// </summary>
    TabSeparatedRaw,

    /// <summary>
    /// XML format.
    /// </summary>
    XML,

    /// <summary>
    /// RowBinary format (binary row-based format).
    /// Efficient for row-by-row processing.
    /// </summary>
    RowBinary,

    /// <summary>
    /// RowBinary format with column names.
    /// </summary>
    RowBinaryWithNames,

    /// <summary>
    /// Values format (SQL INSERT values syntax).
    /// Good for data migration.
    /// </summary>
    Values,

    /// <summary>
    /// Vertical format (one column per line).
    /// Good for wide tables.
    /// </summary>
    Vertical,

    /// <summary>
    /// Template-based custom format.
    /// Requires template configuration.
    /// </summary>
    CustomSeparated
}
