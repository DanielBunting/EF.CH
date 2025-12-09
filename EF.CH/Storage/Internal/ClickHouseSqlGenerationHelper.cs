using System.Text;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Storage.Internal;

/// <summary>
/// Provides ClickHouse-specific SQL generation utilities including identifier quoting,
/// literal escaping, and parameter placeholder formatting.
/// </summary>
public class ClickHouseSqlGenerationHelper : RelationalSqlGenerationHelper
{
    /// <summary>
    /// ClickHouse uses double quotes for identifier delimiters.
    /// Backticks are also supported but double quotes are more standard.
    /// </summary>
    private const char IdentifierDelimiter = '"';

    public ClickHouseSqlGenerationHelper(RelationalSqlGenerationHelperDependencies dependencies)
        : base(dependencies)
    {
    }

    /// <summary>
    /// Gets the character used for single-line comments in ClickHouse.
    /// </summary>
    public override string SingleLineCommentToken => "--";

    /// <summary>
    /// Delimits (quotes) an identifier to make it safe for use in SQL.
    /// </summary>
    public override string DelimitIdentifier(string identifier)
        => $"{IdentifierDelimiter}{EscapeIdentifier(identifier)}{IdentifierDelimiter}";

    /// <summary>
    /// Writes a delimited identifier to the StringBuilder.
    /// </summary>
    public override void DelimitIdentifier(StringBuilder builder, string identifier)
    {
        builder.Append(IdentifierDelimiter);
        EscapeIdentifier(builder, identifier);
        builder.Append(IdentifierDelimiter);
    }

    /// <summary>
    /// Delimits an identifier with a schema (database in ClickHouse terms).
    /// </summary>
    public override string DelimitIdentifier(string name, string? schema)
        => schema is null
            ? DelimitIdentifier(name)
            : $"{DelimitIdentifier(schema)}.{DelimitIdentifier(name)}";

    /// <summary>
    /// Writes a schema-qualified delimited identifier to the StringBuilder.
    /// </summary>
    public override void DelimitIdentifier(StringBuilder builder, string name, string? schema)
    {
        if (schema is not null)
        {
            DelimitIdentifier(builder, schema);
            builder.Append('.');
        }
        DelimitIdentifier(builder, name);
    }

    /// <summary>
    /// Escapes an identifier by doubling any embedded quotes.
    /// </summary>
    public override string EscapeIdentifier(string identifier)
        => identifier.Replace("\"", "\"\"");

    /// <summary>
    /// Writes an escaped identifier to the StringBuilder.
    /// </summary>
    public override void EscapeIdentifier(StringBuilder builder, string identifier)
    {
        var start = 0;
        for (var i = 0; i < identifier.Length; i++)
        {
            if (identifier[i] == IdentifierDelimiter)
            {
                builder.Append(identifier, start, i - start + 1);
                builder.Append(IdentifierDelimiter);
                start = i + 1;
            }
        }

        if (start < identifier.Length)
        {
            builder.Append(identifier, start, identifier.Length - start);
        }
    }

    /// <summary>
    /// Generates a parameter name for use in ClickHouse parameterized queries.
    /// ClickHouse.Driver expects parameter names without prefix (no @ or : prefix).
    /// The {name:Type} format is generated in the QuerySqlGenerator.
    /// </summary>
    public override string GenerateParameterName(string name)
        => name;

    /// <summary>
    /// Writes a parameter name to the StringBuilder.
    /// </summary>
    public override void GenerateParameterName(StringBuilder builder, string name)
        => builder.Append(name);

    /// <summary>
    /// Generates a ClickHouse parameter placeholder with type information.
    /// Format: {paramName:ClickHouseType}
    /// </summary>
    public virtual void GenerateParameterNamePlaceholder(
        StringBuilder builder,
        string name,
        string? storeType)
    {
        builder.Append('{');
        builder.Append(name);
        if (storeType is not null)
        {
            builder.Append(':');
            builder.Append(storeType);
        }
        builder.Append('}');
    }

    /// <summary>
    /// Generates the SQL representation of a boolean value.
    /// ClickHouse uses true/false keywords or 1/0.
    /// </summary>
    public virtual string GenerateBooleanLiteral(bool value)
        => value ? "true" : "false";

    /// <summary>
    /// Writes a boolean literal to the StringBuilder.
    /// </summary>
    public virtual void GenerateBooleanLiteral(StringBuilder builder, bool value)
        => builder.Append(value ? "true" : "false");

    /// <summary>
    /// Escapes a string literal for safe use in ClickHouse SQL.
    /// Handles single quotes and backslashes, plus control characters.
    /// ClickHouse uses backslash escaping unlike standard SQL doubling.
    /// </summary>
    public virtual string EscapeClickHouseLiteral(string literal)
    {
        var builder = new StringBuilder(literal.Length + 10);
        EscapeClickHouseLiteral(builder, literal);
        return builder.ToString();
    }

    /// <summary>
    /// Writes an escaped string literal to the StringBuilder.
    /// </summary>
    public virtual void EscapeClickHouseLiteral(StringBuilder builder, string literal)
    {
        foreach (var c in literal)
        {
            switch (c)
            {
                case '\'':
                    builder.Append("\\'");
                    break;
                case '\\':
                    builder.Append("\\\\");
                    break;
                case '\n':
                    builder.Append("\\n");
                    break;
                case '\r':
                    builder.Append("\\r");
                    break;
                case '\t':
                    builder.Append("\\t");
                    break;
                case '\0':
                    builder.Append("\\0");
                    break;
                default:
                    builder.Append(c);
                    break;
            }
        }
    }

    /// <summary>
    /// Generates a string literal for ClickHouse.
    /// ClickHouse uses single quotes with backslash escaping.
    /// </summary>
    public virtual string GenerateClickHouseLiteral(string value)
        => $"'{EscapeClickHouseLiteral(value)}'";

    /// <summary>
    /// Writes a string literal to the StringBuilder using ClickHouse escaping.
    /// </summary>
    public virtual void GenerateClickHouseLiteral(StringBuilder builder, string value)
    {
        builder.Append('\'');
        EscapeClickHouseLiteral(builder, value);
        builder.Append('\'');
    }
}
