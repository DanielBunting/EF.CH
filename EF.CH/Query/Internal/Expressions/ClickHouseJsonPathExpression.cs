using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using static System.Linq.Expressions.Expression;

namespace EF.CH.Query.Internal.Expressions;

/// <summary>
/// Represents JSON path access in ClickHouse using native JSON subcolumn syntax.
/// Examples: "column"."path"."subpath" or "column"."array"[1]
/// </summary>
/// <remarks>
/// This expression is used to translate GetPath&lt;T&gt;("path.to.field") calls
/// to ClickHouse's native JSON subcolumn access syntax.
/// </remarks>
public sealed class ClickHouseJsonPathExpression : SqlExpression
{
    /// <summary>
    /// Gets the JSON column expression.
    /// </summary>
    public SqlExpression Column { get; }

    /// <summary>
    /// Gets the path segments (e.g., ["user", "email"] for "user.email").
    /// </summary>
    public IReadOnlyList<string> PathSegments { get; }

    /// <summary>
    /// Gets optional array indices for each segment.
    /// Null means no array index for that segment.
    /// </summary>
    public IReadOnlyList<int?> ArrayIndices { get; }

    /// <summary>
    /// Creates a new JSON path expression.
    /// </summary>
    /// <param name="column">The JSON column expression.</param>
    /// <param name="pathSegments">The path segments.</param>
    /// <param name="arrayIndices">Optional array indices (null if not an array access).</param>
    /// <param name="type">The CLR type of the result.</param>
    /// <param name="typeMapping">The type mapping for the result.</param>
    public ClickHouseJsonPathExpression(
        SqlExpression column,
        IReadOnlyList<string> pathSegments,
        IReadOnlyList<int?>? arrayIndices,
        Type type,
        RelationalTypeMapping? typeMapping)
        : base(type, typeMapping)
    {
        Column = column ?? throw new ArgumentNullException(nameof(column));
        PathSegments = pathSegments ?? throw new ArgumentNullException(nameof(pathSegments));
        ArrayIndices = arrayIndices ?? Enumerable.Repeat<int?>(null, pathSegments.Count).ToList();

        if (ArrayIndices.Count != PathSegments.Count)
        {
            throw new ArgumentException(
                "ArrayIndices must have the same count as PathSegments",
                nameof(arrayIndices));
        }
    }

    /// <summary>
    /// Creates a new JSON path expression from a dot-separated path string.
    /// </summary>
    public static ClickHouseJsonPathExpression Create(
        SqlExpression column,
        string path,
        Type type,
        RelationalTypeMapping? typeMapping)
    {
        var (segments, indices) = ParsePath(path);
        return new ClickHouseJsonPathExpression(column, segments, indices, type, typeMapping);
    }

    /// <summary>
    /// Parses a path string like "user.email" or "tags[0].name" into segments and indices.
    /// </summary>
    private static (List<string> segments, List<int?> indices) ParsePath(string path)
    {
        var segments = new List<string>();
        var indices = new List<int?>();

        // Split by dots, handling array indices
        var current = "";
        var i = 0;

        while (i < path.Length)
        {
            var c = path[i];

            if (c == '.')
            {
                if (current.Length > 0)
                {
                    AddSegment(current, segments, indices);
                    current = "";
                }
                i++;
            }
            else if (c == '[')
            {
                // Array index
                if (current.Length > 0)
                {
                    segments.Add(current);
                    current = "";
                }
                else if (segments.Count == 0)
                {
                    throw new ArgumentException($"Invalid path: array index without segment at position {i}");
                }

                // Parse the index
                var endBracket = path.IndexOf(']', i);
                if (endBracket == -1)
                {
                    throw new ArgumentException($"Invalid path: unclosed bracket at position {i}");
                }

                var indexStr = path.Substring(i + 1, endBracket - i - 1);
                if (!int.TryParse(indexStr, out var index))
                {
                    throw new ArgumentException($"Invalid path: non-integer array index '{indexStr}'");
                }

                // Add the index to the last segment
                if (indices.Count < segments.Count)
                {
                    // Need to add a null for previous segment
                    indices.Add(null);
                }
                indices[indices.Count - 1] = index;

                i = endBracket + 1;
            }
            else
            {
                current += c;
                i++;
            }
        }

        if (current.Length > 0)
        {
            AddSegment(current, segments, indices);
        }

        // Ensure indices list matches segments list
        while (indices.Count < segments.Count)
        {
            indices.Add(null);
        }

        return (segments, indices);
    }

    private static void AddSegment(string segment, List<string> segments, List<int?> indices)
    {
        segments.Add(segment);
        indices.Add(null);
    }

    /// <summary>
    /// Applies the specified type mapping to this expression.
    /// </summary>
    public ClickHouseJsonPathExpression ApplyTypeMapping(RelationalTypeMapping? typeMapping)
        => new(Column, PathSegments, ArrayIndices, Type, typeMapping ?? TypeMapping);

    /// <inheritdoc />
    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        var newColumn = (SqlExpression)visitor.Visit(Column);
        return newColumn != Column
            ? new ClickHouseJsonPathExpression(newColumn, PathSegments, ArrayIndices, Type, TypeMapping)
            : this;
    }

    /// <summary>
    /// Creates an updated expression with a new column.
    /// </summary>
    public ClickHouseJsonPathExpression Update(SqlExpression column)
    {
        return ReferenceEquals(column, Column)
            ? this
            : new ClickHouseJsonPathExpression(column, PathSegments, ArrayIndices, Type, TypeMapping);
    }

    /// <inheritdoc />
    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Visit(Column);

        for (var i = 0; i < PathSegments.Count; i++)
        {
            expressionPrinter.Append(".");
            expressionPrinter.Append($"\"{PathSegments[i]}\"");

            if (ArrayIndices[i].HasValue)
            {
                // ClickHouse uses 1-based indexing for arrays
                expressionPrinter.Append($"[{ArrayIndices[i].Value + 1}]");
            }
        }
    }

    /// <inheritdoc />
    public override bool Equals(object? obj)
        => obj is ClickHouseJsonPathExpression other && Equals(other);

    private bool Equals(ClickHouseJsonPathExpression other)
        => base.Equals(other)
           && Column.Equals(other.Column)
           && PathSegments.SequenceEqual(other.PathSegments)
           && ArrayIndices.SequenceEqual(other.ArrayIndices);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(base.GetHashCode());
        hash.Add(Column);
        foreach (var segment in PathSegments) hash.Add(segment);
        foreach (var index in ArrayIndices) hash.Add(index);
        return hash.ToHashCode();
    }
}
