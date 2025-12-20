using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace EF.CH.Extensions;

/// <summary>
/// Helper methods for extracting property information from expressions.
/// </summary>
internal static class ExpressionExtensions
{
    /// <summary>
    /// Converts a PascalCase or camelCase string to snake_case.
    /// Examples: "OrderDate" → "order_date", "XMLParser" → "xml_parser", "ID" → "id"
    /// </summary>
    public static string ToSnakeCase(string input)
    {
        if (string.IsNullOrEmpty(input))
            return input;

        var sb = new StringBuilder();
        for (var i = 0; i < input.Length; i++)
        {
            var c = input[i];
            if (char.IsUpper(c))
            {
                // Insert underscore before uppercase if:
                // - Not at the start, AND
                // - Previous char is lowercase, OR
                // - Next char is lowercase (handles "XMLParser" → "xml_parser")
                if (i > 0)
                {
                    var prevIsLower = char.IsLower(input[i - 1]);
                    var nextIsLower = i + 1 < input.Length && char.IsLower(input[i + 1]);
                    if (prevIsLower || nextIsLower)
                    {
                        sb.Append('_');
                    }
                }
                sb.Append(char.ToLowerInvariant(c));
            }
            else
            {
                sb.Append(c);
            }
        }
        return sb.ToString();
    }

    /// <summary>
    /// Extracts property names from an expression that selects one or more properties.
    /// Supports: x => x.Prop, x => new { x.Prop1, x.Prop2 }, x => (x.Prop1, x.Prop2)
    /// </summary>
    public static string[] GetPropertyNames<TEntity>(Expression<Func<TEntity, object>> expression)
    {
        ArgumentNullException.ThrowIfNull(expression);

        return expression.Body switch
        {
            // Single property: x => x.Property
            MemberExpression member => [GetPropertyName(member)],

            // Single property with conversion: x => (object)x.Property
            UnaryExpression { Operand: MemberExpression memberFromUnary } => [GetPropertyName(memberFromUnary)],

            // Anonymous type or tuple: x => new { x.Prop1, x.Prop2 } or x => (x.Prop1, x.Prop2)
            // Both create NewExpression (ValueTuple for tuples, anonymous type for new { })
            NewExpression newExpr => newExpr.Arguments
                .Select(arg => arg switch
                {
                    MemberExpression m => GetPropertyName(m),
                    UnaryExpression { Operand: MemberExpression um } => GetPropertyName(um),
                    _ => throw new ArgumentException($"Unsupported expression type in composite expression: {arg.GetType().Name}")
                })
                .ToArray(),

            _ => throw new ArgumentException(
                $"Expression must be a property access, anonymous type, or tuple. Got: {expression.Body.GetType().Name}")
        };
    }

    /// <summary>
    /// Gets the column name for a property, respecting any column name configuration.
    /// </summary>
    private static string GetPropertyName(MemberExpression member)
    {
        if (member.Member is not PropertyInfo property)
        {
            throw new ArgumentException($"Expression must reference a property, not a {member.Member.MemberType}");
        }

        // Return the property name - EF Core will map this to the column name
        return property.Name;
    }

    /// <summary>
    /// Extracts a single property name from an expression.
    /// </summary>
    public static string GetPropertyName<TEntity, TProperty>(Expression<Func<TEntity, TProperty>> expression)
    {
        ArgumentNullException.ThrowIfNull(expression);

        var member = expression.Body switch
        {
            MemberExpression m => m,
            UnaryExpression { Operand: MemberExpression um } => um,
            _ => throw new ArgumentException(
                $"Expression must be a property access. Got: {expression.Body.GetType().Name}")
        };

        if (member.Member is not PropertyInfo property)
        {
            throw new ArgumentException($"Expression must reference a property, not a {member.Member.MemberType}");
        }

        return property.Name;
    }
}
