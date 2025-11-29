using System.Linq.Expressions;
using System.Reflection;

namespace EF.CH.Extensions;

/// <summary>
/// Helper methods for extracting property information from expressions.
/// </summary>
internal static class ExpressionExtensions
{
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
