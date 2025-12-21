using System.Reflection;
using EF.CH.Metadata.Attributes;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;

namespace EF.CH.Metadata.Conventions;

/// <summary>
/// Discovers MaterializedColumn, AliasColumn, and DefaultExpression attributes
/// and applies them as annotations. Also configures ValueGenerated appropriately.
/// </summary>
/// <remarks>
/// <para>
/// MATERIALIZED columns are configured with <c>ValueGenerated.OnAdd</c> to exclude
/// them from INSERT statements, since ClickHouse computes their values.
/// </para>
/// <para>
/// ALIAS columns are configured with <c>ValueGenerated.OnAddOrUpdate</c> to exclude
/// them from all modifications, since they are virtual computed columns.
/// </para>
/// <para>
/// This convention uses <c>fromDataAnnotation: true</c> to allow fluent API
/// configuration to override attribute-based configuration.
/// </para>
/// </remarks>
public class ClickHouseComputedColumnConvention : IPropertyAddedConvention
{
    /// <inheritdoc />
    public void ProcessPropertyAdded(
        IConventionPropertyBuilder propertyBuilder,
        IConventionContext<IConventionPropertyBuilder> context)
    {
        var property = propertyBuilder.Metadata;
        var memberInfo = property.PropertyInfo ?? (MemberInfo?)property.FieldInfo;

        if (memberInfo == null)
            return;

        // Check for MaterializedColumn (mutually exclusive with others)
        var materializedAttr = memberInfo.GetCustomAttribute<MaterializedColumnAttribute>();
        if (materializedAttr != null)
        {
            propertyBuilder.HasAnnotation(
                ClickHouseAnnotationNames.MaterializedExpression,
                materializedAttr.Expression,
                fromDataAnnotation: true);

            // MATERIALIZED columns are computed on INSERT - don't include in INSERT statements
            propertyBuilder.ValueGenerated(ValueGenerated.OnAdd, fromDataAnnotation: true);
            return;
        }

        // Check for AliasColumn
        var aliasAttr = memberInfo.GetCustomAttribute<AliasColumnAttribute>();
        if (aliasAttr != null)
        {
            propertyBuilder.HasAnnotation(
                ClickHouseAnnotationNames.AliasExpression,
                aliasAttr.Expression,
                fromDataAnnotation: true);

            // ALIAS columns are always computed - never include in INSERT/UPDATE
            propertyBuilder.ValueGenerated(ValueGenerated.OnAddOrUpdate, fromDataAnnotation: true);
            return;
        }

        // Check for DefaultExpression
        var defaultAttr = memberInfo.GetCustomAttribute<DefaultExpressionAttribute>();
        if (defaultAttr != null)
        {
            propertyBuilder.HasAnnotation(
                ClickHouseAnnotationNames.DefaultExpression,
                defaultAttr.Expression,
                fromDataAnnotation: true);
        }
    }
}
