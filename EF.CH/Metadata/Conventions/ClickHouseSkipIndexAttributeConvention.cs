using System.Reflection;
using EF.CH.Metadata.Attributes;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;

namespace EF.CH.Metadata.Conventions;

/// <summary>
/// Convention that discovers <see cref="SkipIndexAttribute"/> on properties
/// and creates corresponding indices with ClickHouse-specific annotations.
/// </summary>
/// <remarks>
/// This convention runs during model finalization. It checks for
/// <see cref="SkipIndexAttribute"/> or any derived attribute (like <see cref="BloomFilterIndexAttribute"/>)
/// and creates an index with appropriate annotations for skip index type, granularity, and parameters.
/// </remarks>
public class ClickHouseSkipIndexAttributeConvention : IModelFinalizingConvention
{
    /// <inheritdoc />
    public void ProcessModelFinalizing(
        IConventionModelBuilder modelBuilder,
        IConventionContext<IConventionModelBuilder> context)
    {
        foreach (var entityType in modelBuilder.Metadata.GetEntityTypes())
        {
            foreach (var property in entityType.GetProperties())
            {
                var memberInfo = property.PropertyInfo ?? (MemberInfo?)property.FieldInfo;
                if (memberInfo == null)
                    continue;

                var skipIndexAttributes = memberInfo.GetCustomAttributes<SkipIndexAttribute>(inherit: true);
                var attributeIndex = 0;

                foreach (var attr in skipIndexAttributes)
                {
                    // Determine index name
                    var indexName = attr.Name ?? GenerateIndexName(entityType.GetTableName() ?? entityType.Name, property.Name, attributeIndex);

                    // Check if index already exists (e.g., from fluent API)
                    var existingIndex = entityType.FindIndex(property);
                    if (existingIndex != null)
                    {
                        // Update existing index with annotations (if not already set by fluent API)
                        SetAnnotationsIfNotPresent(existingIndex.Builder, attr);
                    }
                    else
                    {
                        // Create new index
                        var indexBuilder = entityType.Builder.HasIndex(new[] { property.Name }, indexName, fromDataAnnotation: true);
                        if (indexBuilder != null)
                        {
                            SetAnnotations(indexBuilder, attr);
                        }
                    }

                    attributeIndex++;
                }
            }
        }
    }

    private static string GenerateIndexName(string tableName, string propertyName, int attributeIndex)
    {
        var suffix = attributeIndex > 0 ? $"_{attributeIndex}" : "";
        return $"IX_{tableName}_{propertyName}{suffix}";
    }

    private static void SetAnnotations(IConventionIndexBuilder indexBuilder, SkipIndexAttribute attr)
    {
        indexBuilder.HasAnnotation(
            ClickHouseAnnotationNames.SkipIndexType,
            attr.IndexType,
            fromDataAnnotation: true);

        indexBuilder.HasAnnotation(
            ClickHouseAnnotationNames.SkipIndexGranularity,
            attr.Granularity,
            fromDataAnnotation: true);

        var indexParams = attr.GetParams();
        indexBuilder.HasAnnotation(
            ClickHouseAnnotationNames.SkipIndexParams,
            indexParams,
            fromDataAnnotation: true);
    }

    private static void SetAnnotationsIfNotPresent(IConventionIndexBuilder indexBuilder, SkipIndexAttribute attr)
    {
        var index = indexBuilder.Metadata;

        // Only set if not already configured via fluent API
        if (index.FindAnnotation(ClickHouseAnnotationNames.SkipIndexType) == null)
        {
            indexBuilder.HasAnnotation(
                ClickHouseAnnotationNames.SkipIndexType,
                attr.IndexType,
                fromDataAnnotation: true);
        }

        if (index.FindAnnotation(ClickHouseAnnotationNames.SkipIndexGranularity) == null)
        {
            indexBuilder.HasAnnotation(
                ClickHouseAnnotationNames.SkipIndexGranularity,
                attr.Granularity,
                fromDataAnnotation: true);
        }

        if (index.FindAnnotation(ClickHouseAnnotationNames.SkipIndexParams) == null)
        {
            var indexParams = attr.GetParams();
            indexBuilder.HasAnnotation(
                ClickHouseAnnotationNames.SkipIndexParams,
                indexParams,
                fromDataAnnotation: true);
        }
    }
}
