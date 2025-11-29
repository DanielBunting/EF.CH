using System.Reflection;
using EF.CH.Extensions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Design.Internal;

/// <summary>
/// Generates C# code for ClickHouse-specific annotations in migrations.
/// </summary>
public class ClickHouseAnnotationCodeGenerator : AnnotationCodeGenerator
{
    private static readonly MethodInfo UseMergeTreeMethodInfo
        = typeof(ClickHouseEntityTypeBuilderExtensions).GetRuntimeMethod(
            nameof(ClickHouseEntityTypeBuilderExtensions.UseMergeTree),
            [typeof(EntityTypeBuilder), typeof(string[])])!;

    private static readonly MethodInfo UseReplacingMergeTreeMethodInfo
        = typeof(ClickHouseEntityTypeBuilderExtensions).GetRuntimeMethod(
            nameof(ClickHouseEntityTypeBuilderExtensions.UseReplacingMergeTree),
            [typeof(EntityTypeBuilder), typeof(string[])])!;

    private static readonly MethodInfo UseReplacingMergeTreeWithVersionMethodInfo
        = typeof(ClickHouseEntityTypeBuilderExtensions).GetRuntimeMethod(
            nameof(ClickHouseEntityTypeBuilderExtensions.UseReplacingMergeTree),
            [typeof(EntityTypeBuilder), typeof(string), typeof(string[])])!;

    private static readonly MethodInfo HasPartitionByMethodInfo
        = typeof(ClickHouseEntityTypeBuilderExtensions).GetRuntimeMethod(
            nameof(ClickHouseEntityTypeBuilderExtensions.HasPartitionBy),
            [typeof(EntityTypeBuilder), typeof(string)])!;

    private static readonly MethodInfo HasSampleByMethodInfo
        = typeof(ClickHouseEntityTypeBuilderExtensions).GetRuntimeMethod(
            nameof(ClickHouseEntityTypeBuilderExtensions.HasSampleBy),
            [typeof(EntityTypeBuilder), typeof(string)])!;

    private static readonly MethodInfo HasTtlMethodInfo
        = typeof(ClickHouseEntityTypeBuilderExtensions).GetRuntimeMethod(
            nameof(ClickHouseEntityTypeBuilderExtensions.HasTtl),
            [typeof(EntityTypeBuilder), typeof(string)])!;

    public ClickHouseAnnotationCodeGenerator(AnnotationCodeGeneratorDependencies dependencies)
        : base(dependencies)
    {
    }

    /// <summary>
    /// Generates code for entity type annotations.
    /// </summary>
    protected override bool IsHandledByConvention(IEntityType entityType, IAnnotation annotation)
    {
        // Check if this is a ClickHouse annotation we need to generate code for
        if (annotation.Name.StartsWith(ClickHouseAnnotationNames.Prefix, StringComparison.Ordinal))
        {
            return false; // We need to generate code for this
        }

        return base.IsHandledByConvention(entityType, annotation);
    }

    /// <summary>
    /// Generates fluent API calls for entity type annotations.
    /// </summary>
    public override IReadOnlyList<MethodCallCodeFragment> GenerateFluentApiCalls(
        IEntityType entityType,
        IDictionary<string, IAnnotation> annotations)
    {
        var calls = new List<MethodCallCodeFragment>(base.GenerateFluentApiCalls(entityType, annotations));

        // Handle MergeTree Engine
        if (annotations.TryGetValue(ClickHouseAnnotationNames.Engine, out var engineAnnotation)
            && engineAnnotation.Value is string engine)
        {
            if (annotations.TryGetValue(ClickHouseAnnotationNames.OrderBy, out var orderByAnnotation)
                && orderByAnnotation.Value is string[] orderBy)
            {
                if (engine == "MergeTree")
                {
                    // Use MethodInfo to ensure proper using statement is generated
                    calls.Add(new MethodCallCodeFragment(UseMergeTreeMethodInfo, orderBy.Cast<object>().ToArray()));
                    annotations.Remove(ClickHouseAnnotationNames.Engine);
                    annotations.Remove(ClickHouseAnnotationNames.OrderBy);
                }
                else if (engine == "ReplacingMergeTree")
                {
                    if (annotations.TryGetValue(ClickHouseAnnotationNames.VersionColumn, out var versionAnnotation)
                        && versionAnnotation.Value is string versionColumn)
                    {
                        // ReplacingMergeTree with version column
                        calls.Add(new MethodCallCodeFragment(UseReplacingMergeTreeWithVersionMethodInfo, versionColumn, orderBy));
                        annotations.Remove(ClickHouseAnnotationNames.VersionColumn);
                    }
                    else
                    {
                        // ReplacingMergeTree without version column
                        calls.Add(new MethodCallCodeFragment(UseReplacingMergeTreeMethodInfo, orderBy.Cast<object>().ToArray()));
                    }
                    annotations.Remove(ClickHouseAnnotationNames.Engine);
                    annotations.Remove(ClickHouseAnnotationNames.OrderBy);
                }
            }
        }

        // Handle PartitionBy
        if (annotations.TryGetValue(ClickHouseAnnotationNames.PartitionBy, out var partitionByAnnotation)
            && partitionByAnnotation.Value is string partitionBy)
        {
            calls.Add(new MethodCallCodeFragment(HasPartitionByMethodInfo, partitionBy));
            annotations.Remove(ClickHouseAnnotationNames.PartitionBy);
        }

        // Handle SampleBy
        if (annotations.TryGetValue(ClickHouseAnnotationNames.SampleBy, out var sampleByAnnotation)
            && sampleByAnnotation.Value is string sampleBy)
        {
            calls.Add(new MethodCallCodeFragment(HasSampleByMethodInfo, sampleBy));
            annotations.Remove(ClickHouseAnnotationNames.SampleBy);
        }

        // Handle TTL
        if (annotations.TryGetValue(ClickHouseAnnotationNames.Ttl, out var ttlAnnotation)
            && ttlAnnotation.Value is string ttl)
        {
            calls.Add(new MethodCallCodeFragment(HasTtlMethodInfo, ttl));
            annotations.Remove(ClickHouseAnnotationNames.Ttl);
        }

        return calls;
    }

    /// <summary>
    /// Generates data annotation attributes for entity type annotations.
    /// </summary>
    public override IReadOnlyList<AttributeCodeFragment> GenerateDataAnnotationAttributes(
        IEntityType entityType,
        IDictionary<string, IAnnotation> annotations)
    {
        var attributes = new List<AttributeCodeFragment>(
            base.GenerateDataAnnotationAttributes(entityType, annotations));

        // ClickHouse-specific attributes are typically handled via fluent API
        // since there aren't standard data annotations for MergeTree engines

        return attributes;
    }
}
