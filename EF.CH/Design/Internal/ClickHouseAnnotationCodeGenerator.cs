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
    #region MethodInfo References - Entity Level

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

    private static readonly MethodInfo UseSummingMergeTreeMethodInfo
        = typeof(ClickHouseEntityTypeBuilderExtensions).GetRuntimeMethod(
            nameof(ClickHouseEntityTypeBuilderExtensions.UseSummingMergeTree),
            [typeof(EntityTypeBuilder), typeof(string[])])!;

    private static readonly MethodInfo UseAggregatingMergeTreeMethodInfo
        = typeof(ClickHouseEntityTypeBuilderExtensions).GetRuntimeMethod(
            nameof(ClickHouseEntityTypeBuilderExtensions.UseAggregatingMergeTree),
            [typeof(EntityTypeBuilder), typeof(string[])])!;

    private static readonly MethodInfo UseCollapsingMergeTreeMethodInfo
        = typeof(ClickHouseEntityTypeBuilderExtensions).GetRuntimeMethod(
            nameof(ClickHouseEntityTypeBuilderExtensions.UseCollapsingMergeTree),
            [typeof(EntityTypeBuilder), typeof(string), typeof(string[])])!;

    private static readonly MethodInfo UseVersionedCollapsingMergeTreeMethodInfo
        = typeof(ClickHouseEntityTypeBuilderExtensions).GetRuntimeMethod(
            nameof(ClickHouseEntityTypeBuilderExtensions.UseVersionedCollapsingMergeTree),
            [typeof(EntityTypeBuilder), typeof(string), typeof(string), typeof(string[])])!;

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

    #endregion

    #region MethodInfo References - Property Level

    private static readonly MethodInfo HasCodecMethodInfo
        = typeof(ClickHousePropertyBuilderCodecExtensions).GetMethods()
            .First(m => m.Name == nameof(ClickHousePropertyBuilderCodecExtensions.HasCodec)
                     && m.GetParameters().Length == 2
                     && m.GetParameters()[1].ParameterType == typeof(string));

    #endregion

    #region MethodInfo References - Index Level

    private static readonly MethodInfo HasGranularityMethodInfo
        = typeof(ClickHouseIndexBuilderExtensions).GetRuntimeMethod(
            nameof(ClickHouseIndexBuilderExtensions.HasGranularity),
            [typeof(IndexBuilder), typeof(int)])!;

    private static readonly MethodInfo UseMinmaxMethodInfo
        = typeof(ClickHouseIndexBuilderExtensions).GetRuntimeMethod(
            nameof(ClickHouseIndexBuilderExtensions.UseMinmax),
            [typeof(IndexBuilder)])!;

    private static readonly MethodInfo UseBloomFilterMethodInfo
        = typeof(ClickHouseIndexBuilderExtensions).GetRuntimeMethod(
            nameof(ClickHouseIndexBuilderExtensions.UseBloomFilter),
            [typeof(IndexBuilder), typeof(double)])!;

    private static readonly MethodInfo UseTokenBFMethodInfo
        = typeof(ClickHouseIndexBuilderExtensions).GetRuntimeMethod(
            nameof(ClickHouseIndexBuilderExtensions.UseTokenBF),
            [typeof(IndexBuilder), typeof(int), typeof(int), typeof(int)])!;

    private static readonly MethodInfo UseNgramBFMethodInfo
        = typeof(ClickHouseIndexBuilderExtensions).GetRuntimeMethod(
            nameof(ClickHouseIndexBuilderExtensions.UseNgramBF),
            [typeof(IndexBuilder), typeof(int), typeof(int), typeof(int), typeof(int)])!;

    private static readonly MethodInfo UseSetMethodInfo
        = typeof(ClickHouseIndexBuilderExtensions).GetRuntimeMethod(
            nameof(ClickHouseIndexBuilderExtensions.UseSet),
            [typeof(IndexBuilder), typeof(int)])!;

    #endregion

    /// <summary>
    /// Engines supported by this code generator.
    /// </summary>
    private static readonly HashSet<string> SupportedEngines =
    [
        "MergeTree",
        "ReplacingMergeTree",
        "SummingMergeTree",
        "AggregatingMergeTree",
        "CollapsingMergeTree",
        "VersionedCollapsingMergeTree"
    ];

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

        // Handle MergeTree Engine family
        if (annotations.TryGetValue(ClickHouseAnnotationNames.Engine, out var engineAnnotation)
            && engineAnnotation.Value is string engine)
        {
            if (!SupportedEngines.Contains(engine))
            {
                throw new NotSupportedException(
                    $"ClickHouse engine '{engine}' is not supported by the annotation code generator. " +
                    $"Supported engines: {string.Join(", ", SupportedEngines)}.");
            }

            if (annotations.TryGetValue(ClickHouseAnnotationNames.OrderBy, out var orderByAnnotation)
                && orderByAnnotation.Value is string[] orderBy)
            {
                var signColumn = annotations.TryGetValue(ClickHouseAnnotationNames.SignColumn, out var signAnnotation)
                    ? signAnnotation.Value as string
                    : null;

                var versionColumn = annotations.TryGetValue(ClickHouseAnnotationNames.VersionColumn, out var versionAnnotation)
                    ? versionAnnotation.Value as string
                    : null;

                switch (engine)
                {
                    case "MergeTree":
                        calls.Add(new MethodCallCodeFragment(UseMergeTreeMethodInfo, orderBy.Cast<object>().ToArray()));
                        break;

                    case "ReplacingMergeTree":
                        if (versionColumn is not null)
                        {
                            calls.Add(new MethodCallCodeFragment(UseReplacingMergeTreeWithVersionMethodInfo, versionColumn, orderBy));
                            annotations.Remove(ClickHouseAnnotationNames.VersionColumn);
                        }
                        else
                        {
                            calls.Add(new MethodCallCodeFragment(UseReplacingMergeTreeMethodInfo, orderBy.Cast<object>().ToArray()));
                        }
                        break;

                    case "SummingMergeTree":
                        calls.Add(new MethodCallCodeFragment(UseSummingMergeTreeMethodInfo, orderBy.Cast<object>().ToArray()));
                        break;

                    case "AggregatingMergeTree":
                        calls.Add(new MethodCallCodeFragment(UseAggregatingMergeTreeMethodInfo, orderBy.Cast<object>().ToArray()));
                        break;

                    case "CollapsingMergeTree":
                        if (signColumn is null)
                        {
                            throw new InvalidOperationException(
                                $"CollapsingMergeTree engine requires a sign column annotation ({ClickHouseAnnotationNames.SignColumn}).");
                        }
                        calls.Add(new MethodCallCodeFragment(UseCollapsingMergeTreeMethodInfo, signColumn, orderBy));
                        annotations.Remove(ClickHouseAnnotationNames.SignColumn);
                        break;

                    case "VersionedCollapsingMergeTree":
                        if (signColumn is null)
                        {
                            throw new InvalidOperationException(
                                $"VersionedCollapsingMergeTree engine requires a sign column annotation ({ClickHouseAnnotationNames.SignColumn}).");
                        }
                        if (versionColumn is null)
                        {
                            throw new InvalidOperationException(
                                $"VersionedCollapsingMergeTree engine requires a version column annotation ({ClickHouseAnnotationNames.VersionColumn}).");
                        }
                        calls.Add(new MethodCallCodeFragment(UseVersionedCollapsingMergeTreeMethodInfo, signColumn, versionColumn, orderBy));
                        annotations.Remove(ClickHouseAnnotationNames.SignColumn);
                        annotations.Remove(ClickHouseAnnotationNames.VersionColumn);
                        break;
                }

                annotations.Remove(ClickHouseAnnotationNames.Engine);
                annotations.Remove(ClickHouseAnnotationNames.OrderBy);
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

    #region Property-Level Annotations

    /// <summary>
    /// Checks if a property annotation is handled by convention.
    /// </summary>
    protected override bool IsHandledByConvention(IProperty property, IAnnotation annotation)
    {
        // Check if this is a ClickHouse annotation we need to generate code for
        if (annotation.Name.StartsWith(ClickHouseAnnotationNames.Prefix, StringComparison.Ordinal))
        {
            return false; // We need to generate code for this
        }

        return base.IsHandledByConvention(property, annotation);
    }

    /// <summary>
    /// Generates fluent API calls for property annotations.
    /// </summary>
    public override IReadOnlyList<MethodCallCodeFragment> GenerateFluentApiCalls(
        IProperty property,
        IDictionary<string, IAnnotation> annotations)
    {
        var calls = new List<MethodCallCodeFragment>(base.GenerateFluentApiCalls(property, annotations));

        // Handle CompressionCodec
        if (annotations.TryGetValue(ClickHouseAnnotationNames.CompressionCodec, out var codecAnnotation)
            && codecAnnotation.Value is string codecSpec)
        {
            calls.Add(new MethodCallCodeFragment(HasCodecMethodInfo, codecSpec));
            annotations.Remove(ClickHouseAnnotationNames.CompressionCodec);
        }

        return calls;
    }

    #endregion

    #region Index-Level Annotations

    /// <summary>
    /// Checks if an index annotation is handled by convention.
    /// </summary>
    protected override bool IsHandledByConvention(IIndex index, IAnnotation annotation)
    {
        // Check if this is a ClickHouse annotation we need to generate code for
        if (annotation.Name.StartsWith(ClickHouseAnnotationNames.Prefix, StringComparison.Ordinal))
        {
            return false; // We need to generate code for this
        }

        return base.IsHandledByConvention(index, annotation);
    }

    /// <summary>
    /// Generates fluent API calls for index annotations.
    /// </summary>
    public override IReadOnlyList<MethodCallCodeFragment> GenerateFluentApiCalls(
        IIndex index,
        IDictionary<string, IAnnotation> annotations)
    {
        var calls = new List<MethodCallCodeFragment>(base.GenerateFluentApiCalls(index, annotations));

        // Handle SkipIndexType
        if (annotations.TryGetValue(ClickHouseAnnotationNames.SkipIndexType, out var typeAnnotation)
            && typeAnnotation.Value is SkipIndexType indexType)
        {
            var indexParams = annotations.TryGetValue(ClickHouseAnnotationNames.SkipIndexParams, out var paramsAnnotation)
                ? paramsAnnotation.Value as SkipIndexParams
                : null;

            switch (indexType)
            {
                case SkipIndexType.Minmax:
                    calls.Add(new MethodCallCodeFragment(UseMinmaxMethodInfo));
                    break;

                case SkipIndexType.BloomFilter:
                    var falsePositive = indexParams?.BloomFilterFalsePositive ?? 0.025;
                    calls.Add(new MethodCallCodeFragment(UseBloomFilterMethodInfo, falsePositive));
                    break;

                case SkipIndexType.TokenBF:
                    var tokenSize = indexParams?.TokenBFSize ?? 10240;
                    var tokenHashes = indexParams?.TokenBFHashes ?? 3;
                    var tokenSeed = indexParams?.TokenBFSeed ?? 0;
                    calls.Add(new MethodCallCodeFragment(UseTokenBFMethodInfo, tokenSize, tokenHashes, tokenSeed));
                    break;

                case SkipIndexType.NgramBF:
                    var ngramSize = indexParams?.NgramSize ?? 4;
                    var ngramBFSize = indexParams?.NgramBFSize ?? 10240;
                    var ngramHashes = indexParams?.NgramBFHashes ?? 3;
                    var ngramSeed = indexParams?.NgramBFSeed ?? 0;
                    calls.Add(new MethodCallCodeFragment(UseNgramBFMethodInfo, ngramSize, ngramBFSize, ngramHashes, ngramSeed));
                    break;

                case SkipIndexType.Set:
                    var maxRows = indexParams?.SetMaxRows ?? 100;
                    calls.Add(new MethodCallCodeFragment(UseSetMethodInfo, maxRows));
                    break;
            }

            annotations.Remove(ClickHouseAnnotationNames.SkipIndexType);
            annotations.Remove(ClickHouseAnnotationNames.SkipIndexParams);
        }

        // Handle SkipIndexGranularity
        if (annotations.TryGetValue(ClickHouseAnnotationNames.SkipIndexGranularity, out var granularityAnnotation)
            && granularityAnnotation.Value is int granularity)
        {
            calls.Add(new MethodCallCodeFragment(HasGranularityMethodInfo, granularity));
            annotations.Remove(ClickHouseAnnotationNames.SkipIndexGranularity);
        }

        return calls;
    }

    #endregion
}
