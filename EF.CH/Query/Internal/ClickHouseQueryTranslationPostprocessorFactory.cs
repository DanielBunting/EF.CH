using EF.CH.External;
using Microsoft.EntityFrameworkCore.Query;

namespace EF.CH.Query.Internal;

/// <summary>
/// Factory for creating ClickHouse query translation postprocessors.
/// </summary>
public class ClickHouseQueryTranslationPostprocessorFactory : IQueryTranslationPostprocessorFactory
{
    private readonly QueryTranslationPostprocessorDependencies _dependencies;
    private readonly RelationalQueryTranslationPostprocessorDependencies _relationalDependencies;
    private readonly IExternalConfigResolver _externalConfigResolver;

    public ClickHouseQueryTranslationPostprocessorFactory(
        QueryTranslationPostprocessorDependencies dependencies,
        RelationalQueryTranslationPostprocessorDependencies relationalDependencies,
        IExternalConfigResolver externalConfigResolver)
    {
        _dependencies = dependencies;
        _relationalDependencies = relationalDependencies;
        _externalConfigResolver = externalConfigResolver;
    }

    public virtual QueryTranslationPostprocessor Create(QueryCompilationContext queryCompilationContext)
        => new ClickHouseQueryTranslationPostprocessor(
            _dependencies,
            _relationalDependencies,
            (RelationalQueryCompilationContext)queryCompilationContext,
            _externalConfigResolver);
}
