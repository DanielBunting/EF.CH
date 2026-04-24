using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Query.Internal.Expressions;

/// <summary>
/// Carries a ClickHouse aggregate-state merge call through EF Core's query
/// pipeline until it reaches the aggregate method translator.
///
/// <para>
/// When user code writes <c>g.CountMerge(s =&gt; s.State)</c> on a grouping,
/// <see cref="ClickHouseQueryTranslationPreprocessor"/> rewrites it into
/// <c>g.Sum/Count/…(s =&gt; Sentinel(s.State, "countMerge"))</c> so it passes
/// EF's navigation expander whitelist. The sentinel method gets translated
/// into this <see cref="ClickHouseMergeSentinelExpression"/>, which the
/// aggregate translator detects in the selector and unwraps into the real
/// <c>xxxMerge(state)</c> SQL — bypassing the outer <c>sum()</c>/<c>count()</c>
/// that was only there to satisfy EF's type checking.
/// </para>
/// </summary>
public sealed class ClickHouseMergeSentinelExpression : SqlExpression
{
    public SqlExpression StateColumn { get; }
    public string FunctionName { get; }
    public double? MergeParameter { get; }
    public IReadOnlyList<double>? Parameters { get; }
    /// <summary>Second positional argument — used by <c>argMax(arg, val)</c>, <c>topKWeighted(col, weight)</c>, etc.</summary>
    public SqlExpression? SecondArg { get; }

    public ClickHouseMergeSentinelExpression(
        SqlExpression stateColumn,
        string functionName,
        Type type,
        RelationalTypeMapping? typeMapping,
        double? parameter = null,
        IReadOnlyList<double>? parameters = null,
        SqlExpression? secondArg = null)
        : base(type, typeMapping)
    {
        StateColumn = stateColumn ?? throw new ArgumentNullException(nameof(stateColumn));
        FunctionName = functionName ?? throw new ArgumentNullException(nameof(functionName));
        MergeParameter = parameter;
        Parameters = parameters;
        SecondArg = secondArg;
    }

    public override Expression Quote()
        => throw new InvalidOperationException(
            "ClickHouseMergeSentinelExpression is a pipeline marker and should not be quoted.");

    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        var visited = (SqlExpression)visitor.Visit(StateColumn);
        var visitedSecond = SecondArg is null ? null : (SqlExpression)visitor.Visit(SecondArg);
        return visited == StateColumn && visitedSecond == SecondArg
            ? this
            : new ClickHouseMergeSentinelExpression(visited, FunctionName, Type, TypeMapping, MergeParameter, Parameters, visitedSecond);
    }

    protected override void Print(ExpressionPrinter printer)
    {
        printer.Append($"<merge-sentinel {FunctionName}(");
        printer.Visit(StateColumn);
        printer.Append(")>");
    }
}
