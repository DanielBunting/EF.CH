using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EF.CH.Query.Internal;

/// <summary>
/// Translates LINQ expressions into ClickHouse SQL for materialized view definitions.
/// This is a design-time translator that processes expressions during OnModelCreating.
/// </summary>
internal class MaterializedViewSqlTranslator
{
    private readonly IModel _model;
    private readonly string _sourceTableName;

    /// <summary>
    /// Creates a new translator for the given model and source table.
    /// </summary>
    public MaterializedViewSqlTranslator(IModel model, string sourceTableName)
    {
        _model = model;
        _sourceTableName = sourceTableName;
    }

    /// <summary>
    /// Translates a single-source LINQ query expression into a ClickHouse SELECT statement.
    /// </summary>
    public string Translate<T1, TResult>(
        Expression<Func<IQueryable<T1>, IQueryable<TResult>>> queryExpression)
        where T1 : class
        where TResult : class
        => TranslateLambda(queryExpression);

    /// <summary>
    /// Translates a two-source LINQ query expression. T1 is the INSERT trigger.
    /// </summary>
    public string Translate<T1, T2, TResult>(
        Expression<Func<IQueryable<T1>, IQueryable<T2>, IQueryable<TResult>>> queryExpression)
        where T1 : class where T2 : class where TResult : class
        => TranslateLambda(queryExpression);

    /// <summary>
    /// Translates a three-source LINQ query expression. T1 is the INSERT trigger.
    /// </summary>
    public string Translate<T1, T2, T3, TResult>(
        Expression<Func<IQueryable<T1>, IQueryable<T2>, IQueryable<T3>, IQueryable<TResult>>> queryExpression)
        where T1 : class where T2 : class where T3 : class where TResult : class
        => TranslateLambda(queryExpression);

    /// <summary>
    /// Translates a four-source LINQ query expression. T1 is the INSERT trigger.
    /// </summary>
    public string Translate<T1, T2, T3, T4, TResult>(
        Expression<Func<IQueryable<T1>, IQueryable<T2>, IQueryable<T3>, IQueryable<T4>, IQueryable<TResult>>> queryExpression)
        where T1 : class where T2 : class where T3 : class where T4 : class where TResult : class
        => TranslateLambda(queryExpression);

    /// <summary>
    /// Translates a five-source LINQ query expression. T1 is the INSERT trigger.
    /// </summary>
    public string Translate<T1, T2, T3, T4, T5, TResult>(
        Expression<Func<IQueryable<T1>, IQueryable<T2>, IQueryable<T3>, IQueryable<T4>, IQueryable<T5>, IQueryable<TResult>>> queryExpression)
        where T1 : class where T2 : class where T3 : class where T4 : class where T5 : class where TResult : class
        => TranslateLambda(queryExpression);

    /// <summary>
    /// Shared translation entry point. Reflects over the first lambda parameter
    /// (the trigger <c>IQueryable&lt;T1&gt;</c>) to construct the typed visitor,
    /// then visits the lambda body directly.
    /// </summary>
    private string TranslateLambda(LambdaExpression queryExpression)
    {
        // T1 (the first generic argument) is the trigger table.
        var triggerType = queryExpression.Parameters[0].Type.GetGenericArguments()[0];
        var visitorType = typeof(MaterializedViewExpressionVisitor<>).MakeGenericType(triggerType);
        var visitor = (IMaterializedViewVisitor)Activator.CreateInstance(
            visitorType, _model, _sourceTableName)!;
        return visitor.TranslateBody(queryExpression.Body);
    }
}

/// <summary>
/// Internal interface used by <see cref="MaterializedViewSqlTranslator.TranslateLambda"/>
/// to invoke the typed visitor without per-arity duplication.
/// </summary>
internal interface IMaterializedViewVisitor
{
    string TranslateBody(Expression body);
}

/// <summary>
/// Visits LINQ expression trees and builds ClickHouse SQL.
/// </summary>
/// <typeparam name="TSource">The source entity type.</typeparam>
internal class MaterializedViewExpressionVisitor<TSource> : ExpressionVisitor, IMaterializedViewVisitor
    where TSource : class
{
    private readonly IModel _model;
    private readonly string _sourceTableName;
    private readonly IEntityType? _sourceEntityType;
    private readonly StringBuilder _selectSql = new();
    private readonly List<string> _selectColumns = [];
    private readonly List<string> _groupByColumns = [];
    private string? _groupByParameter;
    private readonly Dictionary<string, string> _groupKeyMappings = new();
    private readonly List<string> _whereClauses = [];

    // ----- Multi-source (Join) state -----
    // Populated only when the LINQ chain contains a Join/GroupJoin. When empty,
    // emission is byte-for-byte identical to the legacy single-source path.
    private const string SourceAlias = "t0";
    private readonly List<JoinSource> _joins = new();
    private int _nextJoinAliasIndex = 1;
    // Per-parameter binding overrides — used by Join's two-parameter result selector
    // to bind the "outer" and "inner" parameters to different aliases simultaneously.
    private readonly Dictionary<string, RowBinding> _paramOverrides = new();
    // The "current row" — what an unscoped ParameterExpression resolves to when
    // walking a single-parameter lambda body (Where/GroupBy/Select/inner aggregates).
    private RowBinding _currentRowBinding;
    // Set by VisitJoin/VisitGroupJoin from the join's result selector. The next
    // operator's lambda parameter (e.g. x in .GroupBy(x => x.Region)) inherits it
    // because that x represents the compound row produced by the join.
    private Dictionary<string, MemberRef>? _pendingTransparentIdentifier;
    // Set by VisitGroupJoin when its result selector is a row-passing transparent
    // identifier (`(o, cs) => new { o, cs }`). The next operator (typically
    // SelectMany with DefaultIfEmpty) consumes it. Each map entry binds a member
    // name to a *whole row* binding (outer or inner side of the GroupJoin), not a
    // single column. Distinct from `_pendingTransparentIdentifier` which always
    // carries column-resolved member refs.
    private Dictionary<string, RowBinding>? _pendingTransparentIdentifierRows;
    private bool HasJoins => _joins.Count > 0 || _arrayJoins.Count > 0;

    private sealed record JoinSource(string TableExpr, string Alias, string Kind, string OnPredicate);
    /// <summary>
    /// One ARRAY JOIN clause in the FROM block. <c>ColumnExpr</c> is the SQL
    /// reference to the array-typed column (e.g. <c>"Tags"</c> or <c>t0."Tags"</c>);
    /// <c>Alias</c> is the per-element binding name; <c>IsLeft</c> picks
    /// <c>LEFT ARRAY JOIN</c> over plain <c>ARRAY JOIN</c>.
    /// </summary>
    private sealed record ArrayJoinClause(string ColumnExpr, string Alias, bool IsLeft);
    private readonly List<ArrayJoinClause> _arrayJoins = new();
    private sealed record MemberRef(string Alias, string Column, IEntityType? Entity);
    private sealed class RowBinding
    {
        // A single-source row: alias + entity type for column-name lookup.
        public string? Alias { get; init; }
        public IEntityType? Entity { get; init; }
        // A compound row produced by Join's result selector: each member maps to
        // a (alias, column, entity) triple from one of the joined sources.
        public Dictionary<string, MemberRef>? Transparent { get; init; }
    }

    public MaterializedViewExpressionVisitor(IModel model, string sourceTableName)
    {
        _model = model;
        _sourceTableName = sourceTableName;
        _sourceEntityType = model.FindEntityType(typeof(TSource));
        // Initial row binding — the source table. Alias is only emitted into SQL
        // when joins are present (HasJoins gate keeps single-source SQL untouched).
        _currentRowBinding = new RowBinding { Alias = SourceAlias, Entity = _sourceEntityType };
    }

    /// <summary>
    /// Translates the full query expression to SQL.
    /// </summary>
    public string Translate<TResult>(
        Expression<Func<IQueryable<TSource>, IQueryable<TResult>>> queryExpression)
        => TranslateBody(queryExpression.Body);

    /// <summary>
    /// Translates the body of a (possibly multi-arg) LINQ query lambda. T1 (the
    /// first lambda parameter) was used to construct this typed visitor, so any
    /// reference to the trigger source resolves to the correct alias/entity.
    /// </summary>
    public string TranslateBody(Expression body)
    {
        // The expression body should be a method chain on the IQueryable parameter
        Visit(body);

        var sql = new StringBuilder();
        sql.Append("SELECT ");
        sql.Append(string.Join(", ", _selectColumns));
        sql.AppendLine();
        sql.Append("FROM ");
        sql.Append(QuoteIdentifier(_sourceTableName));
        if (HasJoins || _arrayJoins.Count > 0)
        {
            sql.Append(" AS ").Append(SourceAlias);
            // ARRAY JOIN clauses come immediately after the FROM table and before
            // any regular joins — that's the position ClickHouse expects.
            foreach (var aj in _arrayJoins)
            {
                sql.AppendLine();
                sql.Append(aj.IsLeft ? "LEFT ARRAY JOIN " : "ARRAY JOIN ");
                sql.Append(aj.ColumnExpr).Append(" AS ").Append(QuoteIdentifier(aj.Alias));
            }
            foreach (var join in _joins)
            {
                sql.AppendLine();
                sql.Append($"{join.Kind} JOIN {join.TableExpr} AS {join.Alias}");
                if (!string.IsNullOrEmpty(join.OnPredicate))
                    sql.Append(" ON ").Append(join.OnPredicate);
            }
        }

        if (_whereClauses.Count > 0)
        {
            sql.AppendLine();
            sql.Append("WHERE ");
            sql.Append(string.Join(" AND ", _whereClauses));
        }

        if (_groupByColumns.Count > 0)
        {
            sql.AppendLine();
            sql.Append("GROUP BY ");
            sql.Append(string.Join(", ", _groupByColumns));
        }

        return sql.ToString();
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // Standard LINQ methods (Queryable / Enumerable).
        if (node.Method.DeclaringType == typeof(Queryable) ||
            node.Method.DeclaringType == typeof(Enumerable))
        {
            switch (node.Method.Name)
            {
                case "GroupBy":
                    return VisitGroupBy(node);
                case "Select":
                    return VisitSelect(node);
                case "Where":
                    return VisitWhere(node);
                case "Join":
                    return VisitJoin(node, kind: "INNER");
                case "GroupJoin":
                    return VisitGroupJoin(node);
                case "SelectMany":
                    return VisitSelectMany(node);
                default:
                    // Fail loudly instead of silently descending — silent drops of
                    // unsupported operators (OrderBy, Take, etc.) would emit SQL
                    // that compiles but produces the wrong view contents.
                    throw new NotSupportedException(
                        $"LINQ operator '{node.Method.Name}' is not supported in materialized view definitions. " +
                        "Supported: Where, GroupBy, Select, Join, GroupJoin, SelectMany.");
            }
        }

        // ClickHouse-specific extension methods on IQueryable<T>. The runtime
        // pipeline rewrites these in the preprocessor; the design-time MV
        // translator must recognise them directly because it doesn't go through
        // that preprocessor.
        if (node.Method.DeclaringType?.FullName == "EF.CH.Extensions.ClickHouseQueryableExtensions")
        {
            switch (node.Method.Name)
            {
                // ASOF strictness — ON eq AND outer.col <op> inner.col
                case "AsofJoin":
                    return VisitAsofJoin(node, kind: "ASOF INNER");
                case "AsofLeftJoin":
                    return VisitAsofJoin(node, kind: "ASOF LEFT");
                // ANY strictness — one match per left row.
                case "AnyJoin":
                    return VisitJoin(node, kind: "ANY INNER");
                case "AnyLeftJoin":
                    return VisitJoin(node, kind: "ANY LEFT");
                case "AnyRightJoin":
                    return VisitJoin(node, kind: "ANY RIGHT");
                // Standard RIGHT and FULL OUTER.
                case "RightJoin":
                    return VisitJoin(node, kind: "RIGHT");
                case "FullOuterJoin":
                    return VisitJoin(node, kind: "FULL OUTER");
                // SEMI / ANTI — preserves only one side; result selector is single-arg.
                case "LeftSemiJoin":
                    return VisitSingleSideJoin(node, kind: "LEFT SEMI", outerSidePreserved: true);
                case "LeftAntiJoin":
                    return VisitSingleSideJoin(node, kind: "LEFT ANTI", outerSidePreserved: true);
                case "RightSemiJoin":
                    return VisitSingleSideJoin(node, kind: "RIGHT SEMI", outerSidePreserved: false);
                case "RightAntiJoin":
                    return VisitSingleSideJoin(node, kind: "RIGHT ANTI", outerSidePreserved: false);
                // CROSS — no ON predicate.
                case "CrossJoin":
                    return VisitCrossJoin(node);
                // ARRAY JOIN — flattens an array column inline.
                case "ArrayJoin":
                    return VisitArrayJoin(node, isLeft: false);
                case "LeftArrayJoin":
                    return VisitArrayJoin(node, isLeft: true);
            }
        }

        return base.VisitMethodCall(node);
    }

    private Expression VisitWhere(MethodCallExpression node)
    {
        Visit(node.Arguments[0]);

        LambdaExpression? predicate = node.Arguments[1] switch
        {
            UnaryExpression { Operand: LambdaExpression lambda } => lambda,
            LambdaExpression lambda => lambda,
            _ => null,
        };

        if (predicate != null)
        {
            EnterLambda(predicate);
            _whereClauses.Add(TranslateExpression(predicate.Body));
        }

        return node;
    }

    private Expression VisitGroupBy(MethodCallExpression node)
    {
        // Visit the source first (IQueryable<TSource>)
        Visit(node.Arguments[0]);

        // Get the key selector lambda
        if (node.Arguments[1] is UnaryExpression { Operand: LambdaExpression keySelector })
        {
            EnterLambda(keySelector);
            VisitGroupByKeySelector(keySelector);
        }

        return node;
    }

    /// <summary>
    /// Translates a LINQ <c>Join(outer, inner, outerKey, innerKey, resultSelector)</c>
    /// into a SQL JOIN clause and stages a transparent identifier so the next
    /// operator's lambda parameter resolves member access through it.
    /// Single-source MVs (no Join in the chain) are unaffected — alias emission
    /// is gated on <c>HasJoins</c> in <see cref="TranslateMemberAccess"/> and the
    /// <c>FROM</c> assembly in <see cref="Translate{TResult}"/>.
    /// </summary>
    private Expression VisitJoin(MethodCallExpression node, string kind)
    {
        Visit(node.Arguments[0]); // recurse into LHS — sets _sourceTableName, etc.

        var outerKey = ExtractLambda(node.Arguments[2]);
        var innerKey = ExtractLambda(node.Arguments[3]);
        var resultSel = ExtractLambda(node.Arguments[4]);

        var (innerEntityType, innerTableExpr) = ResolveInnerSource(node.Arguments[1]);
        var innerAlias = $"t{_nextJoinAliasIndex++}";

        // Bind both key selector lambdas to their respective sources, then translate
        // the bodies into SQL. Composite keys appear as NewExpression on both sides.
        // For chained joins (J1.Join(J2, ...)), the outer side has a transparent
        // identifier staged by the prior join — the outer-key lambda references
        // members of the compound row, not the raw source.
        var outerBinding = _pendingTransparentIdentifier is not null
            ? new RowBinding { Transparent = _pendingTransparentIdentifier }
            : new RowBinding { Alias = SourceAlias, Entity = _sourceEntityType };
        var innerBinding = new RowBinding { Alias = innerAlias, Entity = innerEntityType };

        var onPredicate = BuildJoinOnPredicate(outerKey, outerBinding, innerKey, innerBinding);

        _joins.Add(new JoinSource(innerTableExpr, innerAlias, kind, onPredicate));

        // Build a transparent identifier from the result selector so that the next
        // operator's lambda parameter (representing the post-join compound row)
        // can resolve member access through it.
        _pendingTransparentIdentifier = BuildTransparentIdentifier(resultSel, outerBinding, innerBinding);

        return node;
    }

    /// <summary>
    /// Translates LINQ <c>GroupJoin(outer, inner, outerKey, innerKey, resultSelector)</c>
    /// into a SQL <c>LEFT JOIN</c>. ClickHouse has no native group-join, so the
    /// result selector's typical <c>cs.Select(c =&gt; expr).FirstOrDefault()</c> /
    /// <c>?? default</c> idioms are recognised and rewritten to plain
    /// <c>coalesce(t1."col", default)</c> column references.
    /// </summary>
    private Expression VisitGroupJoin(MethodCallExpression node)
    {
        Visit(node.Arguments[0]);

        var outerKey = ExtractLambda(node.Arguments[2]);
        var innerKey = ExtractLambda(node.Arguments[3]);
        var resultSel = ExtractLambda(node.Arguments[4]);

        var (innerEntityType, innerTableExpr) = ResolveInnerSource(node.Arguments[1]);
        var innerAlias = $"t{_nextJoinAliasIndex++}";

        var outerBinding = new RowBinding { Alias = SourceAlias, Entity = _sourceEntityType };
        var innerBinding = new RowBinding { Alias = innerAlias, Entity = innerEntityType };

        var onPredicate = BuildJoinOnPredicate(outerKey, outerBinding, innerKey, innerBinding);

        _joins.Add(new JoinSource(innerTableExpr, innerAlias, "LEFT", onPredicate));

        // Two cases for the result selector:
        //
        // (a) "Transparent identifier" form — body is `new { o, cs }` with bare
        //     parameter args (and one of them is the IEnumerable<TInner> group).
        //     This is the canonical lowering of `from o in outer join c in inner
        //     on … into cs from c in cs.DefaultIfEmpty() select …`. The selector
        //     is *not* a projection; it's plumbing that the next operator
        //     (SelectMany, typically) will consume. Stash a transparent identifier
        //     mapping each member name to the corresponding row binding (outer or
        //     inner) and let SelectMany finish the LEFT JOIN.
        //
        // (b) Terminal projection — the user wrote a one-shot GroupJoin with a
        //     real projection (Phase C surface: `(o, cs) => new TResult { … }`
        //     with `cs.Select(c => c.X).FirstOrDefault()` idioms). Emit columns
        //     directly via the GroupJoin-idiom-aware projection visitor.
        if (IsTransparentIdentifierBody(resultSel))
        {
            _pendingTransparentIdentifierRows = BuildRowTransparentIdentifier(resultSel, outerBinding, innerBinding);
        }
        else
        {
            EmitJoinProjection(resultSel, outerBinding, innerBinding);
        }

        return node;
    }

    /// <summary>
    /// True when a (GroupJoin/Join) result selector body is an anonymous
    /// <c>new { p1, p2, ... }</c> whose every argument is one of the lambda's
    /// parameters (no member access, no method call). This is the LINQ
    /// "transparent identifier" lowering used by query syntax to thread
    /// multi-source rows through chained operators.
    /// </summary>
    private static bool IsTransparentIdentifierBody(LambdaExpression resultSel)
    {
        if (resultSel.Body is not NewExpression newExpr) return false;
        if (newExpr.Members is null) return false;
        var paramSet = new HashSet<ParameterExpression>(resultSel.Parameters);
        foreach (var arg in newExpr.Arguments)
        {
            if (arg is not ParameterExpression pe || !paramSet.Contains(pe))
                return false;
        }
        return true;
    }

    /// <summary>
    /// Build a transparent-identifier map from a "row-passing" result selector
    /// — each member name maps to one of the outer/inner row bindings (whole
    /// rows, not individual columns). Used when GroupJoin's result selector
    /// is the canonical <c>(o, cs) =&gt; new { o, cs }</c> shape that flows
    /// into a chained <c>SelectMany(... .DefaultIfEmpty(), ...)</c>.
    /// </summary>
    private Dictionary<string, RowBinding> BuildRowTransparentIdentifier(
        LambdaExpression resultSel, RowBinding outerBinding, RowBinding innerBinding)
    {
        var rows = new Dictionary<string, RowBinding>(StringComparer.Ordinal);
        var newExpr = (NewExpression)resultSel.Body;
        for (int i = 0; i < newExpr.Arguments.Count; i++)
        {
            var pe = (ParameterExpression)newExpr.Arguments[i];
            var memberName = newExpr.Members![i].Name;
            var binding = pe == resultSel.Parameters[0] ? outerBinding : innerBinding;
            rows[memberName] = binding;
        }
        return rows;
    }

    /// <summary>
    /// Translates LINQ <c>SelectMany(outer, collectionSelector, resultSelector)</c>
    /// — the method form of query-syntax <c>from o in outer from c in inner select …</c>.
    /// Produces an INNER JOIN when the collection-selector body is
    /// <c>inner.Where(c =&gt; o.Key == c.Key)</c>, or a CROSS JOIN when the body
    /// is a bare queryable. The two-arg overload (no result selector) projects
    /// the inner row directly.
    /// </summary>
    /// <summary>
    /// Translates <c>AsofJoin</c> / <c>AsofLeftJoin</c> (6 args: outer, inner,
    /// outerKey, innerKey, asofCondition, resultSelector). Builds the equality
    /// ON predicate via <see cref="BuildJoinOnPredicate"/>, then appends the
    /// inequality predicate parsed from the asofCondition lambda.
    /// Mirrors the runtime SQL shape from
    /// <c>ClickHouseQuerySqlGenerator.VisitAsofJoin</c>:
    /// <c>ASOF [LEFT] JOIN T1 ON eq AND outerCol &lt;op&gt; innerCol</c>.
    /// </summary>
    private Expression VisitAsofJoin(MethodCallExpression node, string kind)
    {
        if (_joins.Any(j => j.Kind.StartsWith("ASOF")))
        {
            throw new NotSupportedException("Only one ASOF JOIN per materialized-view definition is supported (ClickHouse limitation).");
        }

        Visit(node.Arguments[0]); // recurse into LHS

        var outerKey = ExtractLambda(node.Arguments[2]);
        var innerKey = ExtractLambda(node.Arguments[3]);
        var asofLambda = ExtractLambda(node.Arguments[4]);
        var resultSel = ExtractLambda(node.Arguments[5]);

        var (innerEntityType, innerTableExpr) = ResolveInnerSource(node.Arguments[1]);
        var innerAlias = $"t{_nextJoinAliasIndex++}";

        var outerBinding = _pendingTransparentIdentifier is not null
            ? new RowBinding { Transparent = _pendingTransparentIdentifier }
            : new RowBinding { Alias = SourceAlias, Entity = _sourceEntityType };
        var innerBinding = new RowBinding { Alias = innerAlias, Entity = innerEntityType };

        var equalityOn = BuildJoinOnPredicate(outerKey, outerBinding, innerKey, innerBinding);

        // Parse the inequality lambda body and resolve column names through the
        // entity model. Falls back to the property name if the entity isn't tracked
        // — same defensive handling as TranslateMemberAccess.
        var (leftProp, rightProp, op) = ClickHouseAsofConditionParser.Parse(asofLambda);
        var leftCol = outerBinding.Entity?.FindProperty(leftProp)?.GetColumnName() ?? leftProp;
        var rightCol = innerBinding.Entity?.FindProperty(rightProp)?.GetColumnName() ?? rightProp;
        var leftRef = outerBinding.Alias is not null
            ? $"{outerBinding.Alias}.{QuoteIdentifier(leftCol)}"
            : QuoteIdentifier(leftCol);
        var rightRef = $"{innerBinding.Alias}.{QuoteIdentifier(rightCol)}";
        var asofIneq = $"{leftRef} {op} {rightRef}";

        var onPredicate = $"{equalityOn} AND {asofIneq}";
        _joins.Add(new JoinSource(innerTableExpr, innerAlias, kind, onPredicate));

        // Same transparent-identifier flow as VisitJoin: the result selector either
        // produces a transparent identifier consumed by a chained operator, or is
        // the terminal projection — in which case end-of-Translate emits it. We
        // stage both: stage the TI and emit the projection if no further operator
        // consumes it (mirrors VisitGroupJoin's behaviour).
        var bodyType = resultSel.Body.Type;
        var resultIsAnonymous = bodyType.IsGenericType
            && bodyType.Name.StartsWith("<>f__AnonymousType", StringComparison.Ordinal);

        if (resultIsAnonymous)
        {
            _pendingTransparentIdentifier = BuildTransparentIdentifier(resultSel, outerBinding, innerBinding);
        }
        else
        {
            EmitJoinProjection(resultSel, outerBinding, innerBinding);
        }

        return node;
    }

    /// <summary>
    /// Translates LEFT/RIGHT SEMI/ANTI joins. The result selector takes only the
    /// preserved-side row (left for LEFT SEMI/ANTI, right for RIGHT SEMI/ANTI)
    /// because the discarded side is not materialised. SQL kind carries the
    /// strictness; structurally identical to a regular Join otherwise.
    /// </summary>
    private Expression VisitSingleSideJoin(MethodCallExpression node, string kind, bool outerSidePreserved)
    {
        Visit(node.Arguments[0]); // outer source

        var outerKey = ExtractLambda(node.Arguments[2]);
        var innerKey = ExtractLambda(node.Arguments[3]);
        var resultSel = ExtractLambda(node.Arguments[4]);

        var (innerEntityType, innerTableExpr) = ResolveInnerSource(node.Arguments[1]);
        var innerAlias = $"t{_nextJoinAliasIndex++}";

        var outerBinding = _pendingTransparentIdentifier is not null
            ? new RowBinding { Transparent = _pendingTransparentIdentifier }
            : new RowBinding { Alias = SourceAlias, Entity = _sourceEntityType };
        var innerBinding = new RowBinding { Alias = innerAlias, Entity = innerEntityType };

        var onPredicate = BuildJoinOnPredicate(outerKey, outerBinding, innerKey, innerBinding);
        _joins.Add(new JoinSource(innerTableExpr, innerAlias, kind, onPredicate));

        // Result selector has exactly one parameter — bind it to the preserved
        // side and emit projection columns. Reuses the same body shapes
        // (MemberInit / NewExpression) that EmitJoinProjection accepts.
        var preservedBinding = outerSidePreserved ? outerBinding : innerBinding;
        var paramName = resultSel.Parameters[0].Name!;
        _paramOverrides[paramName] = preservedBinding;
        try
        {
            switch (resultSel.Body)
            {
                case MemberInitExpression memberInit:
                    foreach (var b in memberInit.Bindings)
                        if (b is MemberAssignment ma)
                            EmitProjectionColumn(ma.Member.Name, ma.Expression);
                    break;
                case NewExpression newExpr when newExpr.Members is not null:
                    for (int i = 0; i < newExpr.Arguments.Count; i++)
                        EmitProjectionColumn(newExpr.Members[i].Name, newExpr.Arguments[i]);
                    break;
                default:
                    throw new NotSupportedException(
                        $"SEMI/ANTI join result selector must produce an object initialiser or anonymous type. Got {resultSel.Body.NodeType}.");
            }
        }
        finally
        {
            _paramOverrides.Remove(paramName);
        }

        return node;
    }

    /// <summary>
    /// Translates ARRAY JOIN / LEFT ARRAY JOIN. The 3-arg form takes a source
    /// queryable, an array-column selector lambda (<c>e =&gt; e.Tags</c>), and
    /// a result selector with a per-element parameter (<c>(e, tag) =&gt; new {…}</c>).
    /// Emits <c>FROM source AS t0 [LEFT] ARRAY JOIN t0."Tags" AS "tag"</c> and
    /// binds the element parameter so bare references in the projection resolve
    /// to the alias.
    /// </summary>
    private Expression VisitArrayJoin(MethodCallExpression node, bool isLeft)
    {
        Visit(node.Arguments[0]); // source

        var arraySelector = ExtractLambda(node.Arguments[1]);
        var resultSel = ExtractLambda(node.Arguments[2]);

        // Resolve the array column: typical shape is `e => e.Tags` — a single
        // member access on the source row. Use the existing entity model to
        // resolve the column name.
        if (arraySelector.Body is not MemberExpression arrayMember)
        {
            throw new NotSupportedException("ARRAY JOIN array selector must be a single member access (e.g. e => e.Tags).");
        }
        var arrayColName = _sourceEntityType?.FindProperty(arrayMember.Member.Name)?.GetColumnName() ?? arrayMember.Member.Name;
        // Reference the column under the source alias so it doesn't clash with
        // the element alias declared by ARRAY JOIN.
        var columnExpr = $"{SourceAlias}.{QuoteIdentifier(arrayColName)}";
        var elementAlias = resultSel.Parameters[1].Name ?? arrayColName;

        _arrayJoins.Add(new ArrayJoinClause(columnExpr, elementAlias, isLeft));

        // Bind the element parameter to a value-only RowBinding so bare
        // references like `tag` in the projection resolve to the quoted alias.
        // Member access of `tag.<X>` (tuple ARRAY JOIN) would fall back to the
        // existing TranslateMemberAccess path — unsupported for now (throws).
        var sourceRow = new RowBinding { Alias = SourceAlias, Entity = _sourceEntityType };
        var elementRow = new RowBinding { Alias = elementAlias, Entity = null };

        EmitJoinProjection(resultSel, sourceRow, elementRow);

        return node;
    }

    /// <summary>
    /// Translates CROSS JOIN — no ON predicate, both sides full row.
    /// </summary>
    private Expression VisitCrossJoin(MethodCallExpression node)
    {
        Visit(node.Arguments[0]); // outer source

        var resultSel = ExtractLambda(node.Arguments[2]);
        var (innerEntityType, innerTableExpr) = ResolveInnerSource(node.Arguments[1]);
        var innerAlias = $"t{_nextJoinAliasIndex++}";

        var outerBinding = _pendingTransparentIdentifier is not null
            ? new RowBinding { Transparent = _pendingTransparentIdentifier }
            : new RowBinding { Alias = SourceAlias, Entity = _sourceEntityType };
        var innerBinding = new RowBinding { Alias = innerAlias, Entity = innerEntityType };

        // OnPredicate empty → Translate skips the ON clause and emits bare
        // `CROSS JOIN <table> AS <alias>`.
        _joins.Add(new JoinSource(innerTableExpr, innerAlias, "CROSS", string.Empty));

        var bodyType = resultSel.Body.Type;
        var resultIsAnonymous = bodyType.IsGenericType
            && bodyType.Name.StartsWith("<>f__AnonymousType", StringComparison.Ordinal);
        if (resultIsAnonymous)
        {
            _pendingTransparentIdentifier = BuildTransparentIdentifier(resultSel, outerBinding, innerBinding);
        }
        else
        {
            EmitJoinProjection(resultSel, outerBinding, innerBinding);
        }

        return node;
    }

    private Expression VisitSelectMany(MethodCallExpression node)
    {
        Visit(node.Arguments[0]); // outer source

        var collectionSelector = ExtractLambda(node.Arguments[1]);
        var outerParamName = collectionSelector.Parameters[0].Name!;

        // Special case: canonical LINQ LEFT JOIN lowering.
        //
        // `from o in outer join c in inner on … into cs from c in cs.DefaultIfEmpty() select …`
        // becomes `outer.GroupJoin(inner, …, (o, cs) => new { o, cs })
        //              .SelectMany(t => t.cs.DefaultIfEmpty(), (t, c) => …)`.
        //
        // The prior GroupJoin already added a LEFT JOIN to _joins and stashed a
        // row-passing transparent identifier (`_pendingTransparentIdentifierRows`).
        // The collection-selector body here is `t.cs.DefaultIfEmpty()` — `t.cs`
        // dereferences a member of that transparent identifier, which resolves to
        // the inner row binding of that prior GroupJoin. Detect this shape, reuse
        // the existing join, and treat the SelectMany's result selector as the
        // projection. No second JoinSource is added.
        if (_pendingTransparentIdentifierRows is not null
            && node.Arguments.Count >= 3
            && TryUnwrapDefaultIfEmpty(collectionSelector.Body, out var innerMemberAccess)
            && innerMemberAccess.Expression is ParameterExpression ti
            && ti.Name == outerParamName
            && _pendingTransparentIdentifierRows.TryGetValue(innerMemberAccess.Member.Name, out var innerSideRows))
        {
            // Resolve the outer row of the GroupJoin compound — pick the *other*
            // member of the row-pair. The compound has exactly two entries
            // (outer + inner); whichever one isn't the DefaultIfEmpty target is
            // the outer side of the LEFT JOIN.
            RowBinding outerSideRows = innerSideRows;
            foreach (var (member, binding) in _pendingTransparentIdentifierRows)
            {
                if (member != innerMemberAccess.Member.Name) { outerSideRows = binding; break; }
            }

            var rowsTi = _pendingTransparentIdentifierRows;
            _pendingTransparentIdentifierRows = null;

            var resultSelLeft = ExtractLambda(node.Arguments[2]);
            // Bind the SelectMany's two params: the first is the GroupJoin
            // compound (transparent rows); the second is the now-flattened inner
            // row. EmitJoinProjection-equivalent walk over the body emits SELECT
            // columns. We can't use the standard `_paramOverrides` since one
            // param maps to a row-set, not a single binding — instead, expand
            // member access of `t.<member>` inline below.
            var compoundParam = resultSelLeft.Parameters[0];
            var flatInnerParam = resultSelLeft.Parameters[1];

            EmitLeftJoinProjection(resultSelLeft, compoundParam, rowsTi, flatInnerParam, innerSideRows);
            return node;
        }

        // Walk the collection-selector body to extract the inner queryable plus
        // an optional ON predicate (when wrapped in .Where(...)). The outer
        // parameter binding must be active during the predicate translation
        // because the predicate references both sides.
        var (innerSourceExpr, onLambda) = UnwrapSelectManyCollectionBody(collectionSelector.Body);

        var (innerEntityType, innerTableExpr) = ResolveInnerSource(innerSourceExpr);
        var innerAlias = $"t{_nextJoinAliasIndex++}";

        var outerBinding = _pendingTransparentIdentifier is not null
            ? new RowBinding { Transparent = _pendingTransparentIdentifier }
            : new RowBinding { Alias = SourceAlias, Entity = _sourceEntityType };
        var innerBinding = new RowBinding { Alias = innerAlias, Entity = innerEntityType };

        string onPredicate;
        if (onLambda is null)
        {
            onPredicate = "1"; // CROSS JOIN — emitted via INNER JOIN ON 1 since
                               // ClickHouse parses both equivalently.
        }
        else
        {
            // The Where lambda's body references both the outer and inner params.
            _paramOverrides[outerParamName] = outerBinding;
            _paramOverrides[onLambda.Parameters[0].Name!] = innerBinding;
            try { onPredicate = TranslateExpression(onLambda.Body); }
            finally
            {
                _paramOverrides.Remove(outerParamName);
                _paramOverrides.Remove(onLambda.Parameters[0].Name!);
            }
        }

        _joins.Add(new JoinSource(innerTableExpr, innerAlias, "INNER", onPredicate));

        // Two overloads: with or without a result selector. With one, the result
        // selector projects each (outer, inner) pair into TResult. Without, the
        // SelectMany projects the inner row directly — stage a single-element
        // transparent identifier so a subsequent Select/Where/GroupBy can
        // address inner columns.
        if (node.Arguments.Count >= 3)
        {
            var resultSel = ExtractLambda(node.Arguments[2]);
            _pendingTransparentIdentifier = BuildTransparentIdentifier(resultSel, outerBinding, innerBinding);
        }
        else
        {
            _pendingTransparentIdentifier = null;
            _currentRowBinding = innerBinding;
        }

        return node;
    }

    /// <summary>
    /// Recognises <c>x.DefaultIfEmpty()</c> at the top of a SelectMany
    /// collection-selector body and returns the unwrapped <c>x</c> as a
    /// <see cref="MemberExpression"/>. The marker that distinguishes a LEFT
    /// JOIN lowering from a CROSS JOIN.
    /// </summary>
    private static bool TryUnwrapDefaultIfEmpty(Expression body, out MemberExpression innerSourceMember)
    {
        innerSourceMember = null!;
        while (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            body = u.Operand;
        if (body is MethodCallExpression mc
            && (mc.Method.DeclaringType == typeof(Queryable) || mc.Method.DeclaringType == typeof(Enumerable))
            && mc.Method.Name == "DefaultIfEmpty"
            && mc.Arguments.Count == 1
            && mc.Arguments[0] is MemberExpression me)
        {
            innerSourceMember = me;
            return true;
        }
        return false;
    }

    /// <summary>
    /// Emits projection columns for a SelectMany result selector that completes
    /// a canonical LEFT JOIN (GroupJoin + DefaultIfEmpty). The selector's first
    /// parameter is the GroupJoin compound (transparent rows: member name → row
    /// binding); the second is the post-DefaultIfEmpty inner row. Member access
    /// like <c>t.o.Amount</c> resolves through the compound; <c>c.Region</c>
    /// resolves through the inner binding. Null/default-checks against <c>c</c>
    /// (the inner row) collapse to the inner alias's column with no special
    /// rewrite — ClickHouse's LEFT JOIN already fills unmatched rows with
    /// type-defaults.
    /// </summary>
    private void EmitLeftJoinProjection(
        LambdaExpression resultSel,
        ParameterExpression compoundParam,
        Dictionary<string, RowBinding> rowsTi,
        ParameterExpression flatInnerParam,
        RowBinding flatInnerBinding)
    {
        // Use a small visitor to rewrite `compound.<member>.<col>` → direct member
        // access against the appropriate binding. For each rows-TI member, push a
        // synthetic param override; do the same for flatInnerParam → flatInnerBinding.
        // We use a ParameterReplacer-style approach: replace `compound.<member>`
        // sub-expressions with a fake ParameterExpression bound to the row, then
        // run normal translation.
        //
        // Practical approach: install per-member synthetic params in
        // _paramOverrides with names like "<compound>$<member>", and rewrite the
        // body to substitute each `compound.<member>` with a corresponding
        // ParameterExpression. Simpler: directly translate by walking the body.
        var rewriter = new CompoundMemberRewriter(compoundParam, rowsTi);
        var rewrittenBody = rewriter.Visit(resultSel.Body);

        // Bind the substituted param names to their underlying row bindings, plus
        // the flat inner param.
        foreach (var (memberName, binding) in rowsTi)
            _paramOverrides[CompoundMemberRewriter.SyntheticParamName(compoundParam.Name!, memberName)] = binding;
        _paramOverrides[flatInnerParam.Name!] = flatInnerBinding;
        try
        {
            switch (rewrittenBody)
            {
                case MemberInitExpression memberInit:
                    foreach (var b in memberInit.Bindings)
                        if (b is MemberAssignment ma)
                            EmitProjectionColumn(ma.Member.Name, ma.Expression);
                    break;
                case NewExpression newExpr when newExpr.Members is not null:
                    for (int i = 0; i < newExpr.Arguments.Count; i++)
                        EmitProjectionColumn(newExpr.Members[i].Name, newExpr.Arguments[i]);
                    break;
                default:
                    throw new NotSupportedException(
                        $"SelectMany after GroupJoin/DefaultIfEmpty must project to an object initialiser or anonymous type. Got {rewrittenBody.NodeType}.");
            }
        }
        finally
        {
            foreach (var memberName in rowsTi.Keys)
                _paramOverrides.Remove(CompoundMemberRewriter.SyntheticParamName(compoundParam.Name!, memberName));
            _paramOverrides.Remove(flatInnerParam.Name!);
        }
    }

    /// <summary>
    /// Rewrites <c>compound.&lt;member&gt;</c> member-access nodes into synthetic
    /// <see cref="ParameterExpression"/>s named <c>"&lt;compound&gt;$&lt;member&gt;"</c>.
    /// The downstream translator then resolves those through <c>_paramOverrides</c>
    /// just as if the user had written each row as a top-level lambda parameter.
    /// </summary>
    private sealed class CompoundMemberRewriter : ExpressionVisitor
    {
        private readonly ParameterExpression _compound;
        private readonly Dictionary<string, RowBinding> _rows;
        public CompoundMemberRewriter(ParameterExpression compound, Dictionary<string, RowBinding> rows)
        {
            _compound = compound;
            _rows = rows;
        }
        public static string SyntheticParamName(string compoundName, string memberName)
            => compoundName + "$" + memberName;
        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression == _compound && _rows.ContainsKey(node.Member.Name))
            {
                return Expression.Parameter(node.Type, SyntheticParamName(_compound.Name!, node.Member.Name));
            }
            return base.VisitMember(node);
        }
    }

    /// <summary>
    /// Unwraps the body of a SelectMany collection-selector lambda. Recognises
    /// <c>inner.Where(predicate)</c> and lifts the predicate as the JOIN ON
    /// clause; bare <c>inner</c> falls through as a CROSS JOIN. EF/Roslyn can
    /// insert <c>Convert</c> wrappers around the inner queryable; strip those.
    /// </summary>
    private static (Expression InnerSource, LambdaExpression? OnPredicate) UnwrapSelectManyCollectionBody(Expression body)
    {
        while (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            body = u.Operand;

        if (body is MethodCallExpression mc &&
            (mc.Method.DeclaringType == typeof(Queryable) || mc.Method.DeclaringType == typeof(Enumerable)) &&
            mc.Method.Name == "Where" && mc.Arguments.Count == 2)
        {
            return (mc.Arguments[0], ExtractLambda(mc.Arguments[1]));
        }

        return (body, null);
    }

    /// <summary>
    /// Walks a join's result selector and writes each projected member as a
    /// SELECT column. Mirrors <see cref="VisitSelectProjection"/> but with
    /// per-parameter alias bindings active (outer + inner) and special
    /// recognition of GroupJoin collection idioms.
    /// </summary>
    private void EmitJoinProjection(
        LambdaExpression resultSel, RowBinding outerBinding, RowBinding innerBinding)
    {
        var outerName = resultSel.Parameters[0].Name!;
        var innerName = resultSel.Parameters[1].Name!;
        _paramOverrides[outerName] = outerBinding;
        _paramOverrides[innerName] = innerBinding;
        try
        {
            var body = resultSel.Body;
            if (body is MemberInitExpression memberInit)
            {
                foreach (var b in memberInit.Bindings)
                {
                    if (b is MemberAssignment ma)
                        EmitProjectionColumn(ma.Member.Name, ma.Expression);
                }
            }
            else if (body is NewExpression newExpr && newExpr.Members is not null)
            {
                for (int i = 0; i < newExpr.Arguments.Count; i++)
                    EmitProjectionColumn(newExpr.Members[i].Name, newExpr.Arguments[i]);
            }
            else
            {
                throw new NotSupportedException(
                    $"GroupJoin result selector must produce an object initialiser or anonymous type. Got {body.NodeType}.");
            }
        }
        finally
        {
            _paramOverrides.Remove(outerName);
            _paramOverrides.Remove(innerName);
        }
    }

    private void EmitProjectionColumn(string memberName, Expression expr)
    {
        var sql = TranslateProjectionExpression(expr);
        var memberType = expr.Type;
        // Best-effort: pick up the target CLR type from the parent member if accessible —
        // but EmitJoinProjection is called for object-init / anonymous-new bodies, so the
        // member declaration's PropertyType is recoverable via reflection on the parent.
        sql = WrapWithClrTypeCast(sql, memberType, expr.Type);
        _selectColumns.Add($"{sql} AS {QuoteIdentifier(memberName)}");
    }

    /// <summary>
    /// Like <see cref="TranslateExpression"/> but recognises the GroupJoin
    /// collection idioms <c>cs.Select(c => c.X).FirstOrDefault()</c> and
    /// <c>... ?? default</c> as single-column references against the inner alias.
    /// </summary>
    private string TranslateProjectionExpression(Expression expr)
    {
        // Strip Convert wrappers EF can insert.
        while (expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            expr = u.Operand;

        // Coalesce idiom: <FirstOrDefault expr> ?? default
        if (expr is BinaryExpression { NodeType: ExpressionType.Coalesce } coal
            && TryUnwrapGroupJoinFirstOrDefault(coal.Left, out var leftRef))
        {
            var defaultSql = TranslateExpression(coal.Right);
            return $"coalesce({leftRef.Alias}.{QuoteIdentifier(leftRef.Column)}, {defaultSql})";
        }
        if (TryUnwrapGroupJoinFirstOrDefault(expr, out var firstRef))
        {
            return $"{firstRef.Alias}.{QuoteIdentifier(firstRef.Column)}";
        }
        return TranslateExpression(expr);
    }

    /// <summary>
    /// Resolves a join's right-hand-side <c>IQueryable&lt;TInner&gt;</c> argument to
    /// the inner entity type and the SQL table expression to put after the JOIN keyword.
    /// Detects the <c>ClickHouse:Dictionary</c> annotation and emits
    /// <c>dictionary('name')</c> instead of a quoted table name when present.
    /// </summary>
    private (IEntityType? Entity, string TableExpr) ResolveInnerSource(Expression rhs)
    {
        var elementType = ResolveQueryableElementType(rhs)
            ?? throw new NotSupportedException(
                "Could not resolve the inner sequence element type for the join. " +
                "Pass an IQueryable<T> reference whose T is a tracked entity type.");

        var entity = _model.FindEntityType(elementType);
        var tableName = entity?.GetTableName() ?? elementType.Name;

        var isDictionary = entity?.FindAnnotation(Metadata.ClickHouseAnnotationNames.Dictionary)?.Value is true;
        var tableExpr = isDictionary
            ? $"dictionary('{tableName.Replace("'", "\\'")}')"
            : QuoteIdentifier(tableName);

        return (entity, tableExpr);
    }

    private static Type? ResolveQueryableElementType(Expression expr)
    {
        // Strip any unbox/convert wrappers EF closure-capture sometimes inserts.
        while (expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            expr = u.Operand;

        // Common shape: ConstantExpression carrying the IQueryable directly (e.g. EnumerableQuery<T>)
        // or any other IQueryable<T>.
        if (expr is ConstantExpression { Value: IQueryable q })
            return q.ElementType;

        // Fallback: walk the static expression Type. IQueryable<T> / IEnumerable<T> / DbSet<T>.
        var t = expr.Type;
        if (t.IsGenericType)
        {
            var def = t.GetGenericTypeDefinition();
            if (def == typeof(IQueryable<>) || def == typeof(IEnumerable<>))
                return t.GetGenericArguments()[0];
        }
        foreach (var iface in t.GetInterfaces())
        {
            if (iface.IsGenericType && iface.GetGenericTypeDefinition() == typeof(IQueryable<>))
                return iface.GetGenericArguments()[0];
        }
        foreach (var iface in t.GetInterfaces())
        {
            if (iface.IsGenericType && iface.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                return iface.GetGenericArguments()[0];
        }

        // Last resort: closure-captured IQueryable reference. Compile and evaluate.
        // Acceptable at design time; the materialised view definition is built once
        // during OnModelCreating, not in any hot path.
        try
        {
            var value = Expression.Lambda(expr).Compile().DynamicInvoke();
            if (value is IQueryable q2) return q2.ElementType;
        }
        catch
        {
            // fall through to null
        }
        return null;
    }

    /// <summary>
    /// Constructs the SQL ON predicate for a join from its outer/inner key selectors.
    /// Supports composite keys via paired anonymous <c>NewExpression</c> bodies.
    /// </summary>
    private string BuildJoinOnPredicate(
        LambdaExpression outerKey, RowBinding outerBinding,
        LambdaExpression innerKey, RowBinding innerBinding)
    {
        // Composite key: both bodies are NewExpression with matching arity.
        if (outerKey.Body is NewExpression outerNew && innerKey.Body is NewExpression innerNew
            && outerNew.Arguments.Count == innerNew.Arguments.Count)
        {
            var conds = new List<string>(outerNew.Arguments.Count);
            for (int i = 0; i < outerNew.Arguments.Count; i++)
            {
                var l = TranslateUnderBindings(outerNew.Arguments[i], outerKey.Parameters[0].Name!, outerBinding);
                var r = TranslateUnderBindings(innerNew.Arguments[i], innerKey.Parameters[0].Name!, innerBinding);
                conds.Add($"{l} = {r}");
            }
            return string.Join(" AND ", conds);
        }

        var lhs = TranslateUnderBindings(outerKey.Body, outerKey.Parameters[0].Name!, outerBinding);
        var rhs = TranslateUnderBindings(innerKey.Body, innerKey.Parameters[0].Name!, innerBinding);
        return $"{lhs} = {rhs}";
    }

    /// <summary>
    /// Translates an expression with a single named parameter override active.
    /// Used by join-predicate construction so each side resolves to its own alias.
    /// </summary>
    private string TranslateUnderBindings(Expression body, string paramName, RowBinding binding)
    {
        _paramOverrides[paramName] = binding;
        try
        {
            return TranslateExpression(body);
        }
        finally
        {
            _paramOverrides.Remove(paramName);
        }
    }

    /// <summary>
    /// Walks a join's result selector and produces the transparent-identifier
    /// map: each result-property name → the (alias, column, entity) it came from.
    /// Used by the next operator's lambda to resolve compound-row member access.
    /// </summary>
    private Dictionary<string, MemberRef> BuildTransparentIdentifier(
        LambdaExpression resultSel, RowBinding outerBinding, RowBinding innerBinding)
    {
        // Bind both result-selector params for the duration of this walk.
        var outerName = resultSel.Parameters[0].Name!;
        var innerName = resultSel.Parameters[1].Name!;
        _paramOverrides[outerName] = outerBinding;
        _paramOverrides[innerName] = innerBinding;
        try
        {
            return BuildTransparentIdentifierFromBody(resultSel.Body);
        }
        finally
        {
            _paramOverrides.Remove(outerName);
            _paramOverrides.Remove(innerName);
        }
    }

    private Dictionary<string, MemberRef> BuildTransparentIdentifierFromBody(Expression body)
    {
        var ti = new Dictionary<string, MemberRef>();
        switch (body)
        {
            case NewExpression newExpr when newExpr.Members is not null:
                for (int i = 0; i < newExpr.Arguments.Count; i++)
                    ti[newExpr.Members[i].Name] = ResolveMemberRef(newExpr.Arguments[i]);
                break;
            case MemberInitExpression memberInit:
                foreach (var b in memberInit.Bindings)
                    if (b is MemberAssignment ma)
                        ti[ma.Member.Name] = ResolveMemberRef(ma.Expression);
                break;
            default:
                throw new NotSupportedException(
                    $"Join result selector must produce an anonymous type or object initialiser. Got {body.NodeType}.");
        }
        return ti;
    }

    /// <summary>
    /// Maps a result-selector member expression (typically <c>o.Amount</c>,
    /// <c>c.Region</c>, or for GroupJoin: <c>cs.Select(c => c.X).FirstOrDefault() ?? d</c>)
    /// into the alias + column + entity triple used by the transparent identifier.
    /// </summary>
    private MemberRef ResolveMemberRef(Expression expr)
    {
        // Strip Convert wrappers EF can insert around member access in projections.
        while (expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            expr = u.Operand;

        // GroupJoin idiom: cs.Select(c => c.X).FirstOrDefault() ?? "default"
        // Collapses to a coalesce(alias.column, default) reference under the inner alias.
        if (expr is BinaryExpression { NodeType: ExpressionType.Coalesce } coal
            && TryUnwrapGroupJoinFirstOrDefault(coal.Left, out var leftRef))
        {
            var defaultSql = TranslateExpression(coal.Right);
            return leftRef with { Column = $"coalesce({leftRef.Alias}.{QuoteIdentifier(leftRef.Column)}, {defaultSql})", Alias = "" };
        }
        if (TryUnwrapGroupJoinFirstOrDefault(expr, out var simpleRef))
        {
            return simpleRef;
        }

        // Plain o.Member / c.Member from a single-parameter binding.
        if (expr is MemberExpression { Expression: ParameterExpression pe } me
            && _paramOverrides.TryGetValue(pe.Name!, out var binding)
            && binding.Alias is not null)
        {
            var col = binding.Entity?.FindProperty(me.Member.Name)?.GetColumnName() ?? me.Member.Name;
            return new MemberRef(binding.Alias, col, binding.Entity);
        }

        // Fallback: translate to an SQL fragment and surface it as a synthetic
        // column reference under the source alias. Loses the alias.column shape
        // but preserves the SQL — used for computed projections in result selectors.
        var sql = TranslateExpression(expr);
        return new MemberRef("", sql, null) { };
    }

    /// <summary>
    /// Recognises <c>cs.Select(c => c.Member).FirstOrDefault()</c> shapes inside
    /// a GroupJoin result selector and resolves them to a single-column reference
    /// under the inner-side alias. Returns false for any other shape.
    /// </summary>
    private bool TryUnwrapGroupJoinFirstOrDefault(Expression expr, out MemberRef memberRef)
    {
        memberRef = default!;
        if (expr is not MethodCallExpression { Method.Name: "FirstOrDefault" or "First" } first)
            return false;
        if (first.Arguments.Count == 0)
            return false;

        Expression sourceArg = first.Arguments[0];
        // Optional .Select(c => c.Member) between the collection param and First*.
        if (sourceArg is MethodCallExpression { Method.Name: "Select" } sel
            && sel.Arguments.Count == 2
            && (sel.Arguments[1] is LambdaExpression
                || sel.Arguments[1] is UnaryExpression { Operand: LambdaExpression })
            && sel.Arguments[0] is ParameterExpression collectionParam
            && _paramOverrides.TryGetValue(collectionParam.Name!, out var binding)
            && binding.Alias is not null)
        {
            var selLambda = ExtractLambda(sel.Arguments[1]);
            // selLambda is c => c.Member — resolve member through inner binding.
            if (selLambda.Body is MemberExpression me)
            {
                var col = binding.Entity?.FindProperty(me.Member.Name)?.GetColumnName() ?? me.Member.Name;
                memberRef = new MemberRef(binding.Alias, col, binding.Entity);
                return true;
            }
        }
        return false;
    }

    /// <summary>
    /// Called at the start of each operator's lambda body. Binds each lambda
    /// parameter to either the staged transparent identifier (post-Join) or to
    /// the current row binding (otherwise). Idempotent for nested aggregate
    /// lambdas (e.g. <c>g.Sum(x => x.Amount)</c>) where x inherits g's binding.
    /// </summary>
    private void EnterLambda(LambdaExpression lambda)
    {
        if (_pendingTransparentIdentifier is not null)
        {
            _currentRowBinding = new RowBinding { Transparent = _pendingTransparentIdentifier };
            _pendingTransparentIdentifier = null;
        }
        // No need to populate _paramOverrides — single-parameter lambdas resolve
        // through _currentRowBinding when no override is present. Multi-parameter
        // lambdas (only Join's keys/result) set overrides themselves.
        _ = lambda;
    }

    private void VisitGroupByKeySelector(LambdaExpression keySelector)
    {
        var body = keySelector.Body;

        // Handle anonymous type: new { x.Date, x.ProductId }
        if (body is NewExpression newExpr)
        {
            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                var memberName = newExpr.Members?[i].Name;
                var columnSql = TranslateExpression(newExpr.Arguments[i]);
                _groupByColumns.Add(columnSql);

                if (memberName != null)
                {
                    _groupKeyMappings[memberName] = columnSql;
                }
            }
        }
        // Handle single column: x.Id
        else if (body is MemberExpression memberExpr)
        {
            var columnSql = TranslateMemberAccess(memberExpr);
            _groupByColumns.Add(columnSql);
            _groupKeyMappings[memberExpr.Member.Name] = columnSql;
        }
        // Handle member init: new KeyClass { Date = x.Date }
        else if (body is MemberInitExpression memberInit)
        {
            foreach (var binding in memberInit.Bindings)
            {
                if (binding is MemberAssignment assignment)
                {
                    var columnSql = TranslateExpression(assignment.Expression);
                    _groupByColumns.Add(columnSql);
                    _groupKeyMappings[assignment.Member.Name] = columnSql;
                }
            }
        }
        // Handle method call: x.OrderDate.ToStartOfHour()
        else if (body is MethodCallExpression methodExpr)
        {
            var columnSql = TranslateExpression(methodExpr);
            _groupByColumns.Add(columnSql);
            // For method calls, we don't have a member name, but the SQL is stored
            // and will be used when g.Key is accessed directly
        }
        // Fallback: any other expression form (Conditional ternary, Constant
        // literal, Binary arithmetic, Unary negate, etc.). Without this,
        // such keys silently leave _groupByColumns empty and a downstream
        // g.Key projection falls back to emitting the literal "Key" identifier.
        else
        {
            var columnSql = TranslateExpression(body);
            _groupByColumns.Add(columnSql);
        }
    }

    private Expression VisitSelect(MethodCallExpression node)
    {
        // Visit the source first (could be GroupBy result)
        Visit(node.Arguments[0]);

        // Get the result selector lambda
        if (node.Arguments[1] is UnaryExpression { Operand: LambdaExpression resultSelector })
        {
            EnterLambda(resultSelector);
            _groupByParameter = resultSelector.Parameters[0].Name;
            VisitSelectProjection(resultSelector);
        }

        return node;
    }

    private void VisitSelectProjection(LambdaExpression resultSelector)
    {
        var body = resultSelector.Body;

        // Handle new TResult { ... }
        if (body is MemberInitExpression memberInit)
        {
            foreach (var binding in memberInit.Bindings)
            {
                if (binding is MemberAssignment assignment)
                {
                    var columnSql = TranslateExpression(assignment.Expression);
                    var alias = assignment.Member.Name;
                    var memberType = (assignment.Member as System.Reflection.PropertyInfo)?.PropertyType
                                  ?? (assignment.Member as System.Reflection.FieldInfo)?.FieldType;
                    columnSql = WrapWithClrTypeCast(columnSql, memberType, assignment.Expression.Type);
                    _selectColumns.Add($"{columnSql} AS {QuoteIdentifier(alias)}");
                }
            }
        }
        // Handle anonymous type: new { g.Key.Date, Total = g.Sum(...) }
        // and positional records: new Tgt(g.Key, g.Sum(...)) — for records
        // the compiler does not populate newExpr.Members, so fall back to
        // the constructor's parameter names (which match the record's
        // positional property names). The column-name-derived fallback would
        // emit "Column0/Column1" and miss the entity columns.
        else if (body is NewExpression newExpr)
        {
            var ctorParams = newExpr.Constructor?.GetParameters();
            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                var memberName = newExpr.Members?[i].Name
                              ?? ctorParams?[i].Name
                              ?? $"Column{i}";
                var columnSql = TranslateExpression(newExpr.Arguments[i]);
                var memberType = (newExpr.Members?[i] as System.Reflection.PropertyInfo)?.PropertyType
                              ?? (newExpr.Members?[i] as System.Reflection.FieldInfo)?.FieldType
                              ?? ctorParams?[i].ParameterType;
                columnSql = WrapWithClrTypeCast(columnSql, memberType, newExpr.Arguments[i].Type);
                _selectColumns.Add($"{columnSql} AS {QuoteIdentifier(memberName)}");
            }
        }
    }

    /// <summary>
    /// Wraps <paramref name="columnSql"/> with a ClickHouse <c>toX(...)</c> cast
    /// when the projected expression's natural type would differ from the
    /// declared MV-target CLR type. Required because <c>CREATE MATERIALIZED
    /// VIEW … ENGINE = … AS SELECT</c> infers column types from the SELECT
    /// rather than the entity properties — e.g. a <c>long</c> property fed by
    /// <c>g.Count()</c> would otherwise materialise as <c>UInt64</c>, and
    /// <c>1L</c> as <c>UInt8</c>.
    /// </summary>
    private static string WrapWithClrTypeCast(string columnSql, Type? targetClrType, Type sourceExprType)
    {
        if (targetClrType is null) return columnSql;
        var nonNullable = Nullable.GetUnderlyingType(targetClrType) ?? targetClrType;

        // Primitive arrays — e.g. double[], long[]. ClickHouse aggregates like
        // quantilesTDigest return Array(Float32) by design, but the user's
        // declared property is Array(Float64). CAST handles both Array element
        // promotion and outer wrapping.
        if (nonNullable.IsArray && nonNullable != typeof(byte[]))
        {
            var elementChType = ClickHouseElementType(nonNullable.GetElementType());
            if (elementChType is null) return columnSql;
            return $"CAST({columnSql} AS Array({elementChType}))";
        }

        var cast = nonNullable.Name switch
        {
            "Int64" => "toInt64",
            "Int32" => "toInt32",
            "Int16" => "toInt16",
            "SByte" => "toInt8",
            "UInt64" => "toUInt64",
            "UInt32" => "toUInt32",
            "UInt16" => "toUInt16",
            "Byte" => "toUInt8",
            "Single" => "toFloat32",
            "Double" => "toFloat64",
            "Boolean" => "toUInt8",
            _ => null,
        };
        if (cast is null) return columnSql;
        return $"{cast}({columnSql})";
    }

    private static string? ClickHouseElementType(Type? clrElement) => clrElement?.Name switch
    {
        "Int64" => "Int64",
        "Int32" => "Int32",
        "Int16" => "Int16",
        "SByte" => "Int8",
        "UInt64" => "UInt64",
        "UInt32" => "UInt32",
        "UInt16" => "UInt16",
        "Byte" => "UInt8",
        "Single" => "Float32",
        "Double" => "Float64",
        "Boolean" => "UInt8",
        "String" => "String",
        _ => null,
    };

    private string TranslateExpression(Expression expr)
    {
        return expr switch
        {
            MemberExpression memberExpr => TranslateMemberAccess(memberExpr),
            MethodCallExpression methodExpr => TranslateMethodCall(methodExpr),
            ConstantExpression constExpr => TranslateConstant(constExpr),
            BinaryExpression binaryExpr => TranslateBinary(binaryExpr),
            UnaryExpression unaryExpr => TranslateUnary(unaryExpr),
            ConditionalExpression condExpr => TranslateConditional(condExpr),
            DefaultExpression defaultExpr => TranslateDefault(defaultExpr),
            NewExpression newExpr => TranslateNew(newExpr),
            ParameterExpression paramExpr => TranslateParameterReference(paramExpr),
            _ => throw new NotSupportedException(
                $"Expression type {expr.GetType().Name} is not supported in materialized view definitions.")
        };
    }

    /// <summary>
    /// Translates a bare parameter reference (e.g. <c>tag</c> in ARRAY JOIN's
    /// result selector <c>(e, tag) =&gt; new { e.Id, Tag = tag }</c>). Resolves
    /// through <c>_paramOverrides</c>: an override whose <c>Alias</c> is set
    /// but <c>Entity</c> is not is the signal "this param IS the value at
    /// alias" — used by ARRAY JOIN's element binding.
    /// </summary>
    private string TranslateParameterReference(ParameterExpression p)
    {
        if (_paramOverrides.TryGetValue(p.Name!, out var binding)
            && binding.Alias is not null && binding.Entity is null)
        {
            return QuoteIdentifier(binding.Alias);
        }
        throw new NotSupportedException(
            $"Bare parameter reference '{p.Name}' could not be resolved in this materialized-view context.");
    }

    /// <summary>
    /// Constant-folds a <c>NewExpression</c> by evaluating its constructor
    /// arguments at translate time and invoking the constructor via reflection.
    /// Handles inline literals like <c>new DateTime(2026, 1, 15, 10, 0, 0, DateTimeKind.Utc)</c>
    /// and <c>new Guid("…")</c>, as well as nested ctors and closure-captured args.
    /// The resulting object is re-emitted via <see cref="TranslateConstant"/>.
    /// </summary>
    private string TranslateNew(NewExpression newExpr)
    {
        if (newExpr.Constructor is null)
        {
            throw new NotSupportedException(
                $"NewExpression for {newExpr.Type.Name} has no constructor and cannot be folded.");
        }

        var args = new object?[newExpr.Arguments.Count];
        for (int i = 0; i < newExpr.Arguments.Count; i++)
        {
            if (!TryEvaluateConstant(newExpr.Arguments[i], out args[i]))
            {
                throw new NotSupportedException(
                    $"NewExpression for {newExpr.Type.Name} requires every constructor argument to be a " +
                    $"compile-time-evaluable constant or closure-captured local; argument at index {i} " +
                    $"is {newExpr.Arguments[i].NodeType} and could not be folded.");
            }
        }

        object? instance;
        try
        {
            instance = newExpr.Constructor.Invoke(args);
        }
        catch (Exception ex)
        {
            throw new NotSupportedException(
                $"NewExpression for {newExpr.Type.Name} could not be evaluated at translate time: {ex.Message}", ex);
        }

        return TranslateConstant(Expression.Constant(instance, newExpr.Type));
    }

    /// <summary>
    /// Tries to fold an expression to its runtime value. Recognises constants,
    /// closure captures (member access on a captured display class), static
    /// fields/properties, nested <c>NewExpression</c>s, and falls back to
    /// compile-and-invoke for anything else side-effect-free.
    /// </summary>
    private static bool TryEvaluateConstant(Expression expr, out object? value)
    {
        // Strip Convert wrappers EF can insert (e.g. int → long for ctor args).
        while (expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            expr = u.Operand;

        switch (expr)
        {
            case ConstantExpression c:
                value = c.Value;
                return true;
            case MemberExpression me when me.Expression is ConstantExpression cc:
                if (me.Member is FieldInfo fi) { value = fi.GetValue(cc.Value); return true; }
                if (me.Member is PropertyInfo pi) { value = pi.GetValue(cc.Value); return true; }
                break;
            case MemberExpression me when me.Expression is null:
                if (me.Member is FieldInfo sfi) { value = sfi.GetValue(null); return true; }
                if (me.Member is PropertyInfo spi) { value = spi.GetValue(null); return true; }
                break;
            case NewExpression nested when nested.Constructor is not null:
                var nargs = new object?[nested.Arguments.Count];
                for (int i = 0; i < nested.Arguments.Count; i++)
                {
                    if (!TryEvaluateConstant(nested.Arguments[i], out nargs[i]))
                    {
                        value = null;
                        return false;
                    }
                }
                value = nested.Constructor.Invoke(nargs);
                return true;
        }

        try
        {
            value = Expression.Lambda(expr).Compile().DynamicInvoke();
            return true;
        }
        catch
        {
            value = null;
            return false;
        }
    }

    private string TranslateDefault(DefaultExpression defaultExpr)
    {
        // Default values for common types
        return defaultExpr.Type.Name switch
        {
            "Int64" or "Int32" or "Int16" or "SByte" => "0",
            "UInt64" or "UInt32" or "UInt16" or "Byte" => "0",
            "Double" or "Single" or "Decimal" => "0.0",
            "String" => "''",
            _ => "NULL"
        };
    }

    private string TranslateMemberAccess(MemberExpression memberExpr)
    {
        // Handle static field access (e.g., UInt64.MaxValue)
        if (memberExpr.Expression == null)
        {
            // Static member access
            return memberExpr.Member switch
            {
                FieldInfo { Name: "MaxValue", DeclaringType.Name: "UInt64" } => "18446744073709551615",
                FieldInfo { Name: "MinValue", DeclaringType.Name: "UInt64" } => "0",
                FieldInfo { Name: "MaxValue", DeclaringType.Name: "Int64" } => "9223372036854775807",
                FieldInfo { Name: "MinValue", DeclaringType.Name: "Int64" } => "-9223372036854775808",
                _ => throw new NotSupportedException(
                    $"Static member {memberExpr.Member.DeclaringType?.Name}.{memberExpr.Member.Name} is not supported.")
            };
        }

        // Check if this is accessing the grouping key (g.Key.X) for anonymous type keys
        if (memberExpr.Expression is MemberExpression parentMember)
        {
            // g.Key.PropertyName
            if (parentMember.Member.Name == "Key" &&
                parentMember.Expression is ParameterExpression param &&
                param.Name == _groupByParameter)
            {
                if (_groupKeyMappings.TryGetValue(memberExpr.Member.Name, out var keySql))
                {
                    return keySql;
                }
            }
        }

        // Check if this is accessing g.Key directly (single-value key, not anonymous type)
        if (memberExpr.Member.Name == "Key" &&
            memberExpr.Expression is ParameterExpression keyParam &&
            keyParam.Name == _groupByParameter)
        {
            // Single-value key - return the first (and only) group by expression
            if (_groupByColumns.Count == 1)
            {
                return _groupByColumns[0];
            }
            // Multiple columns but accessing Key directly - shouldn't happen with proper LINQ
            // Fall through to default handling
        }

        // Check if this is accessing the source entity
        if (memberExpr.Expression is ParameterExpression sourceParam)
        {
            // Per-param override (set by BuildJoinOnPredicate while building an ON
            // predicate or by BuildTransparentIdentifier while walking a join's
            // result selector). Takes effect even before the JoinSource is staged,
            // because the ON predicate is computed before _joins is appended.
            if (_paramOverrides.TryGetValue(sourceParam.Name!, out var bound))
            {
                if (bound.Transparent is not null
                    && bound.Transparent.TryGetValue(memberExpr.Member.Name, out var tref0))
                {
                    return tref0.Alias.Length == 0
                        ? tref0.Column
                        : $"{tref0.Alias}.{QuoteIdentifier(tref0.Column)}";
                }
                if (bound.Alias is not null)
                {
                    var col0 = bound.Entity?.FindProperty(memberExpr.Member.Name)?.GetColumnName()
                               ?? memberExpr.Member.Name;
                    return $"{bound.Alias}.{QuoteIdentifier(col0)}";
                }
            }
            // Multi-source path: active whenever joins have been recorded. Falls
            // through to the current row binding (post-Join transparent identifier
            // staged via EnterLambda).
            if (HasJoins)
            {
                var binding = _currentRowBinding;
                if (binding.Transparent is not null
                    && binding.Transparent.TryGetValue(memberExpr.Member.Name, out var tref))
                {
                    return tref.Alias.Length == 0
                        ? tref.Column
                        : $"{tref.Alias}.{QuoteIdentifier(tref.Column)}";
                }
                if (binding.Alias is not null)
                {
                    var col = binding.Entity?.FindProperty(memberExpr.Member.Name)?.GetColumnName()
                              ?? memberExpr.Member.Name;
                    return $"{binding.Alias}.{QuoteIdentifier(col)}";
                }
            }
            // Single-source path: byte-for-byte identical to legacy behaviour.
            return GetColumnName(memberExpr.Member);
        }

        // Handle DateTime.Date property
        if (memberExpr.Member.Name == "Date" &&
            memberExpr.Member.DeclaringType == typeof(DateTime))
        {
            var innerSql = TranslateExpression(memberExpr.Expression!);
            return $"toDate({innerSql})";
        }

        // Handle other DateTime properties
        if (memberExpr.Member.DeclaringType == typeof(DateTime))
        {
            var innerSql = TranslateExpression(memberExpr.Expression!);
            return memberExpr.Member.Name switch
            {
                "Year" => $"toYear({innerSql})",
                "Month" => $"toMonth({innerSql})",
                "Day" => $"toDayOfMonth({innerSql})",
                "Hour" => $"toHour({innerSql})",
                "Minute" => $"toMinute({innerSql})",
                "Second" => $"toSecond({innerSql})",
                _ => throw new NotSupportedException($"DateTime.{memberExpr.Member.Name} is not supported.")
            };
        }

        // Could be a nested property - try to translate the full path
        if (memberExpr.Expression != null)
        {
            // This might be x.SomeProperty where x is from GroupBy lambda
            return GetColumnName(memberExpr.Member);
        }

        throw new NotSupportedException(
            $"Member access {memberExpr.Member.Name} could not be translated.");
    }

    private string TranslateMethodCall(MethodCallExpression methodExpr)
    {
        // Handle aggregate methods on IGrouping
        if (methodExpr.Method.DeclaringType == typeof(Enumerable))
        {
            return methodExpr.Method.Name switch
            {
                "Sum" => TranslateAggregate("sum", methodExpr),
                "Count" => TranslateCount(methodExpr),
                "Average" or "Avg" => TranslateAggregate("avg", methodExpr),
                "Min" => TranslateAggregate("min", methodExpr),
                "Max" => TranslateAggregate("max", methodExpr),
                _ => throw new NotSupportedException($"Method {methodExpr.Method.Name} is not supported.")
            };
        }

        // Handle ClickHouse extension methods
        if (methodExpr.Method.DeclaringType?.FullName == "EF.CH.Extensions.ClickHouseFunctions")
        {
            return TranslateClickHouseFunction(methodExpr);
        }

        // Handle ClickHouse aggregate functions
        if (methodExpr.Method.DeclaringType?.FullName == "EF.CH.Extensions.ClickHouseAggregates")
        {
            return TranslateClickHouseAggregate(methodExpr);
        }

        throw new NotSupportedException($"Method {methodExpr.Method.Name} is not supported.");
    }

    private string TranslateAggregate(string function, MethodCallExpression methodExpr)
    {
        // Sum(x => x.Value) has 2 arguments: source and selector
        if (methodExpr.Arguments.Count >= 2)
        {
            // Extract lambda - may be wrapped in UnaryExpression (Quote) or direct
            var selectorArg = methodExpr.Arguments[1];
            LambdaExpression? selector = selectorArg switch
            {
                UnaryExpression { Operand: LambdaExpression lambda } => lambda,
                LambdaExpression lambda => lambda,
                _ => null
            };

            if (selector != null)
            {
                var innerSql = TranslateExpression(selector.Body);
                return $"{function}({innerSql})";
            }
        }

        // Sum() with no selector - sum all values (rarely used)
        return $"{function}()";
    }

    private string TranslateCount(MethodCallExpression methodExpr)
    {
        // Count() with predicate
        if (methodExpr.Arguments.Count >= 2)
        {
            // Extract lambda - may be wrapped in UnaryExpression (Quote) or direct
            var predicateArg = methodExpr.Arguments[1];
            LambdaExpression? predicate = predicateArg switch
            {
                UnaryExpression { Operand: LambdaExpression lambda } => lambda,
                LambdaExpression lambda => lambda,
                _ => null
            };

            if (predicate != null)
            {
                var condition = TranslateExpression(predicate.Body);
                return $"countIf({condition})";
            }
        }

        // Simple Count()
        return "count()";
    }

    private string TranslateClickHouseFunction(MethodCallExpression methodExpr)
    {
        var functionName = methodExpr.Method.Name;
        var arg = methodExpr.Arguments.Count > 0
            ? TranslateExpression(methodExpr.Arguments[0])
            : throw new InvalidOperationException($"ClickHouse function {functionName} requires an argument.");

        return functionName switch
        {
            "ToYYYYMM" => $"toYYYYMM({arg})",
            "ToYYYYMMDD" => $"toYYYYMMDD({arg})",
            "ToStartOfHour" => $"toStartOfHour({arg})",
            "ToStartOfDay" => $"toStartOfDay({arg})",
            "ToStartOfMonth" => $"toStartOfMonth({arg})",
            "ToStartOfYear" => $"toStartOfYear({arg})",
            "ToStartOfWeek" => $"toStartOfWeek({arg})",
            "ToStartOfQuarter" => $"toStartOfQuarter({arg})",
            "ToStartOfMinute" => $"toStartOfMinute({arg})",
            "ToStartOfFiveMinutes" => $"toStartOfFiveMinutes({arg})",
            "ToStartOfFifteenMinutes" => $"toStartOfFifteenMinutes({arg})",
            "ToUnixTimestamp64Milli" => $"toUnixTimestamp64Milli({arg})",
            "CityHash64" => $"cityHash64({arg})",
            "ToISOYear" => $"toISOYear({arg})",
            "ToISOWeek" => $"toISOWeek({arg})",
            "ToDayOfWeek" => $"toDayOfWeek({arg})",
            "ToDayOfYear" => $"toDayOfYear({arg})",
            "ToQuarter" => $"toQuarter({arg})",
            _ => throw new NotSupportedException($"ClickHouse function {functionName} is not supported.")
        };
    }

    private string TranslateClickHouseAggregate(MethodCallExpression methodExpr)
    {
        var methodName = methodExpr.Method.Name;

        return methodName switch
        {
            // Phase 1 - Simple single-selector aggregates
            "Uniq" => TranslateSimpleClickHouseAggregate("uniq", methodExpr),
            "UniqExact" => TranslateSimpleClickHouseAggregate("uniqExact", methodExpr),
            "AnyValue" => TranslateSimpleClickHouseAggregate("any", methodExpr),
            "AnyLastValue" => TranslateSimpleClickHouseAggregate("anyLast", methodExpr),

            // Phase 1 - Two-selector aggregates
            "ArgMax" => TranslateTwoSelectorAggregate("argMax", methodExpr),
            "ArgMin" => TranslateTwoSelectorAggregate("argMin", methodExpr),

            // Phase 2 - Statistical aggregates
            "Median" => TranslateSimpleClickHouseAggregate("median", methodExpr),
            "StddevPop" => TranslateSimpleClickHouseAggregate("stddevPop", methodExpr),
            "StddevSamp" => TranslateSimpleClickHouseAggregate("stddevSamp", methodExpr),
            "VarPop" => TranslateSimpleClickHouseAggregate("varPop", methodExpr),
            "VarSamp" => TranslateSimpleClickHouseAggregate("varSamp", methodExpr),

            // Phase 2 - Parameterized aggregates
            "Quantile" => TranslateParametricQuantile("quantile", methodExpr),

            // Approximate count distinct
            "UniqCombined" => TranslateSimpleClickHouseAggregate("uniqCombined", methodExpr),
            "UniqCombined64" => TranslateSimpleClickHouseAggregate("uniqCombined64", methodExpr),
            "UniqHLL12" => TranslateSimpleClickHouseAggregate("uniqHLL12", methodExpr),
            "UniqTheta" => TranslateSimpleClickHouseAggregate("uniqTheta", methodExpr),

            // Quantile variants
            "QuantileTDigest" => TranslateParametricQuantile("quantileTDigest", methodExpr),
            "QuantileDD" => TranslateQuantileDD(methodExpr),
            "QuantileExact" => TranslateParametricQuantile("quantileExact", methodExpr),
            "QuantileTiming" => TranslateParametricQuantile("quantileTiming", methodExpr),

            // Multi-quantile
            "Quantiles" => TranslateMultiQuantile("quantiles", methodExpr),
            "QuantilesTDigest" => TranslateMultiQuantile("quantilesTDigest", methodExpr),

            // Phase 3 - Array aggregates
            "GroupArray" => TranslateGroupArray(methodExpr),
            "GroupUniqArray" => TranslateSimpleClickHouseAggregate("groupUniqArray", methodExpr),
            "TopK" => TranslateTopK(methodExpr),
            "TopKWeighted" => TranslateTopKWeighted(methodExpr),

            // If combinators - conditional aggregation
            "CountIf" => TranslateCountIfAggregate(methodExpr),
            "SumIf" => TranslateIfAggregate("sumIf", methodExpr),
            "AvgIf" => TranslateIfAggregate("avgIf", methodExpr),
            "MinIf" => TranslateIfAggregate("minIf", methodExpr),
            "MaxIf" => TranslateIfAggregate("maxIf", methodExpr),
            "UniqIf" => TranslateIfAggregate("uniqIf", methodExpr),
            "UniqExactIf" => TranslateIfAggregate("uniqExactIf", methodExpr),
            "AnyIf" => TranslateIfAggregate("anyIf", methodExpr),
            "AnyLastIf" => TranslateIfAggregate("anyLastIf", methodExpr),
            "QuantileIf" => TranslateQuantileIfAggregate(methodExpr),
            "ArgMaxIf" => TranslateTwoSelectorIfAggregate("argMaxIf", methodExpr),
            "ArgMinIf" => TranslateTwoSelectorIfAggregate("argMinIf", methodExpr),
            "TopKIf" => TranslateTopKIfAggregate(methodExpr),
            "TopKWeightedIf" => TranslateTopKWeightedIfAggregate(methodExpr),
            "GroupArrayIf" => TranslateGroupArrayIfAggregate(methodExpr),
            "GroupUniqArrayIf" => TranslateGroupUniqArrayIfAggregate(methodExpr),
            "MedianIf" => TranslateIfAggregate("medianIf", methodExpr),
            "StddevPopIf" => TranslateIfAggregate("stddevPopIf", methodExpr),
            "StddevSampIf" => TranslateIfAggregate("stddevSampIf", methodExpr),
            "VarPopIf" => TranslateIfAggregate("varPopIf", methodExpr),
            "VarSampIf" => TranslateIfAggregate("varSampIf", methodExpr),
            "UniqCombinedIf" => TranslateIfAggregate("uniqCombinedIf", methodExpr),
            "UniqCombined64If" => TranslateIfAggregate("uniqCombined64If", methodExpr),
            "UniqHLL12If" => TranslateIfAggregate("uniqHLL12If", methodExpr),
            "UniqThetaIf" => TranslateIfAggregate("uniqThetaIf", methodExpr),
            "QuantileTDigestIf" => TranslateParametricQuantileIfAggregate("quantileTDigestIf", methodExpr),
            "QuantileExactIf" => TranslateParametricQuantileIfAggregate("quantileExactIf", methodExpr),
            "QuantileTimingIf" => TranslateParametricQuantileIfAggregate("quantileTimingIf", methodExpr),
            "QuantileDDIf" => TranslateQuantileDDIfAggregate(methodExpr),
            "QuantilesIf" => TranslateMultiQuantileIfAggregate("quantilesIf", methodExpr),
            "QuantilesTDigestIf" => TranslateMultiQuantileIfAggregate("quantilesTDigestIf", methodExpr),

            // -State combinators — for AggregatingMergeTree storage.
            "CountState" => "countState()",
            "SumState" => TranslateSimpleClickHouseAggregate("sumState", methodExpr),
            "AvgState" => TranslateSimpleClickHouseAggregate("avgState", methodExpr),
            "MinState" => TranslateSimpleClickHouseAggregate("minState", methodExpr),
            "MaxState" => TranslateSimpleClickHouseAggregate("maxState", methodExpr),
            "UniqState" => TranslateSimpleClickHouseAggregate("uniqState", methodExpr),
            "UniqExactState" => TranslateSimpleClickHouseAggregate("uniqExactState", methodExpr),
            "AnyState" => TranslateSimpleClickHouseAggregate("anyState", methodExpr),
            "AnyLastState" => TranslateSimpleClickHouseAggregate("anyLastState", methodExpr),
            "MedianState" => TranslateSimpleClickHouseAggregate("medianState", methodExpr),
            "StddevPopState" => TranslateSimpleClickHouseAggregate("stddevPopState", methodExpr),
            "StddevSampState" => TranslateSimpleClickHouseAggregate("stddevSampState", methodExpr),
            "VarPopState" => TranslateSimpleClickHouseAggregate("varPopState", methodExpr),
            "VarSampState" => TranslateSimpleClickHouseAggregate("varSampState", methodExpr),
            "UniqCombinedState" => TranslateSimpleClickHouseAggregate("uniqCombinedState", methodExpr),
            "UniqCombined64State" => TranslateSimpleClickHouseAggregate("uniqCombined64State", methodExpr),
            "UniqHLL12State" => TranslateSimpleClickHouseAggregate("uniqHLL12State", methodExpr),
            "UniqThetaState" => TranslateSimpleClickHouseAggregate("uniqThetaState", methodExpr),
            "ArgMaxState" => TranslateTwoSelectorAggregate("argMaxState", methodExpr),
            "ArgMinState" => TranslateTwoSelectorAggregate("argMinState", methodExpr),
            "QuantileState" => TranslateParametricQuantile("quantileState", methodExpr),
            "QuantileTDigestState" => TranslateParametricQuantile("quantileTDigestState", methodExpr),
            "QuantileExactState" => TranslateParametricQuantile("quantileExactState", methodExpr),
            "QuantileTimingState" => TranslateParametricQuantile("quantileTimingState", methodExpr),
            "QuantileDDState" => TranslateQuantileDDState(methodExpr),
            "QuantilesState" => TranslateMultiQuantile("quantilesState", methodExpr),
            "QuantilesTDigestState" => TranslateMultiQuantile("quantilesTDigestState", methodExpr),
            "GroupArrayState" => TranslateGroupArrayState(methodExpr),
            "GroupUniqArrayState" => TranslateGroupUniqArrayState(methodExpr),
            "TopKState" => TranslateTopKState(methodExpr),
            "TopKWeightedState" => TranslateTopKWeightedState(methodExpr),

            // -MergeState combinators — chain AggregatingMergeTree → AggregatingMergeTree.
            // Reads an AMT state column, merges, and re-emits state for the next AMT.
            "CountMergeState" => TranslateSimpleClickHouseAggregate("countMergeState", methodExpr),
            "SumMergeState" => TranslateSimpleClickHouseAggregate("sumMergeState", methodExpr),
            "AvgMergeState" => TranslateSimpleClickHouseAggregate("avgMergeState", methodExpr),
            "MinMergeState" => TranslateSimpleClickHouseAggregate("minMergeState", methodExpr),
            "MaxMergeState" => TranslateSimpleClickHouseAggregate("maxMergeState", methodExpr),
            "UniqMergeState" => TranslateSimpleClickHouseAggregate("uniqMergeState", methodExpr),
            "UniqExactMergeState" => TranslateSimpleClickHouseAggregate("uniqExactMergeState", methodExpr),
            "AnyMergeState" => TranslateSimpleClickHouseAggregate("anyMergeState", methodExpr),
            "AnyLastMergeState" => TranslateSimpleClickHouseAggregate("anyLastMergeState", methodExpr),
            "QuantileMergeState" => TranslateParametricQuantile("quantileMergeState", methodExpr),

            // -StateIf combinators — conditional -State for AggregatingMergeTree.
            "CountStateIf" => TranslateCountStateIfAggregate(methodExpr),
            "SumStateIf" => TranslateIfAggregate("sumStateIf", methodExpr),
            "AvgStateIf" => TranslateIfAggregate("avgStateIf", methodExpr),
            "MinStateIf" => TranslateIfAggregate("minStateIf", methodExpr),
            "MaxStateIf" => TranslateIfAggregate("maxStateIf", methodExpr),
            "UniqStateIf" => TranslateIfAggregate("uniqStateIf", methodExpr),
            "UniqExactStateIf" => TranslateIfAggregate("uniqExactStateIf", methodExpr),
            "AnyStateIf" => TranslateIfAggregate("anyStateIf", methodExpr),
            "AnyLastStateIf" => TranslateIfAggregate("anyLastStateIf", methodExpr),
            "MedianStateIf" => TranslateIfAggregate("medianStateIf", methodExpr),
            "StddevPopStateIf" => TranslateIfAggregate("stddevPopStateIf", methodExpr),
            "StddevSampStateIf" => TranslateIfAggregate("stddevSampStateIf", methodExpr),
            "VarPopStateIf" => TranslateIfAggregate("varPopStateIf", methodExpr),
            "VarSampStateIf" => TranslateIfAggregate("varSampStateIf", methodExpr),
            "UniqCombinedStateIf" => TranslateIfAggregate("uniqCombinedStateIf", methodExpr),
            "UniqCombined64StateIf" => TranslateIfAggregate("uniqCombined64StateIf", methodExpr),
            "UniqHLL12StateIf" => TranslateIfAggregate("uniqHLL12StateIf", methodExpr),
            "UniqThetaStateIf" => TranslateIfAggregate("uniqThetaStateIf", methodExpr),
            "ArgMaxStateIf" => TranslateTwoSelectorIfAggregate("argMaxStateIf", methodExpr),
            "ArgMinStateIf" => TranslateTwoSelectorIfAggregate("argMinStateIf", methodExpr),
            "QuantileStateIf" => TranslateQuantileStateIf("quantileStateIf", methodExpr),
            "QuantileTDigestStateIf" => TranslateParametricQuantileIfAggregate("quantileTDigestStateIf", methodExpr),
            "QuantileExactStateIf" => TranslateParametricQuantileIfAggregate("quantileExactStateIf", methodExpr),
            "QuantileTimingStateIf" => TranslateParametricQuantileIfAggregate("quantileTimingStateIf", methodExpr),
            "QuantileDDStateIf" => TranslateQuantileDDStateIf(methodExpr),
            "QuantilesStateIf" => TranslateMultiQuantileIfAggregate("quantilesStateIf", methodExpr),
            "QuantilesTDigestStateIf" => TranslateMultiQuantileIfAggregate("quantilesTDigestStateIf", methodExpr),
            "GroupArrayStateIf" => TranslateGroupArrayStateIf(methodExpr),
            "GroupUniqArrayStateIf" => TranslateGroupUniqArrayStateIf(methodExpr),
            "TopKStateIf" => TranslateTopKStateIf(methodExpr),
            "TopKWeightedStateIf" => TranslateTopKWeightedStateIf(methodExpr),

            _ => throw new NotSupportedException($"ClickHouse aggregate {methodName} is not supported.")
        };
    }

    // -State helpers that mirror their non-State counterparts but emit stateful function names.

    private string TranslateCountStateIfAggregate(MethodCallExpression methodExpr)
    {
        // CountStateIf(source, predicate)
        var predicate = ExtractLambda(methodExpr.Arguments[1]);
        var conditionSql = TranslateExpression(predicate.Body);
        return $"countStateIf({conditionSql})";
    }

    private string TranslateQuantileDDState(MethodCallExpression methodExpr)
    {
        var accuracy = ExtractConstantValue<double>(methodExpr.Arguments[1]);
        var level = ExtractConstantValue<double>(methodExpr.Arguments[2]);
        var selector = ExtractLambda(methodExpr.Arguments[3]);
        var innerSql = TranslateExpression(selector.Body);
        var accuracyStr = accuracy.ToString(System.Globalization.CultureInfo.InvariantCulture);
        var levelStr = level.ToString(System.Globalization.CultureInfo.InvariantCulture);
        return $"quantileDDState({accuracyStr}, {levelStr})({innerSql})";
    }

    private string TranslateGroupArrayState(MethodCallExpression methodExpr)
    {
        if (methodExpr.Arguments.Count == 2)
        {
            var selector = ExtractLambda(methodExpr.Arguments[1]);
            return $"groupArrayState({TranslateExpression(selector.Body)})";
        }
        else
        {
            var maxSize = ExtractConstantValue<int>(methodExpr.Arguments[1]);
            var selector = ExtractLambda(methodExpr.Arguments[2]);
            return $"groupArrayState({maxSize})({TranslateExpression(selector.Body)})";
        }
    }

    private string TranslateGroupUniqArrayState(MethodCallExpression methodExpr)
    {
        if (methodExpr.Arguments.Count == 2)
        {
            var selector = ExtractLambda(methodExpr.Arguments[1]);
            return $"groupUniqArrayState({TranslateExpression(selector.Body)})";
        }
        else
        {
            var maxSize = ExtractConstantValue<int>(methodExpr.Arguments[1]);
            var selector = ExtractLambda(methodExpr.Arguments[2]);
            return $"groupUniqArrayState({maxSize})({TranslateExpression(selector.Body)})";
        }
    }

    private string TranslateTopKState(MethodCallExpression methodExpr)
    {
        var k = ExtractConstantValue<int>(methodExpr.Arguments[1]);
        var selector = ExtractLambda(methodExpr.Arguments[2]);
        return $"topKState({k})({TranslateExpression(selector.Body)})";
    }

    private string TranslateTopKWeightedState(MethodCallExpression methodExpr)
    {
        var k = ExtractConstantValue<int>(methodExpr.Arguments[1]);
        var selector = ExtractLambda(methodExpr.Arguments[2]);
        var weightSelector = ExtractLambda(methodExpr.Arguments[3]);
        return $"topKWeightedState({k})({TranslateExpression(selector.Body)}, {TranslateExpression(weightSelector.Body)})";
    }

    private string TranslateQuantileStateIf(string functionName, MethodCallExpression methodExpr)
    {
        var level = ExtractConstantValue<double>(methodExpr.Arguments[1]);
        var selector = ExtractLambda(methodExpr.Arguments[2]);
        var predicate = ExtractLambda(methodExpr.Arguments[3]);
        var valueSql = TranslateExpression(selector.Body);
        var conditionSql = TranslateExpression(predicate.Body);
        return $"{functionName}({level.ToString(System.Globalization.CultureInfo.InvariantCulture)})({valueSql}, {conditionSql})";
    }

    private string TranslateQuantileDDStateIf(MethodCallExpression methodExpr)
    {
        var accuracy = ExtractConstantValue<double>(methodExpr.Arguments[1]);
        var level = ExtractConstantValue<double>(methodExpr.Arguments[2]);
        var selector = ExtractLambda(methodExpr.Arguments[3]);
        var predicate = ExtractLambda(methodExpr.Arguments[4]);
        var valueSql = TranslateExpression(selector.Body);
        var conditionSql = TranslateExpression(predicate.Body);
        var accuracyStr = accuracy.ToString(System.Globalization.CultureInfo.InvariantCulture);
        var levelStr = level.ToString(System.Globalization.CultureInfo.InvariantCulture);
        return $"quantileDDStateIf({accuracyStr}, {levelStr})({valueSql}, {conditionSql})";
    }

    private string TranslateGroupArrayStateIf(MethodCallExpression methodExpr)
    {
        if (methodExpr.Arguments.Count == 3)
        {
            var selector = ExtractLambda(methodExpr.Arguments[1]);
            var predicate = ExtractLambda(methodExpr.Arguments[2]);
            return $"groupArrayStateIf({TranslateExpression(selector.Body)}, {TranslateExpression(predicate.Body)})";
        }
        else
        {
            var maxSize = ExtractConstantValue<int>(methodExpr.Arguments[1]);
            var selector = ExtractLambda(methodExpr.Arguments[2]);
            var predicate = ExtractLambda(methodExpr.Arguments[3]);
            return $"groupArrayStateIf({maxSize})({TranslateExpression(selector.Body)}, {TranslateExpression(predicate.Body)})";
        }
    }

    private string TranslateGroupUniqArrayStateIf(MethodCallExpression methodExpr)
    {
        if (methodExpr.Arguments.Count == 3)
        {
            var selector = ExtractLambda(methodExpr.Arguments[1]);
            var predicate = ExtractLambda(methodExpr.Arguments[2]);
            return $"groupUniqArrayStateIf({TranslateExpression(selector.Body)}, {TranslateExpression(predicate.Body)})";
        }
        else
        {
            var maxSize = ExtractConstantValue<int>(methodExpr.Arguments[1]);
            var selector = ExtractLambda(methodExpr.Arguments[2]);
            var predicate = ExtractLambda(methodExpr.Arguments[3]);
            return $"groupUniqArrayStateIf({maxSize})({TranslateExpression(selector.Body)}, {TranslateExpression(predicate.Body)})";
        }
    }

    private string TranslateTopKStateIf(MethodCallExpression methodExpr)
    {
        var k = ExtractConstantValue<int>(methodExpr.Arguments[1]);
        var selector = ExtractLambda(methodExpr.Arguments[2]);
        var predicate = ExtractLambda(methodExpr.Arguments[3]);
        return $"topKStateIf({k})({TranslateExpression(selector.Body)}, {TranslateExpression(predicate.Body)})";
    }

    private string TranslateTopKWeightedStateIf(MethodCallExpression methodExpr)
    {
        var k = ExtractConstantValue<int>(methodExpr.Arguments[1]);
        var selector = ExtractLambda(methodExpr.Arguments[2]);
        var weightSelector = ExtractLambda(methodExpr.Arguments[3]);
        var predicate = ExtractLambda(methodExpr.Arguments[4]);
        return $"topKWeightedStateIf({k})({TranslateExpression(selector.Body)}, {TranslateExpression(weightSelector.Body)}, {TranslateExpression(predicate.Body)})";
    }

    private string TranslateCountIfAggregate(MethodCallExpression methodExpr)
    {
        // Pattern: CountIf(source, predicate)
        var predicate = ExtractLambda(methodExpr.Arguments[1]);
        var conditionSql = TranslateExpression(predicate.Body);
        return $"countIf({conditionSql})";
    }

    private string TranslateIfAggregate(string function, MethodCallExpression methodExpr)
    {
        // Pattern: SumIf(source, selector, predicate) etc.
        var selector = ExtractLambda(methodExpr.Arguments[1]);
        var predicate = ExtractLambda(methodExpr.Arguments[2]);
        var valueSql = TranslateExpression(selector.Body);
        var conditionSql = TranslateExpression(predicate.Body);
        return $"{function}({valueSql}, {conditionSql})";
    }

    private string TranslateQuantileIfAggregate(MethodCallExpression methodExpr)
    {
        // Pattern: QuantileIf(source, level, selector, predicate)
        var level = ExtractConstantValue<double>(methodExpr.Arguments[1]);
        var selector = ExtractLambda(methodExpr.Arguments[2]);
        var predicate = ExtractLambda(methodExpr.Arguments[3]);
        var valueSql = TranslateExpression(selector.Body);
        var conditionSql = TranslateExpression(predicate.Body);
        return $"quantileIf({level.ToString(System.Globalization.CultureInfo.InvariantCulture)})({valueSql}, {conditionSql})";
    }

    private string TranslateTwoSelectorIfAggregate(string function, MethodCallExpression methodExpr)
    {
        // Pattern: ArgMaxIf/ArgMinIf(source, argSelector, valSelector, predicate)
        var argSelector = ExtractLambda(methodExpr.Arguments[1]);
        var valSelector = ExtractLambda(methodExpr.Arguments[2]);
        var predicate = ExtractLambda(methodExpr.Arguments[3]);
        var argSql = TranslateExpression(argSelector.Body);
        var valSql = TranslateExpression(valSelector.Body);
        var conditionSql = TranslateExpression(predicate.Body);
        return $"{function}({argSql}, {valSql}, {conditionSql})";
    }

    private string TranslateTopKIfAggregate(MethodCallExpression methodExpr)
    {
        var k = ExtractConstantValue<int>(methodExpr.Arguments[1]);
        var selector = ExtractLambda(methodExpr.Arguments[2]);
        var predicate = ExtractLambda(methodExpr.Arguments[3]);
        var valueSql = TranslateExpression(selector.Body);
        var conditionSql = TranslateExpression(predicate.Body);
        return $"topKIf({k})({valueSql}, {conditionSql})";
    }

    private string TranslateTopKWeightedIfAggregate(MethodCallExpression methodExpr)
    {
        var k = ExtractConstantValue<int>(methodExpr.Arguments[1]);
        var selector = ExtractLambda(methodExpr.Arguments[2]);
        var weightSelector = ExtractLambda(methodExpr.Arguments[3]);
        var predicate = ExtractLambda(methodExpr.Arguments[4]);
        var valueSql = TranslateExpression(selector.Body);
        var weightSql = TranslateExpression(weightSelector.Body);
        var conditionSql = TranslateExpression(predicate.Body);
        return $"topKWeightedIf({k})({valueSql}, {weightSql}, {conditionSql})";
    }

    private string TranslateGroupArrayIfAggregate(MethodCallExpression methodExpr)
    {
        if (methodExpr.Arguments.Count == 3)
        {
            var selector = ExtractLambda(methodExpr.Arguments[1]);
            var predicate = ExtractLambda(methodExpr.Arguments[2]);
            var valueSql = TranslateExpression(selector.Body);
            var conditionSql = TranslateExpression(predicate.Body);
            return $"groupArrayIf({valueSql}, {conditionSql})";
        }
        else
        {
            var maxSize = ExtractConstantValue<int>(methodExpr.Arguments[1]);
            var selector = ExtractLambda(methodExpr.Arguments[2]);
            var predicate = ExtractLambda(methodExpr.Arguments[3]);
            var valueSql = TranslateExpression(selector.Body);
            var conditionSql = TranslateExpression(predicate.Body);
            return $"groupArrayIf({maxSize})({valueSql}, {conditionSql})";
        }
    }

    private string TranslateGroupUniqArrayIfAggregate(MethodCallExpression methodExpr)
    {
        if (methodExpr.Arguments.Count == 3)
        {
            var selector = ExtractLambda(methodExpr.Arguments[1]);
            var predicate = ExtractLambda(methodExpr.Arguments[2]);
            var valueSql = TranslateExpression(selector.Body);
            var conditionSql = TranslateExpression(predicate.Body);
            return $"groupUniqArrayIf({valueSql}, {conditionSql})";
        }
        else
        {
            var maxSize = ExtractConstantValue<int>(methodExpr.Arguments[1]);
            var selector = ExtractLambda(methodExpr.Arguments[2]);
            var predicate = ExtractLambda(methodExpr.Arguments[3]);
            var valueSql = TranslateExpression(selector.Body);
            var conditionSql = TranslateExpression(predicate.Body);
            return $"groupUniqArrayIf({maxSize})({valueSql}, {conditionSql})";
        }
    }

    private string TranslateParametricQuantileIfAggregate(string functionName, MethodCallExpression methodExpr)
    {
        var level = ExtractConstantValue<double>(methodExpr.Arguments[1]);
        var selector = ExtractLambda(methodExpr.Arguments[2]);
        var predicate = ExtractLambda(methodExpr.Arguments[3]);
        var valueSql = TranslateExpression(selector.Body);
        var conditionSql = TranslateExpression(predicate.Body);
        return $"{functionName}({level.ToString(System.Globalization.CultureInfo.InvariantCulture)})({valueSql}, {conditionSql})";
    }

    private string TranslateQuantileDDIfAggregate(MethodCallExpression methodExpr)
    {
        var accuracy = ExtractConstantValue<double>(methodExpr.Arguments[1]);
        var level = ExtractConstantValue<double>(methodExpr.Arguments[2]);
        var selector = ExtractLambda(methodExpr.Arguments[3]);
        var predicate = ExtractLambda(methodExpr.Arguments[4]);
        var valueSql = TranslateExpression(selector.Body);
        var conditionSql = TranslateExpression(predicate.Body);
        var accuracyStr = accuracy.ToString(System.Globalization.CultureInfo.InvariantCulture);
        var levelStr = level.ToString(System.Globalization.CultureInfo.InvariantCulture);
        return $"quantileDDIf({accuracyStr}, {levelStr})({valueSql}, {conditionSql})";
    }

    private string TranslateMultiQuantileIfAggregate(string functionName, MethodCallExpression methodExpr)
    {
        var levels = ExtractConstantValue<double[]>(methodExpr.Arguments[1]);
        var selector = ExtractLambda(methodExpr.Arguments[2]);
        var predicate = ExtractLambda(methodExpr.Arguments[3]);
        var valueSql = TranslateExpression(selector.Body);
        var conditionSql = TranslateExpression(predicate.Body);
        var levelsStr = string.Join(", ", levels.Select(l => l.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        return $"{functionName}({levelsStr})({valueSql}, {conditionSql})";
    }

    private string TranslateSimpleClickHouseAggregate(string function, MethodCallExpression methodExpr)
    {
        // Pattern: Method(source, selector) where selector is the last argument
        var selector = ExtractLambda(methodExpr.Arguments[^1]);
        var innerSql = TranslateExpression(selector.Body);
        return $"{function}({innerSql})";
    }

    private string TranslateTwoSelectorAggregate(string function, MethodCallExpression methodExpr)
    {
        // Pattern: Method(source, argSelector, valSelector)
        var argSelector = ExtractLambda(methodExpr.Arguments[1]);
        var valSelector = ExtractLambda(methodExpr.Arguments[2]);
        var argSql = TranslateExpression(argSelector.Body);
        var valSql = TranslateExpression(valSelector.Body);
        return $"{function}({argSql}, {valSql})";
    }

    private string TranslateParametricQuantile(string functionName, MethodCallExpression methodExpr)
    {
        // Pattern: QuantileXxx(source, level, selector)
        var level = ExtractConstantValue<double>(methodExpr.Arguments[1]);
        var selector = ExtractLambda(methodExpr.Arguments[2]);
        var innerSql = TranslateExpression(selector.Body);
        return $"{functionName}({level.ToString(System.Globalization.CultureInfo.InvariantCulture)})({innerSql})";
    }

    private string TranslateGroupArray(MethodCallExpression methodExpr)
    {
        // Pattern: GroupArray(source, selector) or GroupArray(source, maxSize, selector)
        if (methodExpr.Arguments.Count == 2)
        {
            // Simple groupArray without limit
            var selector = ExtractLambda(methodExpr.Arguments[1]);
            return $"groupArray({TranslateExpression(selector.Body)})";
        }
        else
        {
            // groupArray with maxSize limit
            var maxSize = ExtractConstantValue<int>(methodExpr.Arguments[1]);
            var selector = ExtractLambda(methodExpr.Arguments[2]);
            return $"groupArray({maxSize})({TranslateExpression(selector.Body)})";
        }
    }

    private string TranslateTopK(MethodCallExpression methodExpr)
    {
        // Pattern: TopK(source, k, selector)
        var k = ExtractConstantValue<int>(methodExpr.Arguments[1]);
        var selector = ExtractLambda(methodExpr.Arguments[2]);
        var innerSql = TranslateExpression(selector.Body);
        return $"topK({k})({innerSql})";
    }

    private string TranslateQuantileDD(MethodCallExpression methodExpr)
    {
        // Pattern: QuantileDD(source, relativeAccuracy, level, selector)
        var accuracy = ExtractConstantValue<double>(methodExpr.Arguments[1]);
        var level = ExtractConstantValue<double>(methodExpr.Arguments[2]);
        var selector = ExtractLambda(methodExpr.Arguments[3]);
        var innerSql = TranslateExpression(selector.Body);
        var accuracyStr = accuracy.ToString(System.Globalization.CultureInfo.InvariantCulture);
        var levelStr = level.ToString(System.Globalization.CultureInfo.InvariantCulture);
        return $"quantileDD({accuracyStr}, {levelStr})({innerSql})";
    }

    private string TranslateMultiQuantile(string functionName, MethodCallExpression methodExpr)
    {
        // Pattern: Quantiles(source, levels[], selector)
        var levels = ExtractConstantValue<double[]>(methodExpr.Arguments[1]);
        var selector = ExtractLambda(methodExpr.Arguments[2]);
        var innerSql = TranslateExpression(selector.Body);
        var levelsStr = string.Join(", ", levels.Select(l => l.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        return $"{functionName}({levelsStr})({innerSql})";
    }

    private string TranslateTopKWeighted(MethodCallExpression methodExpr)
    {
        // Pattern: TopKWeighted(source, k, selector, weightSelector)
        var k = ExtractConstantValue<int>(methodExpr.Arguments[1]);
        var selector = ExtractLambda(methodExpr.Arguments[2]);
        var weightSelector = ExtractLambda(methodExpr.Arguments[3]);
        var valueSql = TranslateExpression(selector.Body);
        var weightSql = TranslateExpression(weightSelector.Body);
        return $"topKWeighted({k})({valueSql}, {weightSql})";
    }

    private static LambdaExpression ExtractLambda(Expression arg)
    {
        return arg switch
        {
            UnaryExpression { Operand: LambdaExpression lambda } => lambda,
            LambdaExpression lambda => lambda,
            _ => throw new NotSupportedException($"Cannot extract lambda from {arg.GetType().Name}")
        };
    }

    private static T ExtractConstantValue<T>(Expression arg)
    {
        // Handle constant directly
        if (arg is ConstantExpression constExpr && constExpr.Value is T value)
        {
            return value;
        }

        // Handle member access to a captured variable (closure)
        if (arg is MemberExpression memberExpr)
        {
            var container = memberExpr.Expression;
            if (container is ConstantExpression containerConst)
            {
                var field = memberExpr.Member as System.Reflection.FieldInfo;
                var prop = memberExpr.Member as System.Reflection.PropertyInfo;

                if (field != null)
                {
                    return (T)field.GetValue(containerConst.Value)!;
                }
                if (prop != null)
                {
                    return (T)prop.GetValue(containerConst.Value)!;
                }
            }
        }

        // Handle inline array initialization: new[] { 0.5, 0.9, 0.99 }
        if (arg is NewArrayExpression newArrayExpr && typeof(T).IsArray)
        {
            var elementType = typeof(T).GetElementType()!;
            var array = Array.CreateInstance(elementType, newArrayExpr.Expressions.Count);
            for (int i = 0; i < newArrayExpr.Expressions.Count; i++)
            {
                if (newArrayExpr.Expressions[i] is ConstantExpression elemConst)
                {
                    array.SetValue(Convert.ChangeType(elemConst.Value, elementType), i);
                }
                else
                {
                    throw new NotSupportedException(
                        $"Array element at index {i} is not a constant expression.");
                }
            }
            return (T)(object)array;
        }

        throw new NotSupportedException($"Cannot extract constant value of type {typeof(T).Name} from {arg.GetType().Name}");
    }

    private string TranslateConstant(ConstantExpression constExpr)
    {
        // Typed null: C# emits Constant(null, typeof(T)) for default(T) when T
        // is a reference type (e.g. default(string)). Bare NULL would be typed
        // as Nullable(Nothing) which ClickHouse rejects in MV target columns,
        // so emit a typed zero-value default keyed off the declared CLR type.
        if (constExpr.Value is null)
        {
            return TypedNullDefault(constExpr.Type);
        }

        return constExpr.Value switch
        {
            string s => $"'{s.Replace("'", "''")}'",
            bool b => b ? "1" : "0",
            sbyte sb => $"toInt8({sb})",
            byte b => $"toUInt8({b})",
            ulong ul => ul.ToString(),
            long l => l.ToString(),
            DateTime dt => $"toDateTime64('{dt:yyyy-MM-dd HH:mm:ss.fff}', 3)",
            // Guid implements IFormattable as bare uuid text; ClickHouse needs
            // the literal quoted (or wrapped in toUUID). Place this before the
            // IFormattable arm so Guid doesn't fall through.
            Guid g => $"toUUID('{g}')",
            IFormattable f => f.ToString(null, System.Globalization.CultureInfo.InvariantCulture),
            _ => constExpr.Value.ToString() ?? "NULL"
        };
    }

    private static string TypedNullDefault(Type type)
    {
        var nonNullable = Nullable.GetUnderlyingType(type) ?? type;
        if (nonNullable == typeof(string)) return "''";
        if (nonNullable == typeof(byte[])) return "''";
        return nonNullable.Name switch
        {
            "Int64" or "Int32" or "Int16" or "SByte" => "0",
            "UInt64" or "UInt32" or "UInt16" or "Byte" => "0",
            "Double" or "Single" or "Decimal" => "0.0",
            "Boolean" => "0",
            _ => "NULL",
        };
    }

    private string TranslateBinary(BinaryExpression binaryExpr)
    {
        // Defensive null-checks against a join-bound row parameter (e.g.
        // `c == null` after a LEFT JOIN where `c` is the inner row). ClickHouse
        // fills unmatched LEFT JOIN rows with type-defaults rather than NULL,
        // so the row-vs-null comparison is constant — emit 0/1 directly.
        if (binaryExpr.NodeType is ExpressionType.Equal or ExpressionType.NotEqual
            && IsRowBoundParameterNullCheck(binaryExpr.Left, binaryExpr.Right, out _)
            || binaryExpr.NodeType is ExpressionType.Equal or ExpressionType.NotEqual
            && IsRowBoundParameterNullCheck(binaryExpr.Right, binaryExpr.Left, out _))
        {
            return binaryExpr.NodeType == ExpressionType.Equal ? "0" : "1";
        }

        var left = TranslateExpression(binaryExpr.Left);
        var right = TranslateExpression(binaryExpr.Right);

        var op = binaryExpr.NodeType switch
        {
            ExpressionType.Add => "+",
            ExpressionType.Subtract => "-",
            ExpressionType.Multiply => "*",
            ExpressionType.Divide => "/",
            ExpressionType.Modulo => "%",
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "!=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.AndAlso => "AND",
            ExpressionType.OrElse => "OR",
            _ => throw new NotSupportedException($"Binary operator {binaryExpr.NodeType} is not supported.")
        };

        return $"({left} {op} {right})";
    }

    private bool IsRowBoundParameterNullCheck(Expression maybeParam, Expression maybeNull, out string paramName)
    {
        paramName = string.Empty;
        if (maybeParam is not ParameterExpression pe) return false;
        if (maybeNull is not ConstantExpression { Value: null }) return false;
        if (!_paramOverrides.ContainsKey(pe.Name!)) return false;
        paramName = pe.Name!;
        return true;
    }

    private string TranslateUnary(UnaryExpression unaryExpr)
    {
        if (unaryExpr.NodeType == ExpressionType.Convert ||
            unaryExpr.NodeType == ExpressionType.ConvertChecked)
        {
            // Often just unwrap conversions
            return TranslateExpression(unaryExpr.Operand);
        }

        if (unaryExpr.NodeType == ExpressionType.Not)
        {
            return $"NOT ({TranslateExpression(unaryExpr.Operand)})";
        }

        if (unaryExpr.NodeType == ExpressionType.Negate)
        {
            return $"-({TranslateExpression(unaryExpr.Operand)})";
        }

        throw new NotSupportedException($"Unary operator {unaryExpr.NodeType} is not supported.");
    }

    private string TranslateConditional(ConditionalExpression condExpr)
    {
        var test = TranslateExpression(condExpr.Test);
        var ifTrue = TranslateExpression(condExpr.IfTrue);
        var ifFalse = TranslateExpression(condExpr.IfFalse);

        return $"if({test}, {ifTrue}, {ifFalse})";
    }

    private string GetColumnName(MemberInfo member)
    {
        // Try to get the column name from EF Core model
        if (_sourceEntityType != null)
        {
            var property = _sourceEntityType.FindProperty(member.Name);
            if (property != null)
            {
                var columnName = property.GetColumnName() ?? member.Name;
                return QuoteIdentifier(columnName);
            }
        }

        // Fall back to member name
        return QuoteIdentifier(member.Name);
    }

    private static string QuoteIdentifier(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\\\"")}\"";
    }
}
