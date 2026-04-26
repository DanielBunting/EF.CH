using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Views;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Features;

public class ViewTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:25.6")
        .Build();

    public async Task InitializeAsync() => await _container.StartAsync();
    public async Task DisposeAsync() => await _container.DisposeAsync();

    private string GetConnectionString() => _container.GetConnectionString();

    #region SQL-shape unit tests

    [Fact]
    public void BuildSelectSql_PlainName_QuotesViewName()
    {
        var sql = ClickHouseViewExtensions.BuildSelectSql("active_users", schema: null);
        Assert.Equal("SELECT * FROM \"active_users\"", sql);
    }

    [Fact]
    public void BuildSelectSql_WithSchema_QuotesBoth()
    {
        var sql = ClickHouseViewExtensions.BuildSelectSql("active_users", "analytics");
        Assert.Equal("SELECT * FROM \"analytics\".\"active_users\"", sql);
    }

    [Fact]
    public void BuildSelectSql_EscapesEmbeddedQuotes()
    {
        var sql = ClickHouseViewExtensions.BuildSelectSql("view\"name", schema: null);
        Assert.Contains("\"view\"\"name\"", sql);
    }

    [Fact]
    public void GenerateDropViewSql_BasicForm()
    {
        var sql = ViewSqlGenerator.GenerateDropViewSql("v");
        Assert.Equal("DROP VIEW IF EXISTS \"v\"", sql);
    }

    [Fact]
    public void GenerateDropViewSql_WithSchemaAndCluster()
    {
        var sql = ViewSqlGenerator.GenerateDropViewSql("v", schema: "s", ifExists: false, onCluster: "main");
        Assert.Equal("DROP VIEW \"s\".\"v\" ON CLUSTER main", sql);
    }

    [Fact]
    public void GenerateCreateViewSql_RawSelect_BasicForm()
    {
        var metadata = new ViewMetadataBase
        {
            ViewName = "v",
            ResultType = typeof(object),
            RawSelectSql = "SELECT 1"
        };

        var sql = ViewSqlGenerator.GenerateCreateViewSql(EmptyModel(), metadata);
        Assert.Equal("CREATE VIEW \"v\" AS\nSELECT 1", sql);
    }

    [Fact]
    public void GenerateCreateViewSql_AllOptions_CombinesCorrectly()
    {
        var metadata = new ViewMetadataBase
        {
            ViewName = "v",
            ResultType = typeof(object),
            RawSelectSql = "SELECT 1",
            OrReplace = true,
            OnCluster = "main",
            Schema = "s"
        };

        var sql = ViewSqlGenerator.GenerateCreateViewSql(EmptyModel(), metadata);
        Assert.Equal("CREATE OR REPLACE VIEW \"s\".\"v\" ON CLUSTER main AS\nSELECT 1", sql);
    }

    [Fact]
    public void GenerateCreateViewSql_IfNotExists_EmitsClause()
    {
        var metadata = new ViewMetadataBase
        {
            ViewName = "v",
            ResultType = typeof(object),
            RawSelectSql = "SELECT 1",
            IfNotExists = true
        };

        var sql = ViewSqlGenerator.GenerateCreateViewSql(EmptyModel(), metadata);
        Assert.Equal("CREATE VIEW IF NOT EXISTS \"v\" AS\nSELECT 1", sql);
    }

    [Fact]
    public void GenerateCreateViewSql_OrReplaceAndIfNotExists_Throws()
    {
        var metadata = new ViewMetadataBase
        {
            ViewName = "v",
            ResultType = typeof(object),
            RawSelectSql = "SELECT 1",
            OrReplace = true,
            IfNotExists = true
        };

        Assert.Throws<InvalidOperationException>(
            () => ViewSqlGenerator.GenerateCreateViewSql(EmptyModel(), metadata));
    }

    private static Microsoft.EntityFrameworkCore.Metadata.IModel EmptyModel()
    {
        var options = new DbContextOptionsBuilder<EmptyContext>()
            .UseClickHouse("Host=localhost;Port=9000;Database=default;Username=default;Password=")
            .Options;
        using var ctx = new EmptyContext(options);
        return ctx.Model;
    }

    private sealed class EmptyContext : DbContext
    {
        public EmptyContext(DbContextOptions<EmptyContext> options) : base(options) { }
    }

    #endregion

    #region Configuration validation

    [Fact]
    public void ViewConfiguration_WithoutSelect_FailsValidation()
    {
        var cfg = new ViewConfiguration<ActiveUserView, ActiveUser>();
        var errors = cfg.Validate().ToList();
        Assert.Contains(errors, e => e.Contains("Select()"));
    }

    [Fact]
    public void ViewConfiguration_OrReplaceAndIfNotExists_FailsValidation()
    {
        var cfg = new ViewConfiguration<ActiveUserView, ActiveUser>()
            .Select(u => new ActiveUserView { UserId = u.UserId, Name = u.Name })
            .OrReplace()
            .IfNotExists();

        var errors = cfg.Validate().ToList();
        Assert.Contains(errors, e => e.Contains("mutually exclusive"));
    }

    #endregion

    #region Annotation tests

    [Fact]
    public void HasView_SetsCorrectAnnotations()
    {
        using var context = CreateHasViewContext();
        var entityType = context.Model.FindEntityType(typeof(BasicView));

        Assert.NotNull(entityType);
        Assert.True((bool?)entityType.FindAnnotation(ClickHouseAnnotationNames.View)?.Value);
        Assert.Equal("basic_view", entityType.FindAnnotation(ClickHouseAnnotationNames.ViewName)?.Value);
    }

    [Fact]
    public void AsView_StoresFluentMetadata()
    {
        using var context = CreateFluentViewContext();
        var entityType = context.Model.FindEntityType(typeof(ActiveUserView));

        Assert.NotNull(entityType);
        Assert.True((bool?)entityType.FindAnnotation(ClickHouseAnnotationNames.View)?.Value);

        var metadata = entityType.FindAnnotation(ClickHouseAnnotationNames.ViewMetadata)?.Value as ViewMetadataBase;
        Assert.NotNull(metadata);
        Assert.Equal("active_users", metadata.ViewName);
        Assert.Equal(typeof(ActiveUser), metadata.SourceType);
        Assert.NotNull(metadata.ProjectionExpression);
        Assert.NotNull(metadata.WhereExpressions);
        Assert.Single(metadata.WhereExpressions);
        Assert.True(metadata.OrReplace);
    }

    [Fact]
    public void AsViewRaw_StoresRawSelectSql()
    {
        using var context = CreateRawViewContext();
        var entityType = context.Model.FindEntityType(typeof(RawSampleView));

        Assert.NotNull(entityType);
        var metadata = entityType.FindAnnotation(ClickHouseAnnotationNames.ViewMetadata)?.Value as ViewMetadataBase;
        Assert.NotNull(metadata);
        Assert.Equal("raw_sample", metadata.ViewName);
        Assert.NotNull(metadata.RawSelectSql);
        Assert.Contains("SELECT", metadata.RawSelectSql);
        Assert.True(metadata.IfNotExists);
    }

    [Fact]
    public void AsViewDeferred_SetsDeferredAnnotation()
    {
        using var context = CreateRawViewContext();
        var entityType = context.Model.FindEntityType(typeof(RawSampleView));

        Assert.True((bool?)entityType?.FindAnnotation(ClickHouseAnnotationNames.ViewDeferred)?.Value);
    }

    [Fact]
    public void GetViewSql_ReturnsGeneratedDdl()
    {
        using var context = CreateFluentViewContext();
        var sql = context.Database.GetViewSql<ActiveUserView>();

        Assert.Contains("CREATE OR REPLACE VIEW \"active_users\"", sql);
        Assert.Contains("AS \"UserId\"", sql);
        Assert.Contains("AS \"Name\"", sql);
        Assert.Contains("FROM \"users\"", sql);
        Assert.Contains("WHERE", sql);
    }

    #endregion

    #region Integration tests

    [Fact]
    public async Task HasView_RoundTrip_QueryViaFromView()
    {
        await using var context = CreateHasViewContext();

        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS source_rows (
                id UInt64,
                name String
            ) ENGINE = MergeTree() ORDER BY id");

        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO source_rows (id, name) VALUES (1, 'one'), (2, 'two')");

        await context.Database.CreateViewAsync(
            "basic_view",
            "SELECT id AS \"Id\", name AS \"Name\" FROM source_rows",
            ifNotExists: true);

        var rows = await context.FromView<BasicView>("basic_view")
            .OrderBy(r => r.Id)
            .ToListAsync();

        Assert.Equal(2, rows.Count);
        Assert.Equal("one", rows[0].Name);
        Assert.Equal("two", rows[1].Name);
    }

    [Fact]
    public async Task AsView_EnsureViewAsync_CreatesAndQueries()
    {
        await using var context = CreateFluentViewContext();

        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS users (
                user_id UInt64,
                name String,
                is_active UInt8
            ) ENGINE = MergeTree() ORDER BY user_id");

        await context.Database.EnsureViewAsync<ActiveUserView>();

        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO users (user_id, name, is_active) VALUES
            (1, 'Alice', 1),
            (2, 'Bob', 0),
            (3, 'Carol', 1)");

        var rows = await context.Set<ActiveUserView>()
            .OrderBy(u => u.UserId)
            .ToListAsync();

        Assert.Equal(2, rows.Count);
        Assert.Equal("Alice", rows[0].Name);
        Assert.Equal("Carol", rows[1].Name);
    }

    [Fact]
    public async Task AsView_OrReplace_SwapsViewBody()
    {
        await using var context = CreateFluentViewContext();

        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS users (
                user_id UInt64,
                name String,
                is_active UInt8
            ) ENGINE = MergeTree() ORDER BY user_id");

        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO users (user_id, name, is_active) VALUES (1, 'Alice', 1)");

        await context.Database.EnsureViewAsync<ActiveUserView>();

        var firstSql = context.Database.GetViewSql<ActiveUserView>();
        Assert.Contains("OR REPLACE", firstSql);

        // Re-running EnsureViewAsync should succeed because OR REPLACE is set on the view config.
        await context.Database.EnsureViewAsync<ActiveUserView>();

        var rows = await context.Set<ActiveUserView>().ToListAsync();
        Assert.Single(rows);
    }

    [Fact]
    public async Task EnsureViewsAsync_CreatesAllNonDeferredViews()
    {
        await using var context = CreateMixedViewContext();

        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS users (
                user_id UInt64,
                name String,
                is_active UInt8
            ) ENGINE = MergeTree() ORDER BY user_id");

        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS source_rows (
                id UInt64,
                name String
            ) ENGINE = MergeTree() ORDER BY id");

        var created = await context.Database.EnsureViewsAsync();

        // ActiveUserView (AsView) + RawSampleView is deferred, so it's skipped.
        Assert.Equal(1, created);
    }

    [Fact]
    public async Task CreateViewAsync_OrReplaceAndIfNotExists_Throws()
    {
        await using var context = CreateHasViewContext();

        await Assert.ThrowsAsync<ArgumentException>(() =>
            context.Database.CreateViewAsync(
                "v",
                "SELECT 1",
                ifNotExists: true,
                orReplace: true));
    }

    [Fact]
    public async Task DropViewAsync_DropsThenIdempotent()
    {
        await using var context = CreateHasViewContext();

        await context.Database.CreateViewAsync("temp_v", "SELECT 1 AS x", ifNotExists: true);
        await context.Database.DropViewAsync("temp_v");
        // Second drop with ifExists=true should not throw.
        await context.Database.DropViewAsync("temp_v", ifExists: true);
    }

    #endregion

    #region Migration builder

    [Fact]
    public void MigrationBuilder_CreateView_OrReplaceAndIfNotExists_Throws()
    {
        var ex = Assert.Throws<ArgumentException>(() =>
        {
            var mb = new Microsoft.EntityFrameworkCore.Migrations.MigrationBuilder("ClickHouse");
            mb.CreateView("v", "SELECT 1", ifNotExists: true, orReplace: true);
        });

        Assert.Contains("OR REPLACE", ex.Message);
    }

    [Fact]
    public void MigrationBuilder_CreateView_HappyPath_EmitsCorrectSql()
    {
        var mb = new Microsoft.EntityFrameworkCore.Migrations.MigrationBuilder("ClickHouse");
        mb.CreateView(
            "v",
            "SELECT 1",
            orReplace: true,
            onCluster: "main",
            schema: "s");

        var op = Assert.Single(mb.Operations);
        var sqlOp = Assert.IsType<Microsoft.EntityFrameworkCore.Migrations.Operations.SqlOperation>(op);
        Assert.Equal("CREATE OR REPLACE VIEW \"s\".\"v\" ON CLUSTER main AS\nSELECT 1", sqlOp.Sql);
    }

    [Fact]
    public void MigrationBuilder_DropView_HappyPath_EmitsCorrectSql()
    {
        var mb = new Microsoft.EntityFrameworkCore.Migrations.MigrationBuilder("ClickHouse");
        mb.DropView("v", ifExists: false, onCluster: "main", schema: "s");

        var sqlOp = Assert.IsType<Microsoft.EntityFrameworkCore.Migrations.Operations.SqlOperation>(
            Assert.Single(mb.Operations));
        Assert.Equal("DROP VIEW \"s\".\"v\" ON CLUSTER main", sqlOp.Sql);
    }

    [Fact]
    public void MigrationBuilder_DropView_DefaultIsIfExists()
    {
        var mb = new Microsoft.EntityFrameworkCore.Migrations.MigrationBuilder("ClickHouse");
        mb.DropView("v");

        var sqlOp = Assert.IsType<Microsoft.EntityFrameworkCore.Migrations.Operations.SqlOperation>(
            Assert.Single(mb.Operations));
        Assert.Equal("DROP VIEW IF EXISTS \"v\"", sqlOp.Sql);
    }

    #endregion

    #region WHERE clause translator

    // NOTE: WhereOpsRow is not registered in the EmptyModel used by these
    // unit tests, so the visitor falls back to snake_case column names.

    [Fact]
    public void WhereTranslation_AllBinaryOperators_RenderCorrectly()
    {
        AssertWhere<WhereOpsRow>(r => r.Score == 1, "(\"score\" = 1)");
        AssertWhere<WhereOpsRow>(r => r.Score != 2, "(\"score\" != 2)");
        AssertWhere<WhereOpsRow>(r => r.Score > 3, "(\"score\" > 3)");
        AssertWhere<WhereOpsRow>(r => r.Score >= 4, "(\"score\" >= 4)");
        AssertWhere<WhereOpsRow>(r => r.Score < 5, "(\"score\" < 5)");
        AssertWhere<WhereOpsRow>(r => r.Score <= 6, "(\"score\" <= 6)");
    }

    [Fact]
    public void WhereTranslation_AndOrNot_RenderCorrectly()
    {
        AssertWhere<WhereOpsRow>(
            r => r.IsActive && r.Score > 0,
            "(\"is_active\" AND (\"score\" > 0))");
        AssertWhere<WhereOpsRow>(
            r => r.IsActive || r.Score == 0,
            "(\"is_active\" OR (\"score\" = 0))");
        AssertWhere<WhereOpsRow>(r => !r.IsActive, "NOT \"is_active\"");
    }

    [Fact]
    public void WhereTranslation_StringConstant_IsEscaped()
    {
        AssertWhere<WhereOpsRow>(r => r.Name == "Bob's", "(\"name\" = 'Bob\\'s')");
    }

    [Fact]
    public void WhereTranslation_CapturedConstant_IsEvaluated()
    {
        var threshold = 42;
        AssertWhere<WhereOpsRow>(r => r.Score > threshold, "(\"score\" > 42)");
    }

    [Fact]
    public void WhereTranslation_DateTimeConstant_FormatsAsLiteral()
    {
        var cutoff = new DateTime(2024, 5, 1, 12, 0, 0);
        AssertWhere<WhereOpsRow>(r => r.LastSeen >= cutoff, "(\"last_seen\" >= '2024-05-01 12:00:00')");
    }

    [Fact]
    public void WhereTranslation_MultipleClauses_JoinedWithAnd()
    {
        var metadata = new ViewMetadataBase
        {
            ViewName = "v",
            ResultType = typeof(WhereOpsRow),
            SourceType = typeof(WhereOpsRow),
            SourceTable = "rows",
            ProjectionExpression = (System.Linq.Expressions.Expression<Func<WhereOpsRow, WhereOpsRow>>)
                (r => new WhereOpsRow { Name = r.Name, Score = r.Score, IsActive = r.IsActive, LastSeen = r.LastSeen }),
            WhereExpressions = new()
            {
                (System.Linq.Expressions.Expression<Func<WhereOpsRow, bool>>)(r => r.IsActive),
                (System.Linq.Expressions.Expression<Func<WhereOpsRow, bool>>)(r => r.Score > 10)
            }
        };

        var sql = ViewSqlGenerator.GenerateCreateViewSql(EmptyModel(), metadata);
        Assert.Contains("WHERE \"is_active\"\n  AND (\"score\" > 10)", sql);
    }

    private static void AssertWhere<TSource>(
        System.Linq.Expressions.Expression<Func<TSource, bool>> predicate,
        string expectedFragment) where TSource : class
    {
        var metadata = new ViewMetadataBase
        {
            ViewName = "v",
            ResultType = typeof(TSource),
            SourceType = typeof(TSource),
            SourceTable = "rows",
            ProjectionExpression = BuildIdentityProjection<TSource>(),
            WhereExpressions = new() { predicate }
        };

        var sql = ViewSqlGenerator.GenerateCreateViewSql(EmptyModel(), metadata);
        Assert.Contains(expectedFragment, sql);
    }

    private static System.Linq.Expressions.LambdaExpression BuildIdentityProjection<TSource>()
    {
        var param = System.Linq.Expressions.Expression.Parameter(typeof(TSource), "r");
        var firstProp = typeof(TSource).GetProperties()[0];
        var binding = System.Linq.Expressions.Expression.Bind(
            firstProp,
            System.Linq.Expressions.Expression.Property(param, firstProp));
        var newExpr = System.Linq.Expressions.Expression.New(typeof(TSource));
        var body = System.Linq.Expressions.Expression.MemberInit(newExpr, binding);
        return System.Linq.Expressions.Expression.Lambda(body, param);
    }

    #endregion

    #region Select projection

    [Fact]
    public void Projection_MultipleProperties_AllRendered()
    {
        var metadata = new ViewMetadataBase
        {
            ViewName = "v",
            ResultType = typeof(WhereOpsRow),
            SourceType = typeof(WhereOpsRow),
            SourceTable = "rows",
            ProjectionExpression = (System.Linq.Expressions.Expression<Func<WhereOpsRow, WhereOpsRow>>)
                (r => new WhereOpsRow { Name = r.Name, Score = r.Score, IsActive = r.IsActive, LastSeen = r.LastSeen })
        };

        var sql = ViewSqlGenerator.GenerateCreateViewSql(EmptyModel(), metadata);
        Assert.Contains("\"name\" AS \"Name\"", sql);
        Assert.Contains("\"score\" AS \"Score\"", sql);
        Assert.Contains("\"is_active\" AS \"IsActive\"", sql);
        Assert.Contains("\"last_seen\" AS \"LastSeen\"", sql);
    }

    [Fact]
    public void Projection_SourceNotInModel_FallsBackToSnakeCase()
    {
        // WhereOpsRow is not registered in the empty model used here, so the
        // generator must fall back to PascalCase → snake_case conversion.
        var metadata = new ViewMetadataBase
        {
            ViewName = "v",
            ResultType = typeof(WhereOpsRow),
            SourceType = typeof(WhereOpsRow),
            SourceTable = "rows",
            ProjectionExpression = (System.Linq.Expressions.Expression<Func<WhereOpsRow, WhereOpsRow>>)
                (r => new WhereOpsRow { LastSeen = r.LastSeen })
        };

        var sql = ViewSqlGenerator.GenerateCreateViewSql(EmptyModel(), metadata);
        Assert.Contains("\"last_seen\" AS \"LastSeen\"", sql);
    }

    [Fact]
    public void Projection_NonMemberInitBody_Throws()
    {
        var metadata = new ViewMetadataBase
        {
            ViewName = "v",
            ResultType = typeof(WhereOpsRow),
            SourceType = typeof(WhereOpsRow),
            SourceTable = "rows",
            ProjectionExpression = (System.Linq.Expressions.Expression<Func<WhereOpsRow, WhereOpsRow>>)(r => r)
        };

        Assert.Throws<InvalidOperationException>(
            () => ViewSqlGenerator.GenerateCreateViewSql(EmptyModel(), metadata));
    }

    #endregion

    #region GetViewSql shape coverage

    [Fact]
    public void GenerateCreateViewSql_OnCluster_AppearsInDdl()
    {
        var metadata = new ViewMetadataBase
        {
            ViewName = "v",
            ResultType = typeof(object),
            RawSelectSql = "SELECT 1",
            OnCluster = "main"
        };

        var sql = ViewSqlGenerator.GenerateCreateViewSql(EmptyModel(), metadata);
        Assert.Equal("CREATE VIEW \"v\" ON CLUSTER main AS\nSELECT 1", sql);
    }

    [Fact]
    public void GenerateCreateViewSql_SchemaOnly_QualifiesViewName()
    {
        var metadata = new ViewMetadataBase
        {
            ViewName = "v",
            ResultType = typeof(object),
            RawSelectSql = "SELECT 1",
            Schema = "analytics"
        };

        var sql = ViewSqlGenerator.GenerateCreateViewSql(EmptyModel(), metadata);
        Assert.StartsWith("CREATE VIEW \"analytics\".\"v\" AS", sql);
    }

    #endregion

    #region Database-extension error paths

    [Fact]
    public void EnsureViewAsync_HasViewOnly_Throws()
    {
        // BasicView is configured with HasView() only — no ViewMetadata annotation.
        using var context = CreateHasViewContext();
        var ex = Assert.Throws<InvalidOperationException>(
            () => context.Database.EnsureViewAsync<BasicView>().GetAwaiter().GetResult());
        Assert.Contains("AsView", ex.Message);
    }

    [Fact]
    public void EnsureViewAsync_UnknownEntity_Throws()
    {
        using var context = CreateHasViewContext();
        Assert.Throws<InvalidOperationException>(
            () => context.Database.EnsureViewAsync<UnknownEntity>().GetAwaiter().GetResult());
    }

    [Fact]
    public void GetViewSql_HasViewOnly_Throws()
    {
        using var context = CreateHasViewContext();
        Assert.Throws<InvalidOperationException>(
            () => context.Database.GetViewSql<BasicView>());
    }

    #endregion

    #region FromView schema integration

    [Fact]
    public async Task FromView_WithSchema_QueriesQualifiedView()
    {
        await using var context = CreateHasViewContext();

        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS schema_rows (
                id UInt64,
                name String
            ) ENGINE = MergeTree() ORDER BY id");
        await context.Database.ExecuteSqlRawAsync(
            "INSERT INTO schema_rows (id, name) VALUES (1, 'one'), (2, 'two')");

        // ClickHouse always has a `default` database — emit a CREATE VIEW that
        // qualifies the view name with it and verify FromView<T>(name, schema)
        // composes the same identifier.
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE VIEW IF NOT EXISTS ""default"".""schema_view"" AS
            SELECT id AS ""Id"", name AS ""Name"" FROM schema_rows");

        var rows = await context.FromView<BasicView>("schema_view", schema: "default")
            .OrderBy(r => r.Id)
            .ToListAsync();

        Assert.Equal(2, rows.Count);
        Assert.Equal("one", rows[0].Name);
    }

    #endregion

    private HasViewContext CreateHasViewContext()
    {
        var options = new DbContextOptionsBuilder<HasViewContext>()
            .UseClickHouse(GetConnectionString())
            .Options;
        return new HasViewContext(options);
    }

    private FluentViewContext CreateFluentViewContext()
    {
        var options = new DbContextOptionsBuilder<FluentViewContext>()
            .UseClickHouse(GetConnectionString())
            .Options;
        return new FluentViewContext(options);
    }

    private RawViewContext CreateRawViewContext()
    {
        var options = new DbContextOptionsBuilder<RawViewContext>()
            .UseClickHouse(GetConnectionString())
            .Options;
        return new RawViewContext(options);
    }

    private MixedViewContext CreateMixedViewContext()
    {
        var options = new DbContextOptionsBuilder<MixedViewContext>()
            .UseClickHouse(GetConnectionString())
            .Options;
        return new MixedViewContext(options);
    }
}

#region Test entities

public class BasicView
{
    public ulong Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class ActiveUser
{
    public ulong UserId { get; set; }
    public string Name { get; set; } = string.Empty;
    public bool IsActive { get; set; }
}

public class ActiveUserView
{
    public ulong UserId { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class RawSampleView
{
    public ulong Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class WhereOpsRow
{
    public string Name { get; set; } = string.Empty;
    public int Score { get; set; }
    public bool IsActive { get; set; }
    public DateTime LastSeen { get; set; }
}

public class UnknownEntity
{
    public ulong Id { get; set; }
}

#endregion

#region Test contexts

public class HasViewContext : DbContext
{
    public HasViewContext(DbContextOptions<HasViewContext> options) : base(options) { }
    public DbSet<BasicView> BasicViews => Set<BasicView>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<BasicView>(e => e.HasView("basic_view"));
    }
}

public class FluentViewContext : DbContext
{
    public FluentViewContext(DbContextOptions<FluentViewContext> options) : base(options) { }
    public DbSet<ActiveUser> Users => Set<ActiveUser>();
    public DbSet<ActiveUserView> ActiveUsers => Set<ActiveUserView>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ActiveUser>(entity =>
        {
            entity.ToTable("users");
            entity.HasKey(e => e.UserId);
            entity.UseMergeTree(e => e.UserId);
            entity.Property(e => e.UserId).HasColumnName("user_id");
            entity.Property(e => e.Name).HasColumnName("name");
            entity.Property(e => e.IsActive).HasColumnName("is_active");
        });

        modelBuilder.Entity<ActiveUserView>(entity =>
        {
            entity.AsView<ActiveUserView, ActiveUser>(cfg => cfg
                .HasName("active_users")
                .FromTable()
                .Select(u => new ActiveUserView
                {
                    UserId = u.UserId,
                    Name = u.Name
                })
                .Where(u => u.IsActive)
                .OrReplace());
        });
    }
}

public class RawViewContext : DbContext
{
    public RawViewContext(DbContextOptions<RawViewContext> options) : base(options) { }
    public DbSet<RawSampleView> RawSamples => Set<RawSampleView>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<RawSampleView>(entity =>
        {
            entity.AsViewRaw(
                viewName: "raw_sample",
                selectSql: "SELECT id AS \"Id\", name AS \"Name\" FROM source_rows",
                ifNotExists: true);
            entity.AsViewDeferred();
        });
    }
}

public class MixedViewContext : DbContext
{
    public MixedViewContext(DbContextOptions<MixedViewContext> options) : base(options) { }
    public DbSet<ActiveUser> Users => Set<ActiveUser>();
    public DbSet<ActiveUserView> ActiveUsers => Set<ActiveUserView>();
    public DbSet<RawSampleView> RawSamples => Set<RawSampleView>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ActiveUser>(entity =>
        {
            entity.ToTable("users");
            entity.HasKey(e => e.UserId);
            entity.UseMergeTree(e => e.UserId);
            entity.Property(e => e.UserId).HasColumnName("user_id");
            entity.Property(e => e.Name).HasColumnName("name");
            entity.Property(e => e.IsActive).HasColumnName("is_active");
        });

        modelBuilder.Entity<ActiveUserView>(entity =>
        {
            entity.AsView<ActiveUserView, ActiveUser>(cfg => cfg
                .HasName("active_users")
                .FromTable()
                .Select(u => new ActiveUserView { UserId = u.UserId, Name = u.Name })
                .Where(u => u.IsActive)
                .OrReplace());
        });

        modelBuilder.Entity<RawSampleView>(entity =>
        {
            entity.AsViewRaw(
                viewName: "raw_sample",
                selectSql: "SELECT id AS \"Id\", name AS \"Name\" FROM source_rows",
                ifNotExists: true);
            entity.AsViewDeferred();
        });
    }
}

#endregion
