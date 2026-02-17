using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;
using EfClass = Microsoft.EntityFrameworkCore.EF;

namespace EF.CH.Tests.Features;

/// <summary>
/// Tests for ClickHouse full-text search function SQL generation.
/// These tests verify that LINQ expressions using text search DbFunctions
/// are correctly translated to ClickHouse SQL.
/// </summary>
public class TextSearchTests
{
    #region Token Function Tests

    [Fact]
    public void HasToken_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Where(l => EfClass.Functions.HasToken(l.Message, "error"));

        var sql = query.ToQueryString();

        Assert.Contains("hasToken(", sql);
        Assert.Contains("error", sql);
    }

    [Fact]
    public void HasTokenCaseInsensitive_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Where(l => EfClass.Functions.HasTokenCaseInsensitive(l.Message, "error"));

        var sql = query.ToQueryString();

        Assert.Contains("hasTokenCaseInsensitive(", sql);
    }

    [Fact]
    public void HasAnyToken_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Where(l => EfClass.Functions.HasAnyToken(l.Message, new[] { "error", "warn" }));

        var sql = query.ToQueryString();

        Assert.Contains("hasAnyToken(", sql);
    }

    [Fact]
    public void HasAllTokens_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Where(l => EfClass.Functions.HasAllTokens(l.Message, new[] { "error", "warn" }));

        var sql = query.ToQueryString();

        Assert.Contains("hasAllTokens(", sql);
    }

    #endregion

    #region Multi-Search Function Tests

    [Fact]
    public void MultiSearchAny_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Where(l => EfClass.Functions.MultiSearchAny(l.Message, new[] { "error", "exception" }));

        var sql = query.ToQueryString();

        Assert.Contains("multiSearchAny(", sql);
    }

    [Fact]
    public void MultiSearchAnyCaseInsensitive_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Where(l => EfClass.Functions.MultiSearchAnyCaseInsensitive(l.Message, new[] { "error", "exception" }));

        var sql = query.ToQueryString();

        Assert.Contains("multiSearchAnyCaseInsensitive(", sql);
    }

    [Fact]
    public void MultiSearchAll_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Where(l => EfClass.Functions.MultiSearchAll(l.Message, new[] { "error", "exception" }));

        var sql = query.ToQueryString();

        Assert.Contains("multiSearchAllPositions(", sql);
    }

    [Fact]
    public void MultiSearchAllCaseInsensitive_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Where(l => EfClass.Functions.MultiSearchAllCaseInsensitive(l.Message, new[] { "error", "exception" }));

        var sql = query.ToQueryString();

        Assert.Contains("multiSearchAllPositionsCaseInsensitive(", sql);
    }

    [Fact]
    public void MultiSearchFirstPosition_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Select(l => new
            {
                l.Id,
                Position = EfClass.Functions.MultiSearchFirstPosition(l.Message, new[] { "error", "warn" })
            });

        var sql = query.ToQueryString();

        Assert.Contains("multiSearchFirstPosition(", sql);
    }

    [Fact]
    public void MultiSearchFirstIndex_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Select(l => new
            {
                l.Id,
                Index = EfClass.Functions.MultiSearchFirstIndex(l.Message, new[] { "error", "warn" })
            });

        var sql = query.ToQueryString();

        Assert.Contains("multiSearchFirstIndex(", sql);
    }

    [Fact]
    public void MultiSearchFirstPositionCaseInsensitive_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Select(l => new
            {
                l.Id,
                Position = EfClass.Functions.MultiSearchFirstPositionCaseInsensitive(l.Message, new[] { "error", "warn" })
            });

        var sql = query.ToQueryString();

        Assert.Contains("multiSearchFirstPositionCaseInsensitive(", sql);
    }

    [Fact]
    public void MultiSearchFirstIndexCaseInsensitive_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Select(l => new
            {
                l.Id,
                Index = EfClass.Functions.MultiSearchFirstIndexCaseInsensitive(l.Message, new[] { "error", "warn" })
            });

        var sql = query.ToQueryString();

        Assert.Contains("multiSearchFirstIndexCaseInsensitive(", sql);
    }

    #endregion

    #region N-gram Function Tests

    [Fact]
    public void NgramSearch_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Select(l => new
            {
                l.Id,
                Score = EfClass.Functions.NgramSearch(l.Message, "error")
            });

        var sql = query.ToQueryString();

        Assert.Contains("ngramSearch(", sql);
    }

    [Fact]
    public void NgramSearchCaseInsensitive_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Select(l => new
            {
                l.Id,
                Score = EfClass.Functions.NgramSearchCaseInsensitive(l.Message, "error")
            });

        var sql = query.ToQueryString();

        Assert.Contains("ngramSearchCaseInsensitive(", sql);
    }

    [Fact]
    public void NgramSearch_InWhereClause_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Where(l => EfClass.Functions.NgramSearch(l.Message, "error") > 0.5f);

        var sql = query.ToQueryString();

        Assert.Contains("ngramSearch(", sql);
        Assert.Contains("0.5", sql);
    }

    [Fact]
    public void NgramDistance_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Select(l => new
            {
                l.Id,
                Distance = EfClass.Functions.NgramDistance(l.Message, "error")
            });

        var sql = query.ToQueryString();

        Assert.Contains("ngramDistance(", sql);
    }

    [Fact]
    public void NgramDistanceCaseInsensitive_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Select(l => new
            {
                l.Id,
                Distance = EfClass.Functions.NgramDistanceCaseInsensitive(l.Message, "error")
            });

        var sql = query.ToQueryString();

        Assert.Contains("ngramDistanceCaseInsensitive(", sql);
    }

    [Fact]
    public void NgramSearchUTF8_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Select(l => new
            {
                l.Id,
                Score = EfClass.Functions.NgramSearchUTF8(l.Message, "error")
            });

        var sql = query.ToQueryString();

        Assert.Contains("ngramSearchUTF8(", sql);
    }

    [Fact]
    public void NgramSearchCaseInsensitiveUTF8_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Select(l => new
            {
                l.Id,
                Score = EfClass.Functions.NgramSearchCaseInsensitiveUTF8(l.Message, "error")
            });

        var sql = query.ToQueryString();

        Assert.Contains("ngramSearchCaseInsensitiveUTF8(", sql);
    }

    [Fact]
    public void NgramDistanceUTF8_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Select(l => new
            {
                l.Id,
                Distance = EfClass.Functions.NgramDistanceUTF8(l.Message, "error")
            });

        var sql = query.ToQueryString();

        Assert.Contains("ngramDistanceUTF8(", sql);
    }

    [Fact]
    public void NgramDistanceCaseInsensitiveUTF8_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Select(l => new
            {
                l.Id,
                Distance = EfClass.Functions.NgramDistanceCaseInsensitiveUTF8(l.Message, "error")
            });

        var sql = query.ToQueryString();

        Assert.Contains("ngramDistanceCaseInsensitiveUTF8(", sql);
    }

    #endregion

    #region Subsequence Function Tests

    [Fact]
    public void HasSubsequence_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Where(l => EfClass.Functions.HasSubsequence(l.Message, "err"));

        var sql = query.ToQueryString();

        Assert.Contains("hasSubsequence(", sql);
    }

    [Fact]
    public void HasSubsequenceCaseInsensitive_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Where(l => EfClass.Functions.HasSubsequenceCaseInsensitive(l.Message, "err"));

        var sql = query.ToQueryString();

        Assert.Contains("hasSubsequenceCaseInsensitive(", sql);
    }

    #endregion

    #region Substring Counting Tests

    [Fact]
    public void CountSubstrings_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Select(l => new
            {
                l.Id,
                Count = EfClass.Functions.CountSubstrings(l.Message, "error")
            });

        var sql = query.ToQueryString();

        Assert.Contains("countSubstrings(", sql);
    }

    [Fact]
    public void CountSubstringsCaseInsensitive_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Select(l => new
            {
                l.Id,
                Count = EfClass.Functions.CountSubstringsCaseInsensitive(l.Message, "error")
            });

        var sql = query.ToQueryString();

        Assert.Contains("countSubstringsCaseInsensitive(", sql);
    }

    #endregion

    #region Multi-Match (Regex) Tests

    [Fact]
    public void MultiMatchAny_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Where(l => EfClass.Functions.MultiMatchAny(l.Message, new[] { "err.*", "warn\\d+" }));

        var sql = query.ToQueryString();

        Assert.Contains("multiMatchAny(", sql);
    }

    [Fact]
    public void MultiMatchAnyIndex_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Select(l => new
            {
                l.Id,
                Index = EfClass.Functions.MultiMatchAnyIndex(l.Message, new[] { "err.*", "warn\\d+" })
            });

        var sql = query.ToQueryString();

        Assert.Contains("multiMatchAnyIndex(", sql);
    }

    [Fact]
    public void MultiMatchAllIndices_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Select(l => new
            {
                l.Id,
                Indices = EfClass.Functions.MultiMatchAllIndices(l.Message, new[] { "err.*", "warn\\d+" })
            });

        var sql = query.ToQueryString();

        Assert.Contains("multiMatchAllIndices(", sql);
    }

    #endregion

    #region Extract Function Tests

    [Fact]
    public void ExtractAll_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Select(l => new
            {
                l.Id,
                Matches = EfClass.Functions.ExtractAll(l.Message, @"\d+")
            });

        var sql = query.ToQueryString();

        Assert.Contains("extractAll(", sql);
    }

    [Fact]
    public void SplitByNonAlpha_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .Select(l => new
            {
                l.Id,
                Tokens = EfClass.Functions.SplitByNonAlpha(l.Message)
            });

        var sql = query.ToQueryString();

        Assert.Contains("splitByNonAlpha(", sql);
    }

    #endregion

    #region Fluent Extension Tests

    [Fact]
    public void ContainsToken_FluentExtension_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .ContainsToken(l => l.Message, "error");

        var sql = query.ToQueryString();

        Assert.Contains("hasToken(", sql);
        Assert.Contains("error", sql);
    }

    [Fact]
    public void ContainsToken_CaseInsensitive_FluentExtension_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .ContainsToken(l => l.Message, "error", caseInsensitive: true);

        var sql = query.ToQueryString();

        Assert.Contains("hasTokenCaseInsensitive(", sql);
    }

    [Fact]
    public void ContainsAny_FluentExtension_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .ContainsAny(l => l.Message, new[] { "error", "exception" });

        var sql = query.ToQueryString();

        Assert.Contains("multiSearchAny(", sql);
    }

    [Fact]
    public void ContainsAny_CaseInsensitive_FluentExtension_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .ContainsAny(l => l.Message, new[] { "error", "exception" }, caseInsensitive: true);

        var sql = query.ToQueryString();

        Assert.Contains("multiSearchAnyCaseInsensitive(", sql);
    }

    [Fact]
    public void FuzzyMatch_FluentExtension_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .FuzzyMatch(l => l.Message, "error", threshold: 0.5f);

        var sql = query.ToQueryString();

        Assert.Contains("ngramSearch(", sql);
        Assert.Contains("0.5", sql);
        Assert.Contains("ORDER BY", sql);
        Assert.Contains("DESC", sql);
    }

    [Fact]
    public void FuzzyMatch_CaseInsensitive_FluentExtension_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.LogEntries
            .FuzzyMatch(l => l.Message, "error", threshold: 0.3f, caseInsensitive: true);

        var sql = query.ToQueryString();

        Assert.Contains("ngramSearchCaseInsensitive(", sql);
        Assert.Contains("ORDER BY", sql);
        Assert.Contains("DESC", sql);
    }

    #endregion

    #region Test Infrastructure

    private static TextSearchTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TextSearchTestContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        return new TextSearchTestContext(options);
    }

    #endregion
}

#region Test Entities

public class LogEntry
{
    public Guid Id { get; set; }
    public string Message { get; set; } = string.Empty;
    public string Level { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; }
}

#endregion

#region Test Context

public class TextSearchTestContext : DbContext
{
    public TextSearchTestContext(DbContextOptions<TextSearchTestContext> options)
        : base(options)
    {
    }

    public DbSet<LogEntry> LogEntries => Set<LogEntry>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<LogEntry>(entity =>
        {
            entity.ToTable("log_entries");
            entity.HasKey(e => e.Id);
        });
    }
}

#endregion
