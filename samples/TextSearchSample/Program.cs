using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using EfClass = Microsoft.EntityFrameworkCore.EF;

// ============================================================
// Text Search Sample
// ============================================================
// Demonstrates ClickHouse full-text search functions:
// - Token search (hasToken, hasTokenAny)
// - Multi-search (multiSearchAny, multiSearchFirstIndex)
// - N-gram similarity (ngramSearch, ngramDistance)
// - Subsequence matching (hasSubsequence)
// - Substring counting (countSubstrings)
// - Regex multi-match via Hyperscan (multiMatchAny)
// - Extract functions (extractAll, splitByNonAlpha)
// - Fluent helpers (ContainsToken, ContainsAny, FuzzyMatch)
// ============================================================

Console.WriteLine("Text Search Sample");
Console.WriteLine("==================\n");

await using var context = new LogDbContext();

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Insert sample log entries
Console.WriteLine("Inserting log entries...\n");

var logs = new List<LogEntry>
{
    new()
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddMinutes(-10),
        Level = "ERROR",
        Service = "auth-service",
        Message = "Failed to authenticate user: invalid credentials for user@example.com"
    },
    new()
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddMinutes(-9),
        Level = "WARN",
        Service = "api-gateway",
        Message = "Rate limit exceeded for client 192.168.1.100 on endpoint /api/users"
    },
    new()
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddMinutes(-8),
        Level = "ERROR",
        Service = "payment-service",
        Message = "Payment processing failed: timeout connecting to payment gateway after 30000ms"
    },
    new()
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddMinutes(-7),
        Level = "INFO",
        Service = "auth-service",
        Message = "User alice@company.com successfully authenticated via SSO"
    },
    new()
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddMinutes(-6),
        Level = "ERROR",
        Service = "data-pipeline",
        Message = "Exception in batch processor: NullReferenceException at DataTransformer.cs:142"
    },
    new()
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddMinutes(-5),
        Level = "WARN",
        Service = "api-gateway",
        Message = "Slow response detected: GET /api/reports took 4523ms (threshold: 2000ms)"
    },
    new()
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddMinutes(-4),
        Level = "INFO",
        Service = "scheduler",
        Message = "Scheduled job cleanup_old_sessions completed successfully in 1250ms"
    },
    new()
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddMinutes(-3),
        Level = "ERROR",
        Service = "notification-service",
        Message = "Failed to send email notification: SMTP connection refused to smtp.provider.com:587"
    },
    new()
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddMinutes(-2),
        Level = "DEBUG",
        Service = "auth-service",
        Message = "Token refresh for session abc-123-def: new expiry 2024-01-15T14:30:00Z"
    },
    new()
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddMinutes(-1),
        Level = "ERROR",
        Service = "payment-service",
        Message = "Duplicate transaction detected: order-id=ORD-99421, amount=149.99 USD"
    }
};

context.Logs.AddRange(logs);
await context.SaveChangesAsync();
Console.WriteLine($"Inserted {logs.Count} log entries.\n");

// ============================================================
// 1. Token Search — hasToken
// ============================================================
// hasToken splits text by non-alphanumeric chars and checks for exact token matches.
// Works well with tokenbf_v1 skip indices for fast filtering.

Console.WriteLine("=== 1. Token Search (hasToken) ===\n");

var errorLogs = await context.Logs
    .Where(l => EfClass.Functions.HasToken(l.Message, "failed"))
    .Select(l => new { l.Service, l.Message })
    .ToListAsync();

Console.WriteLine("Logs containing token 'failed':");
foreach (var log in errorLogs)
{
    Console.WriteLine($"  [{log.Service}] {log.Message}");
}

// Case-insensitive token search
Console.WriteLine();
var caseInsensitiveLogs = await context.Logs
    .Where(l => EfClass.Functions.HasTokenCaseInsensitive(l.Message, "exception"))
    .Select(l => new { l.Service, l.Message })
    .ToListAsync();

Console.WriteLine("Logs containing token 'exception' (case-insensitive):");
foreach (var log in caseInsensitiveLogs)
{
    Console.WriteLine($"  [{log.Service}] {log.Message}");
}

// HasAnyToken — check for any of multiple tokens at once
// Note: hasAnyToken/hasAllTokens require the 'enable_full_text_index' setting
Console.WriteLine();
var multiTokenLogs = await context.Logs
    .WithSetting("enable_full_text_index", 1)
    .Where(l => EfClass.Functions.HasAnyToken(l.Message, new[] { "timeout", "refused", "exceeded" }))
    .Select(l => new { l.Level, l.Message })
    .ToListAsync();

Console.WriteLine("Logs containing any of ['timeout', 'refused', 'exceeded']:");
foreach (var log in multiTokenLogs)
{
    Console.WriteLine($"  [{log.Level}] {log.Message}");
}

// HasAllTokens — check that all tokens are present
Console.WriteLine();
var allTokenLogs = await context.Logs
    .WithSetting("enable_full_text_index", 1)
    .Where(l => EfClass.Functions.HasAllTokens(l.Message, new[] { "payment", "failed" }))
    .Select(l => new { l.Level, l.Message })
    .ToListAsync();

Console.WriteLine("Logs containing ALL of ['payment', 'failed']:");
foreach (var log in allTokenLogs)
{
    Console.WriteLine($"  [{log.Level}] {log.Message}");
}

// ============================================================
// 2. Multi-Search — multiSearchAny / multiSearchFirstIndex
// ============================================================
// Substring search across multiple needles simultaneously.
// More flexible than hasToken — matches substrings, not just tokens.

Console.WriteLine("\n=== 2. Multi-Search (multiSearchAny) ===\n");

var substringMatches = await context.Logs
    .Where(l => EfClass.Functions.MultiSearchAny(l.Message, new[] { "payment", "email", "SMTP" }))
    .Select(l => new { l.Service, l.Message })
    .ToListAsync();

Console.WriteLine("Logs matching any of ['payment', 'email', 'SMTP']:");
foreach (var log in substringMatches)
{
    Console.WriteLine($"  [{log.Service}] {log.Message}");
}

// Find which needle matched first
Console.WriteLine();
var firstMatchIndex = await context.Logs
    .Select(l => new
    {
        l.Service,
        FirstMatch = EfClass.Functions.MultiSearchFirstIndex(l.Message, new[] { "error", "warn", "info" })
    })
    .Where(x => x.FirstMatch > 0)
    .ToListAsync();

Console.WriteLine("First matching term index for ['error', 'warn', 'info'] (1-based):");
foreach (var log in firstMatchIndex)
{
    var matchedTerm = log.FirstMatch switch { 1UL => "error", 2UL => "warn", 3UL => "info", _ => "?" };
    Console.WriteLine($"  [{log.Service}] first match: '{matchedTerm}' (index {log.FirstMatch})");
}

// ============================================================
// 3. N-gram Similarity — ngramSearch / ngramDistance
// ============================================================
// Fuzzy matching using 3-gram comparison. Returns a score between 0 and 1.
// ngramSearch: higher = more similar. ngramDistance: lower = more similar.

Console.WriteLine("\n=== 3. N-gram Similarity (ngramSearch) ===\n");

var similarityScores = await context.Logs
    .Select(l => new
    {
        l.Service,
        l.Message,
        Score = EfClass.Functions.NgramSearch(l.Message, "authentication failed")
    })
    .Where(x => x.Score > 0.1f)
    .OrderByDescending(x => x.Score)
    .ToListAsync();

Console.WriteLine("Logs similar to 'authentication failed' (score > 0.1):");
foreach (var log in similarityScores)
{
    Console.WriteLine($"  [{log.Score:F3}] [{log.Service}] {log.Message}");
}

// N-gram distance (inverse of similarity)
Console.WriteLine();
var distanceScores = await context.Logs
    .Select(l => new
    {
        l.Service,
        l.Message,
        Distance = EfClass.Functions.NgramDistance(l.Message, "connection timeout")
    })
    .OrderBy(x => x.Distance)
    .Take(3)
    .ToListAsync();

Console.WriteLine("Top 3 logs closest to 'connection timeout' (by n-gram distance):");
foreach (var log in distanceScores)
{
    Console.WriteLine($"  [dist={log.Distance:F3}] [{log.Service}] {log.Message}");
}

// ============================================================
// 4. Subsequence Matching — hasSubsequence
// ============================================================
// Checks if characters appear in order (not necessarily contiguous).
// "abc" matches "axbxc" but not "cba".

Console.WriteLine("\n=== 4. Subsequence Matching (hasSubsequence) ===\n");

var subsequenceMatches = await context.Logs
    .Where(l => EfClass.Functions.HasSubsequence(l.Message, "NullRef"))
    .Select(l => new { l.Service, l.Message })
    .ToListAsync();

Console.WriteLine("Logs with subsequence 'NullRef':");
foreach (var log in subsequenceMatches)
{
    Console.WriteLine($"  [{log.Service}] {log.Message}");
}

// ============================================================
// 5. Substring Counting — countSubstrings
// ============================================================
// Count non-overlapping occurrences of a substring.

Console.WriteLine("\n=== 5. Substring Counting (countSubstrings) ===\n");

var substringCounts = await context.Logs
    .Select(l => new
    {
        l.Service,
        l.Message,
        ColonCount = EfClass.Functions.CountSubstrings(l.Message, ":")
    })
    .OrderByDescending(x => x.ColonCount)
    .Take(5)
    .ToListAsync();

Console.WriteLine("Top 5 logs by colon count:");
foreach (var log in substringCounts)
{
    Console.WriteLine($"  [count={log.ColonCount}] [{log.Service}] {log.Message}");
}

// ============================================================
// 6. Regex Multi-Match via Hyperscan — multiMatchAny
// ============================================================
// Uses Hyperscan for high-performance regex matching against multiple patterns.

Console.WriteLine("\n=== 6. Regex Multi-Match (multiMatchAny) ===\n");

var regexMatches = await context.Logs
    .Where(l => EfClass.Functions.MultiMatchAny(l.Message, new[] { @"\d+ms", @"\d+\.\d+\.\d+\.\d+" }))
    .Select(l => new { l.Service, l.Message })
    .ToListAsync();

Console.WriteLine("Logs matching regex patterns [duration in ms, IP addresses]:");
foreach (var log in regexMatches)
{
    Console.WriteLine($"  [{log.Service}] {log.Message}");
}

// Find which regex pattern matched
Console.WriteLine();
var regexIndex = await context.Logs
    .Select(l => new
    {
        l.Service,
        MatchIndex = EfClass.Functions.MultiMatchAnyIndex(l.Message, new[] { @"timeout", @"refused", @"duplicate" })
    })
    .Where(x => x.MatchIndex > 0)
    .ToListAsync();

Console.WriteLine("Which pattern matched ['timeout', 'refused', 'duplicate'] (1-based index):");
foreach (var log in regexIndex)
{
    var pattern = log.MatchIndex switch { 1UL => "timeout", 2UL => "refused", 3UL => "duplicate", _ => "?" };
    Console.WriteLine($"  [{log.Service}] matched: '{pattern}' (index {log.MatchIndex})");
}

// ============================================================
// 7. Extract Functions — extractAll / splitByNonAlpha
// ============================================================
// Extract substrings using regex or split by non-alphanumeric characters.

Console.WriteLine("\n=== 7. Extract Functions (extractAll) ===\n");

var extracted = await context.Logs
    .Select(l => new
    {
        l.Service,
        Numbers = EfClass.Functions.ExtractAll(l.Message, @"\d+")
    })
    .Take(5)
    .ToListAsync();

Console.WriteLine("Numbers extracted from log messages:");
foreach (var log in extracted.Where(x => x.Numbers.Length > 0))
{
    Console.WriteLine($"  [{log.Service}] numbers: [{string.Join(", ", log.Numbers)}]");
}

Console.WriteLine();
var tokens = await context.Logs
    .Select(l => new
    {
        l.Service,
        Tokens = EfClass.Functions.SplitByNonAlpha(l.Message)
    })
    .Take(3)
    .ToListAsync();

Console.WriteLine("Tokens from splitByNonAlpha (first 3 logs):");
foreach (var log in tokens)
{
    Console.WriteLine($"  [{log.Service}] tokens: [{string.Join(", ", log.Tokens.Take(8))}...]");
}

// ============================================================
// 8. Fluent Query Extensions
// ============================================================
// Convenience methods that wrap DbFunctions for common patterns.

Console.WriteLine("\n=== 8. Fluent Query Extensions ===\n");

// ContainsToken — wraps hasToken in a Where clause
var tokenResults = await context.Logs
    .ContainsToken(l => l.Message, "timeout")
    .Select(l => new { l.Service, l.Message })
    .ToListAsync();

Console.WriteLine("ContainsToken('timeout'):");
foreach (var log in tokenResults)
{
    Console.WriteLine($"  [{log.Service}] {log.Message}");
}

// ContainsAny — wraps multiSearchAny in a Where clause
Console.WriteLine();
var anyResults = await context.Logs
    .ContainsAny(l => l.Message, new[] { "SSO", "credentials", "session" })
    .Select(l => new { l.Service, l.Message })
    .ToListAsync();

Console.WriteLine("ContainsAny(['SSO', 'credentials', 'session']):");
foreach (var log in anyResults)
{
    Console.WriteLine($"  [{log.Service}] {log.Message}");
}

// FuzzyMatch — wraps ngramSearch with threshold + ORDER BY score DESC
Console.WriteLine();
var fuzzyResults = await context.Logs
    .FuzzyMatch(l => l.Message, "payment error", threshold: 0.05f)
    .Select(l => new { l.Service, l.Message })
    .ToListAsync();

Console.WriteLine("FuzzyMatch('payment error', threshold: 0.05):");
foreach (var log in fuzzyResults)
{
    Console.WriteLine($"  [{log.Service}] {log.Message}");
}

// ============================================================
// 9. Combining with Skip Indices
// ============================================================
// Text search functions work best with tokenbf_v1 or ngrambf_v1 skip indices.
// The index configuration is in OnModelCreating below.

Console.WriteLine("\n=== 9. Skip Index Integration ===\n");
Console.WriteLine("The Message column has two skip indices configured:");
Console.WriteLine("  - tokenbf_v1(10240, 3, 0)  — accelerates hasToken/hasTokenAny");
Console.WriteLine("  - ngrambf_v1(3, 10240, 3, 0) — accelerates ngramSearch/multiSearchAny");
Console.WriteLine("These indices let ClickHouse skip granules that definitely don't match.\n");

Console.WriteLine("Done!");

// ============================================================
// Entity Definitions
// ============================================================

public class LogEntry
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string Level { get; set; } = string.Empty;
    public string Service { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
}

// ============================================================
// DbContext Definition
// ============================================================

public class LogDbContext : DbContext
{
    public DbSet<LogEntry> Logs => Set<LogEntry>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=text_search_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<LogEntry>(entity =>
        {
            entity.ToTable("Logs");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.Timestamp, x.Id });
            entity.HasPartitionByMonth(x => x.Timestamp);

            // Skip indices to accelerate text search functions
            entity.HasIndex(e => e.Message).UseTokenBF(size: 10240, hashes: 3);
            entity.HasIndex(e => e.Message).UseNgramBF(ngramSize: 3, size: 10240, hashes: 3);
        });
    }
}
