# Troubleshooting

This guide covers common issues and their solutions when working with EF.CH.

## Connection Issues

### "Unable to connect to ClickHouse server"

**Symptoms:** Connection timeout or refused errors.

**Check:**

1. **Host and port**: Default port is 8123 (HTTP) or 9000 (native).
   ```csharp
   // HTTP protocol (default)
   options.UseClickHouse("Host=localhost;Port=8123;Database=mydb");

   // Native protocol
   options.UseClickHouse("Host=localhost;Port=9000;Database=mydb");
   ```

2. **Docker networking**: Use `host.docker.internal` when connecting from Docker to host.

3. **Firewall**: Ensure the ClickHouse port is accessible.

4. **SSL/TLS**: If connecting to ClickHouse Cloud or TLS-enabled servers:
   ```csharp
   options.UseClickHouse("Host=xxx.clickhouse.cloud;Port=8443;Database=mydb;Ssl=true");
   ```

### "Authentication failed"

**Symptoms:** 401 or authentication error.

**Solution:** Verify username and password:
```csharp
options.UseClickHouse("Host=localhost;Database=mydb;Username=default;Password=mypassword");
```

For ClickHouse Cloud, use the credentials from your service dashboard.

---

## Migration Issues

### "Table already exists" during migration

**Symptoms:** Migration fails because tables exist.

**Cause:** ClickHouse split migrations create multiple step files. If migration was partially applied, some objects may exist.

**Solution:** Use idempotent migrations or check manually:
```sql
-- Check if table exists
SELECT name FROM system.tables WHERE database = 'mydb' AND name = 'MyTable';
```

### "Cannot find source table" for Materialized View

**Symptoms:** MV creation fails with missing source table.

**Cause:** Migration phase ordering issue. MVs must be created after their source tables.

**Solution:** Ensure split migrations are enabled (default) - they handle ordering automatically:
```csharp
// In OnConfiguring
options.UseClickHouse("...", o => o.UseSplitMigrations(true));
```

If using single-file migrations, manually order operations:
```csharp
// First: Create source table
migrationBuilder.CreateTable(...);

// Then: Create MV
migrationBuilder.Sql("CREATE MATERIALIZED VIEW...");
```

### "Dictionary not found" at startup

**Symptoms:** Queries using dictGet fail with unknown dictionary.

**Cause:** Dictionaries are created separately from migrations.

**Solution:** Call `EnsureDictionariesAsync()` at startup:
```csharp
await using var context = new MyDbContext();
await context.Database.EnsureCreatedAsync();
await context.EnsureDictionariesAsync();  // Create dictionaries
```

---

## Query Issues

### "NotSupportedException: UPDATE is not supported"

**Symptoms:** Modifying tracked entities and calling SaveChanges throws.

**Cause:** ClickHouse doesn't support row-level UPDATE.

**Solutions:**

1. **Use ReplacingMergeTree** for "last write wins" semantics:
   ```csharp
   entity.UseReplacingMergeTree(x => x.Version, x => x.Id);

   // Insert new version instead of update
   entity.Version++;
   context.Entities.Add(entity);
   await context.SaveChangesAsync();
   ```

2. **Delete and re-insert**:
   ```csharp
   context.Entities.Remove(entity);
   await context.SaveChangesAsync();

   entity.SomeProperty = newValue;
   context.Entities.Add(entity);
   await context.SaveChangesAsync();
   ```

### "NotSupportedException: [LINQ operation] cannot be translated"

**Symptoms:** Complex LINQ operations throw translation errors.

**Common cases and solutions:**

1. **String interpolation in queries**:
   ```csharp
   // Fails
   .Where(e => e.Name == $"{prefix}_{suffix}")

   // Works
   var fullName = $"{prefix}_{suffix}";
   .Where(e => e.Name == fullName)
   ```

2. **Complex subqueries**: Restructure as multiple queries or use raw SQL.

3. **Unsupported functions**: Use `EF.Functions` or raw SQL:
   ```csharp
   var results = await context.Database
       .SqlQuery<MyDto>($"SELECT ... FROM ... WHERE custom_function(...)")
       .ToListAsync();
   ```

### Query returns duplicates with ReplacingMergeTree

**Symptoms:** Same logical row appears multiple times.

**Cause:** ReplacingMergeTree deduplication happens at merge time, not query time.

**Solution:** Use `.Final()` to force deduplication:
```csharp
var users = await context.Users.Final().ToListAsync();
```

**Trade-off:** FINAL adds query overhead. For read-heavy workloads, consider scheduled OPTIMIZE or application-level deduplication.

### Slow queries on non-primary columns

**Symptoms:** Queries filtering on columns not in ORDER BY are slow.

**Cause:** ClickHouse must scan all data without primary key filtering.

**Solutions:**

1. **Add a skip index**:
   ```csharp
   entity.HasIndex(x => x.UserId).UseBloomFilter();
   ```

2. **Add a projection** with different sort order:
   ```csharp
   entity.HasProjection().OrderBy(x => x.UserId).Build();
   ```

3. **Restructure ORDER BY** if query pattern is primary:
   ```csharp
   entity.UseMergeTree(x => new { x.UserId, x.Timestamp });
   ```

---

## Performance Issues

### Slow single-row inserts

**Symptoms:** Inserting records one at a time is slow.

**Cause:** ClickHouse is optimized for batch operations.

**Solution:** Batch inserts:
```csharp
// Slow
foreach (var item in items)
{
    context.Items.Add(item);
    await context.SaveChangesAsync();
}

// Fast
context.Items.AddRange(items);
await context.SaveChangesAsync();
```

For very large batches, consider using ClickHouse.Driver directly for bulk loading.

### High memory usage with large result sets

**Symptoms:** Out of memory when querying many rows.

**Solution:** Use streaming or pagination:
```csharp
// Streaming with AsAsyncEnumerable
await foreach (var item in context.Items.AsAsyncEnumerable())
{
    ProcessItem(item);
}

// Pagination
var page = await context.Items
    .OrderBy(x => x.Id)
    .Skip(pageIndex * pageSize)
    .Take(pageSize)
    .ToListAsync();
```

### Queries bypass projections

**Symptoms:** Queries are slower than expected despite having projections.

**Cause:** ClickHouse optimizer may not select the projection.

**Debug:** Check which projection is used:
```sql
EXPLAIN SELECT ... FROM ...;
-- Look for "Projection" in the plan
```

**Solution:** Ensure query matches projection's sort order or aggregation pattern exactly.

---

## Runtime Errors

### "Engine MergeTree requires ORDER BY"

**Symptoms:** Table creation fails.

**Cause:** MergeTree family engines require ORDER BY specification.

**Solution:** Always configure the engine:
```csharp
entity.UseMergeTree(x => new { x.OrderDate, x.Id });
```

### "Type mismatch" when inserting/querying

**Symptoms:** ClickHouse type errors.

**Common causes:**

1. **Enum values changed**: If enum values change, existing data may not match.

2. **Nullable mismatch**: Inserting null to non-nullable column:
   ```csharp
   // Make nullable in model
   public string? Name { get; set; }
   ```

3. **DateTime precision**: Ensure DateTime values are within ClickHouse's range.

### "Code: 60. DB::Exception: Table ... doesn't exist"

**Symptoms:** Query fails on table that should exist.

**Causes:**

1. **Case sensitivity**: ClickHouse table names are case-sensitive by default.
   ```csharp
   // Check table name casing
   entity.ToTable("MyTable");  // Must match exactly
   ```

2. **Database mismatch**: Check connection string database.

3. **Migrations not applied**: Ensure `EnsureCreatedAsync()` or migrations ran.

---

## Common Mistakes

### Forgetting to configure engine

```csharp
// Wrong - no engine specified
modelBuilder.Entity<Order>();

// Correct
modelBuilder.Entity<Order>(entity =>
{
    entity.UseMergeTree(x => x.Id);
});
```

### Using navigation properties for cascade operations

```csharp
// Won't cascade delete - ClickHouse has no FK enforcement
context.Customers.Remove(customer);
await context.SaveChangesAsync();
// Related orders still exist!

// Manually delete related entities
await context.Orders.Where(o => o.CustomerId == customer.Id).ExecuteDeleteAsync();
await context.Customers.Where(c => c.Id == customer.Id).ExecuteDeleteAsync();
```

### Expecting immediate consistency

```csharp
// Insert
context.Users.Add(new User { Id = id, Name = "Test" });
await context.SaveChangesAsync();

// Query immediately - may not find row yet (eventual consistency)
var user = await context.Users.FindAsync(id);  // Could be null!

// With ReplacingMergeTree, also need FINAL for consistency
var user = await context.Users.Final().FirstOrDefaultAsync(u => u.Id == id);
```

---

## Getting Help

1. **Check [Limitations](limitations.md)** - Many issues are inherent to ClickHouse.

2. **Review the [samples](../samples/)** - Working examples for each feature.

3. **Enable SQL logging** to see generated queries:
   ```csharp
   optionsBuilder
       .UseClickHouse("...")
       .LogTo(Console.WriteLine, LogLevel.Information);
   ```

4. **Test queries directly** in ClickHouse client to isolate EF Core vs ClickHouse issues.

## See Also

- [Limitations](limitations.md) - What's not supported
- [ClickHouse Concepts](clickhouse-concepts.md) - Architecture overview
- [Query Modifiers](features/query/query-modifiers.md) - FINAL, PREWHERE optimization
- [Delete Operations](features/operations/delete-operations.md) - Delete strategies
