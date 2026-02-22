# External Entities

External entities let you query data in PostgreSQL, MySQL, ODBC-accessible databases, or Redis directly from ClickHouse without importing the data. EF.CH maps these as read-only (or optionally writable) entities backed by ClickHouse table functions.

No ClickHouse table is created for external entities. Queries are forwarded to the external source at runtime.

## PostgreSQL

```csharp
public class Customer
{
    public int Id { get; set; }
    public string Name { get; set; }
    public string Email { get; set; }
}
```

```csharp
modelBuilder.ExternalPostgresEntity<Customer>(ext => ext
    .FromTable("customers", schema: "public")
    .Connection(c => c
        .HostPort(env: "PG_HOSTPORT")
        .Database(env: "PG_DATABASE")
        .Credentials(userEnv: "PG_USER", passwordEnv: "PG_PASSWORD")
    )
    .ReadOnly()
);
```

Generated query:

```sql
SELECT * FROM postgresql(
    'host:port',  -- resolved from PG_HOSTPORT env var
    'database',   -- resolved from PG_DATABASE env var
    'customers',
    'user',       -- resolved from PG_USER env var
    'password',   -- resolved from PG_PASSWORD env var
    'public'
)
```

## MySQL

```csharp
modelBuilder.ExternalMySqlEntity<Customer>(ext => ext
    .FromTable("customers")
    .Connection(c => c
        .HostPort(env: "MYSQL_HOSTPORT")
        .Database(env: "MYSQL_DATABASE")
        .Credentials(userEnv: "MYSQL_USER", passwordEnv: "MYSQL_PASSWORD")
    )
    .ReadOnly()
);
```

Generated query:

```sql
SELECT * FROM mysql(
    'host:port',
    'database',
    'customers',
    'user',
    'password'
)
```

## ODBC

ODBC entities use a pre-configured DSN (Data Source Name) from `odbc.ini` on the ClickHouse server.

```csharp
modelBuilder.ExternalOdbcEntity<SalesData>(ext => ext
    .FromTable("sales")
    .Dsn(env: "MSSQL_DSN")
    .Database("reporting")
    .ReadOnly()
);
```

Generated query:

```sql
SELECT * FROM odbc('DSN=mssql_dsn;Database=reporting', 'sales')
```

## Redis

Redis entities map key-value data. Configure the key column and connection details.

```csharp
public class SessionCache
{
    public string SessionId { get; set; }
    public string Data { get; set; }
}
```

```csharp
modelBuilder.ExternalRedisEntity<SessionCache>(ext => ext
    .KeyColumn(x => x.SessionId)
    .Connection(c => c
        .HostPort(env: "REDIS_HOST")
        .Password(env: "REDIS_PASSWORD")
        .DbIndex(0)
    )
    .ReadOnly()
);
```

## Environment Variable Support

All connection builders resolve credentials from environment variables at query time, keeping secrets out of the EF Core model and migration files. The `env:` parameter specifies the environment variable name.

```csharp
.Connection(c => c
    .HostPort(env: "PG_HOSTPORT")       // reads $PG_HOSTPORT
    .Database(env: "PG_DATABASE")        // reads $PG_DATABASE
    .Credentials(
        userEnv: "PG_USER",             // reads $PG_USER
        passwordEnv: "PG_PASSWORD"       // reads $PG_PASSWORD
    )
)
```

> **Note:** Environment variables are resolved when the query executes, not when the model is built. If a required variable is missing at runtime, the query will fail.

## ReadOnly

Calling `.ReadOnly()` marks the entity as having no key in EF Core, preventing any attempt to use `SaveChanges` for inserts or updates. This is the recommended setting for external entities.

Without `.ReadOnly()`, the entity is writable -- ClickHouse will attempt to INSERT into the external source, which may or may not be supported depending on the table function.

## JOIN Patterns

External entities participate in LINQ joins the same way as regular entities. ClickHouse executes the external table function inline.

```csharp
var results = await context.Set<Order>()
    .Join(
        context.Set<Customer>(),
        o => o.CustomerId,
        c => c.Id,
        (o, c) => new { o.Id, o.Amount, c.Name, c.Email }
    )
    .Where(x => x.Amount > 100)
    .ToListAsync();
```

Generated SQL:

```sql
SELECT o."Id", o."Amount", c."Name", c."Email"
FROM "Order" AS o
INNER JOIN postgresql('host', 'db', 'customers', 'user', 'pass', 'public') AS c
    ON o."CustomerId" = c."Id"
WHERE o."Amount" > 100
```

> **Note:** Joining a local ClickHouse table with an external entity transfers the external data over the network for every query. For frequently joined data, consider using a dictionary instead.

## See Also

- [Dictionaries](dictionaries.md) -- cached key-value lookups from external sources
- [Table Functions](table-functions.md) -- ad-hoc queries against S3, URLs, and remote servers
