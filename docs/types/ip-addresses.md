# IP Address Types

ClickHouse has native types for IP addresses: `IPv4` and `IPv6`. EF.CH provides custom struct types for efficient IP address handling.

## Type Mappings

| .NET Type | ClickHouse Type | Storage |
|-----------|-----------------|---------|
| `ClickHouseIPv4` | `IPv4` | 4 bytes |
| `ClickHouseIPv6` | `IPv6` | 16 bytes |
| `IPAddress` | `IPv6` | 16 bytes |

## Using EF.CH IP Types

Import the types namespace:

```csharp
using EF.CH.Types;
```

### Entity Definition

```csharp
using EF.CH.Types;

public class AccessLog
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public ClickHouseIPv4 ClientIPv4 { get; set; }  // IPv4
    public ClickHouseIPv6 ClientIPv6 { get; set; }  // IPv6
    public string Endpoint { get; set; } = string.Empty;
    public int StatusCode { get; set; }
}
```

### Using System.Net.IPAddress

You can also use `System.Net.IPAddress`, which maps to `IPv6`:

```csharp
using System.Net;

public class NetworkEvent
{
    public Guid Id { get; set; }
    public IPAddress SourceIP { get; set; } = IPAddress.None;  // IPv6
    public IPAddress DestinationIP { get; set; } = IPAddress.None;
}
```

**Note:** `IPAddress` always maps to `IPv6`. IPv4 addresses are stored in IPv6-compatible format.

## Configuration

IP types work without special configuration:

```csharp
modelBuilder.Entity<AccessLog>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });
    // IP properties just work
});
```

## Creating IP Values

### ClickHouseIPv4

```csharp
// From string
var ip1 = ClickHouseIPv4.Parse("192.168.1.1");

// From bytes
var ip2 = new ClickHouseIPv4(192, 168, 1, 1);

// From uint
var ip3 = new ClickHouseIPv4(3232235777); // 192.168.1.1

// From IPAddress
var ip4 = ClickHouseIPv4.FromIPAddress(IPAddress.Parse("192.168.1.1"));
```

### ClickHouseIPv6

```csharp
// From string
var ip1 = ClickHouseIPv6.Parse("2001:db8::1");

// From IPAddress
var ip2 = ClickHouseIPv6.FromIPAddress(IPAddress.Parse("2001:db8::1"));

// IPv4-mapped IPv6
var ip3 = ClickHouseIPv6.Parse("::ffff:192.168.1.1");
```

## Inserting Data

```csharp
context.AccessLogs.Add(new AccessLog
{
    Id = Guid.NewGuid(),
    Timestamp = DateTime.UtcNow,
    ClientIPv4 = ClickHouseIPv4.Parse("192.168.1.100"),
    ClientIPv6 = ClickHouseIPv6.Parse("2001:db8::1"),
    Endpoint = "/api/users",
    StatusCode = 200
});
await context.SaveChangesAsync();
```

## Querying

### Filter by IP

```csharp
var targetIP = ClickHouseIPv4.Parse("192.168.1.100");

var logs = await context.AccessLogs
    .Where(l => l.ClientIPv4 == targetIP)
    .ToListAsync();
```

### IP Range Queries

IP addresses support comparison operators:

```csharp
var startIP = ClickHouseIPv4.Parse("192.168.1.0");
var endIP = ClickHouseIPv4.Parse("192.168.1.255");

var subnetLogs = await context.AccessLogs
    .Where(l => l.ClientIPv4 >= startIP && l.ClientIPv4 <= endIP)
    .ToListAsync();
```

### Group by IP

```csharp
var ipCounts = await context.AccessLogs
    .GroupBy(l => l.ClientIPv4)
    .Select(g => new { IP = g.Key, Count = g.Count() })
    .OrderByDescending(x => x.Count)
    .Take(10)
    .ToListAsync();
```

## Generated DDL

```csharp
public class AccessLog
{
    public Guid Id { get; set; }
    public ClickHouseIPv4 ClientIPv4 { get; set; }
    public ClickHouseIPv6 ClientIPv6 { get; set; }
}
```

Generates:

```sql
CREATE TABLE "AccessLogs" (
    "Id" UUID NOT NULL,
    "ClientIPv4" IPv4 NOT NULL,
    "ClientIPv6" IPv6 NOT NULL
)
ENGINE = MergeTree
ORDER BY ("Id")
```

## Converting Between Types

### To String

```csharp
ClickHouseIPv4 ip = ClickHouseIPv4.Parse("192.168.1.1");
string str = ip.ToString();  // "192.168.1.1"
```

### To IPAddress

```csharp
ClickHouseIPv4 ip4 = ClickHouseIPv4.Parse("192.168.1.1");
IPAddress addr = ip4.ToIPAddress();

ClickHouseIPv6 ip6 = ClickHouseIPv6.Parse("2001:db8::1");
IPAddress addr6 = ip6.ToIPAddress();
```

### IPv4 ↔ IPv6

```csharp
// IPv4 to IPv6 (IPv4-mapped)
var ipv4 = ClickHouseIPv4.Parse("192.168.1.1");
var ipv6 = ipv4.ToIPv6();  // ::ffff:192.168.1.1

// IPv6 to IPv4 (if IPv4-mapped)
var mapped = ClickHouseIPv6.Parse("::ffff:192.168.1.1");
if (mapped.IsIPv4Mapped)
{
    var back = mapped.ToIPv4();
}
```

## Real-World Examples

### Web Access Log

```csharp
public class WebAccessLog
{
    public DateTime Timestamp { get; set; }
    public ClickHouseIPv4 ClientIP { get; set; }
    public string Method { get; set; } = string.Empty;
    public string Path { get; set; } = string.Empty;
    public int StatusCode { get; set; }
    public int ResponseTimeMs { get; set; }
    public string? UserAgent { get; set; }
}

modelBuilder.Entity<WebAccessLog>(entity =>
{
    entity.HasNoKey();
    entity.UseMergeTree(x => new { x.Timestamp, x.ClientIP });
    entity.HasPartitionByDay(x => x.Timestamp);
});
```

### Network Flow Data

```csharp
public class NetworkFlow
{
    public DateTime Timestamp { get; set; }
    public ClickHouseIPv4 SourceIP { get; set; }
    public ClickHouseIPv4 DestinationIP { get; set; }
    public ushort SourcePort { get; set; }
    public ushort DestinationPort { get; set; }
    public string Protocol { get; set; } = string.Empty;
    public long Bytes { get; set; }
    public long Packets { get; set; }
}
```

### Security Events

```csharp
public class SecurityEvent
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string EventType { get; set; } = string.Empty;
    public ClickHouseIPv4 SourceIP { get; set; }
    public ClickHouseIPv4? TargetIP { get; set; }  // Nullable
    public string Severity { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
}
```

## Scaffolding

When reverse-engineering a ClickHouse database:

| ClickHouse Type | Generated .NET Type |
|-----------------|---------------------|
| `IPv4` | `ClickHouseIPv4` |
| `IPv6` | `ClickHouseIPv6` |
| `Nullable(IPv4)` | `ClickHouseIPv4?` |

## IPv4 vs IPv6

| Feature | IPv4 | IPv6 |
|---------|------|------|
| Storage | 4 bytes | 16 bytes |
| Format | `192.168.1.1` | `2001:db8::1` |
| Address space | ~4 billion | ~340 undecillion |
| Use case | Most common | Modern/dual-stack |

**Choose IPv4 when:**
- You only deal with IPv4 addresses
- Storage efficiency matters
- Legacy systems

**Choose IPv6 when:**
- You need to store both IPv4 and IPv6
- Using `IPAddress` from .NET
- Future-proofing

## Limitations

- **No CIDR Operations**: Subnet matching requires raw SQL or application code
- **No Geo Functions**: Use ClickHouse functions via raw SQL for geolocation

## See Also

- [Type Mappings Overview](overview.md)
- [ClickHouse IPv4/IPv6 Docs](https://clickhouse.com/docs/en/sql-reference/data-types/ipv4)
