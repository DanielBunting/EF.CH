# IP Address Functions

ClickHouse has built-in IP address functions optimized for log analysis, network monitoring, and web analytics. EF.CH exposes these as `EF.Functions` extensions.

## Available Functions

| C# Method | ClickHouse SQL | Return Type | Description |
|-----------|---------------|-------------|-------------|
| `IPv4NumToString(ip)` | `IPv4NumToString(ip)` | `string` | UInt32 → dotted string (e.g. `"192.168.1.1"`) |
| `IPv4StringToNum(s)` | `IPv4StringToNum(s)` | `uint` | Dotted string → UInt32 |
| `IsIPAddressInRange(addr, cidr)` | `isIPAddressInRange(addr, cidr)` | `bool` | Check if IP is in a CIDR range |
| `IsIPv4String(s)` | `isIPv4String(s)` | `bool` | Validate IPv4 string format |
| `IsIPv6String(s)` | `isIPv6String(s)` | `bool` | Validate IPv6 string format |

## Usage Examples

### Filter by Subnet

```csharp
using EF.CH.Extensions;

// Find all requests from the corporate network
var internal = await context.AccessLogs
    .Where(l => EF.Functions.IsIPAddressInRange(l.ClientIp, "10.0.0.0/8"))
    .ToListAsync();
```

Generates:
```sql
SELECT ... FROM "AccessLogs"
WHERE isIPAddressInRange("ClientIp", '10.0.0.0/8')
```

### Multiple Subnet Filtering

```csharp
// Filter to private IP ranges (RFC 1918)
var privateTraffic = await context.AccessLogs
    .Where(l =>
        EF.Functions.IsIPAddressInRange(l.ClientIp, "10.0.0.0/8") ||
        EF.Functions.IsIPAddressInRange(l.ClientIp, "172.16.0.0/12") ||
        EF.Functions.IsIPAddressInRange(l.ClientIp, "192.168.0.0/16"))
    .ToListAsync();
```

### IP Validation

```csharp
// Find rows with invalid IP addresses
var invalid = await context.AccessLogs
    .Where(l =>
        !EF.Functions.IsIPv4String(l.ClientIp) &&
        !EF.Functions.IsIPv6String(l.ClientIp))
    .ToListAsync();
```

### IP Conversion

```csharp
// Convert between numeric and string representations
var converted = await context.AccessLogs
    .Select(l => new
    {
        l.ClientIp,
        AsNum = EF.Functions.IPv4StringToNum(l.ClientIp)
    })
    .ToListAsync();
```

### Security: Geo-Blocking by IP Range

```csharp
// Block requests from specific CIDR ranges
var blocked = await context.Requests
    .Where(r => EF.Functions.IsIPAddressInRange(r.SourceIp, blockedCidr))
    .CountAsync();
```

## Notes

- IP addresses are typically stored as `String` in ClickHouse. For high-volume tables, consider storing as `UInt32` (for IPv4) and using `IPv4NumToString` for display.
- `IsIPAddressInRange` works with both IPv4 and IPv6 addresses and CIDR notation.
- `IPv4StringToNum` returns 0 for invalid input (doesn't throw).
- For IPv6-specific functions (`IPv6NumToString`, `IPv6StringToNum`), use raw SQL via `ClickHouseFunctions.RawSql<T>()`.

## Learn More

- [ClickHouse IP Address Functions](https://clickhouse.com/docs/en/sql-reference/functions/ip-address-functions)
