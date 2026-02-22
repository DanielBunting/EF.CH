# IP Address Types

## CLR to ClickHouse Mapping

```
ClickHouseIPv4  --> IPv4
ClickHouseIPv6  --> IPv6
IPAddress       --> IPv6
```

## ClickHouseIPv4

A custom `readonly struct` that wraps a `UInt32` value, providing type-safe IPv4 handling:

```csharp
using EF.CH;

public class Server
{
    public uint Id { get; set; }
    public ClickHouseIPv4 Address { get; set; }
}
```

```sql
CREATE TABLE "Servers" (
    "Id" UInt32,
    "Address" IPv4
) ENGINE = MergeTree() ORDER BY ("Id")
```

### Creating ClickHouseIPv4 Values

```csharp
// From string (implicit conversion)
ClickHouseIPv4 ip = "192.168.1.1";

// From UInt32
var ip = new ClickHouseIPv4(3232235777u);

// From IPAddress
var ip = new ClickHouseIPv4(IPAddress.Parse("192.168.1.1"));

// Parse
var ip = ClickHouseIPv4.Parse("10.0.0.1");

// TryParse
if (ClickHouseIPv4.TryParse("10.0.0.1", out var ip)) { }
```

The struct supports comparison operators (`==`, `!=`, `<`, `>`, `<=`, `>=`) and implements `IEquatable<ClickHouseIPv4>` and `IComparable<ClickHouseIPv4>`.

### SQL Literal

```sql
toIPv4('192.168.1.1')
```

## ClickHouseIPv6

A custom `readonly struct` that wraps a 16-byte array, providing type-safe IPv6 handling:

```csharp
public class NetworkEvent
{
    public uint Id { get; set; }
    public ClickHouseIPv6 SourceIp { get; set; }
}
```

```sql
"SourceIp" IPv6
```

### Creating ClickHouseIPv6 Values

```csharp
// From string (implicit conversion)
ClickHouseIPv6 ip = "2001:db8::1";

// From byte array (must be exactly 16 bytes)
var ip = new ClickHouseIPv6(new byte[16] { ... });

// From IPAddress (IPv4 addresses are auto-mapped to IPv6)
var ip = new ClickHouseIPv6(IPAddress.Parse("2001:db8::1"));

// Parse
var ip = ClickHouseIPv6.Parse("::1");
```

IPv4 addresses provided to `ClickHouseIPv6` are automatically mapped to IPv4-mapped IPv6 format (`::ffff:192.168.1.1`).

### SQL Literal

```sql
toIPv6('2001:db8::1')
```

## System.Net.IPAddress

For maximum compatibility, `IPAddress` maps to IPv6 (which can store both IPv4-mapped and native IPv6 addresses):

```csharp
using System.Net;

public class AccessLog
{
    public uint Id { get; set; }
    public IPAddress ClientIp { get; set; }
}
```

```sql
"ClientIp" IPv6
```

The provider uses a string-based value converter:

- **Write**: `IPAddress.ToString()` produces the string representation
- **Read**: `IPAddress.Parse(s)` reconstructs the address

### SQL Literal

```sql
toIPv6('192.168.1.1')    -- IPv4-mapped
toIPv6('2001:db8::1')    -- Native IPv6
```

## IP Functions

ClickHouse IP functions are available through `EF.Functions`:

```csharp
using Microsoft.EntityFrameworkCore;

// IPv4NumToString: Convert UInt32 to dotted-decimal string
context.Servers.Select(s => EF.Functions.IPv4NumToString(s.RawIp))
```

```sql
SELECT IPv4NumToString("RawIp") FROM "Servers"
```

```csharp
// IPv4StringToNum: Convert dotted-decimal string to UInt32
context.Logs.Select(l => EF.Functions.IPv4StringToNum(l.IpString))
```

```sql
SELECT IPv4StringToNum("IpString") FROM "Logs"
```

```csharp
// IsIPAddressInRange: Check if IP is within a CIDR range
context.Logs.Where(l => EF.Functions.IsIPAddressInRange(l.ClientIp, "10.0.0.0/8"))
```

```sql
SELECT * FROM "Logs" WHERE isIPAddressInRange("ClientIp", '10.0.0.0/8')
```

```csharp
// IsIPv4String / IsIPv6String: Validate IP address format
context.Logs.Where(l => EF.Functions.IsIPv4String(l.IpString))
context.Logs.Where(l => EF.Functions.IsIPv6String(l.IpString))
```

```sql
SELECT * FROM "Logs" WHERE isIPv4String("IpString")
SELECT * FROM "Logs" WHERE isIPv6String("IpString")
```

## Choosing the Right Type

| Use Case | Recommended Type | ClickHouse Type |
|----------|-----------------|-----------------|
| IPv4 only, need comparisons and sorting | `ClickHouseIPv4` | `IPv4` |
| IPv6 only | `ClickHouseIPv6` | `IPv6` |
| Mixed IPv4/IPv6, standard .NET interop | `IPAddress` | `IPv6` |
| IPv4 for storage, raw UInt32 operations | `uint` + IP functions | `UInt32` |

## Function Reference

| C# Method | ClickHouse SQL |
|-----------|----------------|
| `EF.Functions.IPv4NumToString(ip)` | `IPv4NumToString(ip)` |
| `EF.Functions.IPv4StringToNum(s)` | `IPv4StringToNum(s)` |
| `EF.Functions.IsIPAddressInRange(addr, cidr)` | `isIPAddressInRange(addr, cidr)` |
| `EF.Functions.IsIPv4String(s)` | `isIPv4String(s)` |
| `EF.Functions.IsIPv6String(s)` | `isIPv6String(s)` |

## See Also

- [Type System Overview](overview.md)
