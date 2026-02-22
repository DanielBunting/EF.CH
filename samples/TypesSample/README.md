# Types Sample

Demonstrates the ClickHouse type system through EF.CH, showing how .NET types map to ClickHouse native types.

## What it demonstrates

| Demo | .NET Type | ClickHouse Type | Key Operations |
|------|-----------|-----------------|----------------|
| Arrays | `string[]`, `List<int>` | `Array(String)`, `Array(Int32)` | `Contains`, `Count` |
| Maps | `Dictionary<string, string>` | `Map(String, String)` | `ContainsKey`, `Keys` |
| Enums | `Priority` enum | `Enum8(...)` | Filter by enum value |
| JSON | `JsonElement` | `JSON` | `GetPath<T>`, `HasPath` |
| Nested | `List<AddressRecord>` | `Nested(Street String, City String)` | Insert and read structured data |
| IP Addresses | `ClickHouseIPv4` | `IPv4` | `IsIPAddressInRange` |
| Tuples | `(string, int)` | `Tuple(String, Int32)` | Insert and read tuple values |

## Prerequisites

- [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0)
- A running ClickHouse instance (Docker is the easiest option)
- ClickHouse 24.8+ is required for the JSON demo

## How to run

1. Start a ClickHouse server:

```bash
docker run -d --name clickhouse -p 8123:8123 clickhouse/clickhouse-server:latest
```

2. Run the sample:

```bash
dotnet run
```

## Expected output

```
=== EF.CH Types Sample ===

[1] Arrays (string[], List<int>)
    ClickHouse type: Array(String), Array(Int32)

    Articles tagged 'csharp':
      - EF Core Guide (tags: [csharp, efcore, dotnet], scores count: 3)
      - LINQ Deep Dive (tags: [csharp, linq], scores count: 1)

[2] Maps (Dictionary<string, string>)
    ClickHouse type: Map(String, String)

    Entries with 'region' key:
      - web-server: region=us-east-1

[3] Enums (Priority enum)
    ClickHouse type: Enum8('Low'=0, 'Medium'=1, 'High'=2)

    High-priority tickets:
      - Fix login bug (High)
      - Security patch (High)

[4] JSON (JsonElement)
    ClickHouse type: JSON (requires CH 24.8+)

    Telemetry data:
      - User=Alice, Score=95, HasPlan=True
      - User=Bob, Score=42, HasPlan=False

[5] Nested (List<AddressRecord>)
    ClickHouse type: Nested(Street String, City String)

    Contacts:
      - Alice (2 address(es))
          123 Main St, Springfield
          456 Oak Ave, Shelbyville
      - Bob (1 address(es))
          789 Elm Blvd, Capital City

[6] IP Addresses (ClickHouseIPv4)
    ClickHouse type: IPv4

    Access logs from 192.168.0.0/16:
      - 192.168.1.1 -> /api/users
      - 192.168.1.100 -> /api/users

[7] Tuples ((string, int))
    ClickHouse type: Tuple(String, Int32)

    Measurements:
      - Humidity: Building=Building B, Floor=1
      - Pressure: Building=Building A, Floor=2
      - Temperature: Building=Building A, Floor=3

=== Done ===
```

## Notes

- Each demo creates its own table, inserts data, runs queries, then cleans up.
- The JSON demo requires ClickHouse 24.8 or later for native JSON type support. If your ClickHouse version is older, that demo will fail with a type error.
- Enums are automatically mapped to `Enum8` or `Enum16` based on the range of values.
- Nested types use parallel arrays internally -- `List<AddressRecord>` becomes `Nested(Street String, City String)` in ClickHouse storage.
