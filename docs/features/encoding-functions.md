# Encoding Functions

ClickHouse provides functions for encoding and decoding data in Base64 and hexadecimal formats. EF.CH exposes these as `EF.Functions` extensions.

## Available Functions

| C# Method | ClickHouse SQL | Return Type | Description |
|-----------|---------------|-------------|-------------|
| `Base64Encode(s)` | `base64Encode(s)` | `string` | Encode string to Base64 |
| `Base64Decode(s)` | `base64Decode(s)` | `string` | Decode Base64 string |
| `Hex(value)` | `hex(x)` | `string` | Hexadecimal representation |
| `Unhex(s)` | `unhex(s)` | `string` | Hex string to bytes/string |

`Hex` is generic — it works with strings, numbers, and any other column type.

## Usage Examples

### Base64 Encoding

```csharp
using EF.CH.Extensions;

var encoded = await context.Users
    .Select(u => new
    {
        u.Name,
        Token = EF.Functions.Base64Encode(u.Email)
    })
    .ToListAsync();
```

Generates:
```sql
SELECT "Name", base64Encode("Email") AS "Token"
FROM "Users"
```

### Hex Representation

```csharp
// Get hex representation of user IDs for debugging
var hex = await context.Events
    .Select(e => new
    {
        e.Id,
        IdHex = EF.Functions.Hex(e.Id)
    })
    .Take(10)
    .ToListAsync();
```

### Round-Trip Encoding

```csharp
// Encode and decode in the same query
var roundTrip = await context.Messages
    .Select(m => new
    {
        Original = m.Body,
        Encoded = EF.Functions.Base64Encode(m.Body),
        Decoded = EF.Functions.Base64Decode(EF.Functions.Base64Encode(m.Body))
    })
    .FirstAsync();
// roundTrip.Original == roundTrip.Decoded
```

## Notes

- `Base64Decode` throws a ClickHouse exception if the input is not valid Base64.
- `Hex` returns uppercase hex characters (e.g. `"48656C6C6F"`).
- For hash functions that return hex strings (`Md5`, `Sha256`), see [Hash Functions](hash-functions.md).

## Learn More

- [ClickHouse Encoding Functions](https://clickhouse.com/docs/en/sql-reference/functions/encoding-functions)
