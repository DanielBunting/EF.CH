# Math Functions

EF.CH translates .NET `System.Math` methods and `System.Convert` type-casting methods to their ClickHouse SQL equivalents. Over 30 mathematical functions and 12 type conversion functions are supported.

---

## Single-Argument Math Functions

These `Math.*` methods accept a single numeric argument and translate directly to their ClickHouse equivalents.

| C# Method | ClickHouse SQL | Description |
|-----------|----------------|-------------|
| `Math.Abs(x)` | `abs(x)` | Absolute value |
| `Math.Sign(x)` | `sign(x)` | Sign (-1, 0, or 1) |
| `Math.Sqrt(x)` | `sqrt(x)` | Square root |
| `Math.Cbrt(x)` | `cbrt(x)` | Cube root |
| `Math.Exp(x)` | `exp(x)` | e raised to the power x |
| `Math.Floor(x)` | `floor(x)` | Round down to nearest integer |
| `Math.Ceiling(x)` | `ceil(x)` | Round up to nearest integer |
| `Math.Truncate(x)` | `trunc(x)` | Truncate toward zero |
| `Math.Log(x)` | `log(x)` | Natural logarithm (base e) |
| `Math.Log10(x)` | `log10(x)` | Logarithm base 10 |
| `Math.Log2(x)` | `log2(x)` | Logarithm base 2 |
| `Math.Sin(x)` | `sin(x)` | Sine |
| `Math.Cos(x)` | `cos(x)` | Cosine |
| `Math.Tan(x)` | `tan(x)` | Tangent |
| `Math.Asin(x)` | `asin(x)` | Arc sine |
| `Math.Acos(x)` | `acos(x)` | Arc cosine |
| `Math.Atan(x)` | `atan(x)` | Arc tangent |
| `Math.Sinh(x)` | `sinh(x)` | Hyperbolic sine |
| `Math.Cosh(x)` | `cosh(x)` | Hyperbolic cosine |
| `Math.Tanh(x)` | `tanh(x)` | Hyperbolic tangent |
| `Math.Asinh(x)` | `asinh(x)` | Inverse hyperbolic sine |
| `Math.Acosh(x)` | `acosh(x)` | Inverse hyperbolic cosine |
| `Math.Atanh(x)` | `atanh(x)` | Inverse hyperbolic tangent |

```csharp
var results = await context.Measurements
    .Select(m => new
    {
        AbsDeviation = Math.Abs(m.Value - m.Expected),    // abs(Value - Expected)
        LogValue = Math.Log(m.Value),                      // log(Value)
        RootValue = Math.Sqrt(m.Value),                    // sqrt(Value)
        Direction = Math.Sign(m.Delta),                    // sign(Delta)
        Rounded = Math.Floor(m.Score)                      // floor(Score)
    })
    .ToListAsync();
```

---

## Two-Argument Math Functions

| C# Method | ClickHouse SQL | Description |
|-----------|----------------|-------------|
| `Math.Pow(x, y)` | `pow(x, y)` | x raised to the power y |
| `Math.Atan2(y, x)` | `atan2(y, x)` | Arc tangent of y/x |
| `Math.Min(x, y)` | `least(x, y)` | Smaller of two values |
| `Math.Max(x, y)` | `greatest(x, y)` | Larger of two values |
| `Math.Round(x, n)` | `round(x, n)` | Round to n decimal places |
| `Math.Log(value, base)` | `log(value) / log(base)` | Logarithm with custom base (computed) |

```csharp
var results = await context.Sensors
    .Select(s => new
    {
        Power = Math.Pow(s.Base, s.Exponent),          // pow(Base, Exponent)
        Clamped = Math.Max(0, Math.Min(100, s.Value)), // greatest(0, least(100, Value))
        Rounded = Math.Round(s.Measurement, 2),        // round(Measurement, 2)
        Angle = Math.Atan2(s.Y, s.X),                 // atan2(Y, X)
        LogBase3 = Math.Log(s.Value, 3)                // log(Value) / log(3)
    })
    .ToListAsync();
```

---

## Three-Argument Math Functions

| C# Method | ClickHouse SQL | Description |
|-----------|----------------|-------------|
| `Math.Clamp(value, min, max)` | `greatest(min, least(max, value))` | Constrain value to range [min, max] |
| `Math.FusedMultiplyAdd(x, y, z)` | `(x * y) + z` | Multiply-add in one operation |

```csharp
var results = await context.Readings
    .Select(r => new
    {
        // Clamp temperature between -40 and 85 degrees
        SafeTemp = Math.Clamp(r.Temperature, -40.0, 85.0),  // greatest(-40, least(85, Temperature))

        // Compute weighted score with offset
        WeightedScore = Math.FusedMultiplyAdd(r.Value, r.Weight, r.Offset) // (Value * Weight) + Offset
    })
    .ToListAsync();
```

---

## Convert Type Casting

`System.Convert` methods translate to ClickHouse type conversion functions. These are used for explicit type casting within LINQ queries.

| C# Method | ClickHouse SQL | Target Type |
|-----------|----------------|-------------|
| `Convert.ToBoolean(x)` | `toBool(x)` | `Bool` |
| `Convert.ToByte(x)` | `toUInt8(x)` | `UInt8` |
| `Convert.ToSByte(x)` | `toInt8(x)` | `Int8` |
| `Convert.ToInt16(x)` | `toInt16(x)` | `Int16` |
| `Convert.ToUInt16(x)` | `toUInt16(x)` | `UInt16` |
| `Convert.ToInt32(x)` | `toInt32(x)` | `Int32` |
| `Convert.ToUInt32(x)` | `toUInt32(x)` | `UInt32` |
| `Convert.ToInt64(x)` | `toInt64(x)` | `Int64` |
| `Convert.ToUInt64(x)` | `toUInt64(x)` | `UInt64` |
| `Convert.ToSingle(x)` | `toFloat32(x)` | `Float32` |
| `Convert.ToDouble(x)` | `toFloat64(x)` | `Float64` |
| `Convert.ToString(x)` | `toString(x)` | `String` |

```csharp
var results = await context.Events
    .Select(e => new
    {
        // Cast string to integer
        NumericId = Convert.ToInt64(e.ExternalId),          // toInt64(ExternalId)

        // Cast integer to float for division
        Ratio = Convert.ToDouble(e.Numerator) /
                Convert.ToDouble(e.Denominator),            // toFloat64(Numerator) / toFloat64(Denominator)

        // Cast to string for concatenation
        Label = Convert.ToString(e.Code),                   // toString(Code)

        // Cast to boolean
        IsActive = Convert.ToBoolean(e.StatusFlag)          // toBool(StatusFlag)
    })
    .ToListAsync();
```

### Type Casting in Aggregations

Type casting is particularly useful in GROUP BY and aggregate expressions to control ClickHouse's type system:

```csharp
var results = await context.Orders
    .GroupBy(o => o.Category)
    .Select(g => new
    {
        Category = g.Key,
        // Ensure floating-point division
        AvgItemCount = Convert.ToDouble(g.Sum(o => o.ItemCount)) /
                       Convert.ToDouble(g.Count())
    })
    .ToListAsync();
```

---

## Combining Math Functions

Math functions can be composed freely in LINQ expressions:

```csharp
var results = await context.Locations
    .Select(l => new
    {
        l.Id,
        // Haversine distance approximation
        Distance = Math.Acos(
            Math.Sin(l.Lat * Math.PI / 180) * Math.Sin(targetLat * Math.PI / 180) +
            Math.Cos(l.Lat * Math.PI / 180) * Math.Cos(targetLat * Math.PI / 180) *
            Math.Cos((l.Lon - targetLon) * Math.PI / 180)
        ) * 6371  // Earth radius in km
    })
    .OrderBy(x => x.Distance)
    .Take(10)
    .ToListAsync();
```

---

## See Also

- [Aggregate Functions](aggregate-functions.md) -- aggregate functions that can be combined with math operations
- [DateTime Functions](datetime-functions.md) -- date arithmetic and truncation
- [Utility Functions](utility-functions.md) -- formatting functions for human-readable output
