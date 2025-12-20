namespace EF.CH.Metadata.Attributes;

/// <summary>
/// Specifies the compression codec for this column.
/// </summary>
/// <remarks>
/// <para>
/// Codecs can be chained for optimal compression. Apply preprocessing codecs first
/// (Delta, DoubleDelta, Gorilla, T64), followed by compression codecs (LZ4, ZSTD).
/// </para>
/// <para>
/// Example: <c>[ClickHouseCodec("DoubleDelta, LZ4")]</c> applies DoubleDelta encoding first, then LZ4 compression.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// public class Metrics
/// {
///     [ClickHouseCodec("DoubleDelta, LZ4")]
///     public DateTime Timestamp { get; set; }
///
///     [ClickHouseCodec("Delta, ZSTD(3)")]
///     public long SensorId { get; set; }
///
///     [ClickHouseCodec("ZSTD(9)")]
///     public string RawPayload { get; set; }
/// }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class ClickHouseCodecAttribute : Attribute
{
    /// <summary>
    /// Gets the codec specification string.
    /// </summary>
    public string CodecSpec { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseCodecAttribute"/> class.
    /// </summary>
    /// <param name="codecSpec">The codec specification string (e.g., "DoubleDelta, LZ4" or "ZSTD(9)").</param>
    /// <exception cref="ArgumentException">If codecSpec is null or whitespace.</exception>
    public ClickHouseCodecAttribute(string codecSpec)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(codecSpec);
        CodecSpec = codecSpec;
    }
}

/// <summary>
/// Optimal codec for timestamp columns (DoubleDelta + LZ4).
/// </summary>
/// <remarks>
/// DoubleDelta stores the delta of deltas, which is ideal for timestamps that
/// increment by similar amounts. LZ4 provides fast compression of the encoded data.
/// </remarks>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class TimestampCodecAttribute : ClickHouseCodecAttribute
{
    /// <summary>
    /// Initializes a new instance of the <see cref="TimestampCodecAttribute"/> class.
    /// </summary>
    public TimestampCodecAttribute() : base("DoubleDelta, LZ4") { }
}

/// <summary>
/// Optimal codec for sequential/monotonic columns (Delta + ZSTD).
/// </summary>
/// <remarks>
/// Delta stores differences between consecutive values, ideal for auto-incrementing IDs
/// or monotonically increasing counters. ZSTD provides good compression of the deltas.
/// </remarks>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class SequentialCodecAttribute : ClickHouseCodecAttribute
{
    /// <summary>
    /// Initializes a new instance of the <see cref="SequentialCodecAttribute"/> class.
    /// </summary>
    public SequentialCodecAttribute() : base("Delta, ZSTD") { }
}

/// <summary>
/// Optimal codec for floating-point sensor data (Gorilla + ZSTD).
/// </summary>
/// <remarks>
/// Gorilla uses XOR-based encoding optimized for floating-point values that change slowly.
/// ZSTD provides additional compression. Do not combine with Delta or DoubleDelta.
/// </remarks>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class FloatCodecAttribute : ClickHouseCodecAttribute
{
    /// <summary>
    /// Initializes a new instance of the <see cref="FloatCodecAttribute"/> class.
    /// </summary>
    public FloatCodecAttribute() : base("Gorilla, ZSTD(1)") { }
}

/// <summary>
/// High compression for large text/binary data (ZSTD level 9).
/// </summary>
/// <remarks>
/// ZSTD level 9 provides high compression ratio at the cost of slower compression speed.
/// Ideal for large payloads like JSON, XML, or binary blobs where storage is more important than write speed.
/// </remarks>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class HighCompressionCodecAttribute : ClickHouseCodecAttribute
{
    /// <summary>
    /// Initializes a new instance of the <see cref="HighCompressionCodecAttribute"/> class.
    /// </summary>
    public HighCompressionCodecAttribute() : base("ZSTD(9)") { }
}

/// <summary>
/// Optimal codec for integers with sparse values (T64 + LZ4).
/// </summary>
/// <remarks>
/// T64 transposes a 64x64 bit matrix and strips unused high bits, ideal for integers
/// that don't use their full range. LZ4 provides fast compression.
/// </remarks>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class IntegerCodecAttribute : ClickHouseCodecAttribute
{
    /// <summary>
    /// Initializes a new instance of the <see cref="IntegerCodecAttribute"/> class.
    /// </summary>
    public IntegerCodecAttribute() : base("T64, LZ4") { }
}

/// <summary>
/// Disables compression for this column.
/// </summary>
/// <remarks>
/// Use this for columns where compression overhead outweighs benefits,
/// or for already-compressed data (e.g., pre-compressed images).
/// </remarks>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class NoCompressionAttribute : ClickHouseCodecAttribute
{
    /// <summary>
    /// Initializes a new instance of the <see cref="NoCompressionAttribute"/> class.
    /// </summary>
    public NoCompressionAttribute() : base("None") { }
}
