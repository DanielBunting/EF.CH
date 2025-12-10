namespace EF.CH.Dictionaries;

/// <summary>
/// Represents the LIFETIME configuration for a ClickHouse dictionary.
/// Controls how often the dictionary is refreshed from its source.
/// </summary>
/// <param name="MinSeconds">Minimum seconds between refreshes.</param>
/// <param name="MaxSeconds">Maximum seconds between refreshes. If null, uses fixed interval.</param>
public readonly record struct DictionaryLifetime(int MinSeconds, int? MaxSeconds = null)
{
    /// <summary>
    /// Creates a lifetime with a random refresh interval between min and max seconds.
    /// The randomization helps prevent thundering herd when multiple dictionaries refresh.
    /// </summary>
    public static DictionaryLifetime Range(int minSeconds, int maxSeconds)
        => new(minSeconds, maxSeconds);

    /// <summary>
    /// Creates a lifetime with a fixed refresh interval.
    /// </summary>
    public static DictionaryLifetime Fixed(int seconds)
        => new(seconds, null);

    /// <summary>
    /// Creates a lifetime of zero - dictionary is never automatically refreshed.
    /// Use for static data that doesn't change.
    /// </summary>
    public static DictionaryLifetime NoRefresh => new(0, null);

    /// <summary>
    /// Generates the LIFETIME(...) SQL clause.
    /// </summary>
    public string ToSql()
    {
        if (MaxSeconds.HasValue)
            return $"LIFETIME(MIN {MinSeconds} MAX {MaxSeconds.Value})";

        return $"LIFETIME({MinSeconds})";
    }
}
