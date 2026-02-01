namespace EF.CH.Query;

/// <summary>
/// Specifies the comparison operator for ASOF JOIN conditions.
/// ASOF JOIN matches rows based on the closest value rather than exact equality.
/// </summary>
public enum AsofJoinType
{
    /// <summary>
    /// Left >= Right (most common). Finds the latest right row before or at the left row's timestamp.
    /// Example: For a trade at 10:05, finds the quote at or before 10:05.
    /// </summary>
    GreaterOrEqual,

    /// <summary>
    /// Left > Right. Finds the latest right row strictly before the left row's timestamp.
    /// Example: For a trade at 10:05, finds the quote strictly before 10:05.
    /// </summary>
    Greater,

    /// <summary>
    /// Left &lt;= Right. Finds the earliest right row at or after the left row's timestamp.
    /// Example: For a trade at 10:05, finds the first quote at or after 10:05.
    /// </summary>
    LessOrEqual,

    /// <summary>
    /// Left &lt; Right. Finds the earliest right row strictly after the left row's timestamp.
    /// Example: For a trade at 10:05, finds the first quote strictly after 10:05.
    /// </summary>
    Less
}
