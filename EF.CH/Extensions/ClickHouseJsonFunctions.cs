using System.Text.Json;

namespace EF.CH.Extensions;

/// <summary>
/// ClickHouse JSON path extraction methods for use in LINQ expressions.
/// These methods are translated to ClickHouse native JSON subcolumn access syntax.
/// </summary>
/// <remarks>
/// <para>
/// Requires ClickHouse 24.8+ for native JSON type support.
/// </para>
/// <para>
/// These are stub methods - they throw at runtime if accidentally invoked directly.
/// They are intended for use in LINQ queries that will be translated to ClickHouse SQL.
/// </para>
/// <para>
/// Example usage:
/// <code>
/// context.Events
///     .Where(e => e.Metadata.GetPath&lt;string&gt;("user.email") == "test@example.com")
///     .Select(e => new {
///         e.Id,
///         Score = e.Metadata.GetPath&lt;int&gt;("metrics.score"),
///         FirstTag = e.Metadata.GetPath&lt;string&gt;("tags[0]")
///     });
/// </code>
/// </para>
/// </remarks>
public static class ClickHouseJsonFunctions
{
    private static T Throw<T>() =>
        throw new InvalidOperationException(
            "This method is a ClickHouse JSON translation stub and should not be invoked directly. " +
            "It is intended for use in LINQ expressions that are translated to ClickHouse SQL.");

    #region GetPath - Extract typed value at path

    /// <summary>
    /// Gets a typed value at the specified JSON path.
    /// Translates to ClickHouse subcolumn syntax: "column"."path"."subpath"
    /// </summary>
    /// <typeparam name="T">The expected type at the path.</typeparam>
    /// <param name="json">The JSON element.</param>
    /// <param name="path">Dot-separated path (e.g., "user.email" or "tags[0]").</param>
    /// <returns>The typed value at the path, or default if not found.</returns>
    /// <example>
    /// <code>
    /// // Simple path
    /// var email = e.Metadata.GetPath&lt;string&gt;("user.email");
    /// // Translates to: "Metadata"."user"."email"
    ///
    /// // Array index
    /// var firstTag = e.Metadata.GetPath&lt;string&gt;("tags[0]");
    /// // Translates to: "Metadata"."tags"[1]  (ClickHouse is 1-indexed)
    ///
    /// // Nested path with array
    /// var itemName = e.Metadata.GetPath&lt;string&gt;("order.items[0].name");
    /// // Translates to: "Metadata"."order"."items"[1]."name"
    /// </code>
    /// </example>
    public static T GetPath<T>(this JsonElement json, string path) => Throw<T>();

    /// <summary>
    /// Gets a typed value at the specified JSON path from a JsonDocument.
    /// </summary>
    public static T GetPath<T>(this JsonDocument json, string path) => Throw<T>();

    #endregion

    #region GetPathOrDefault - Extract with fallback

    /// <summary>
    /// Gets a typed value at the specified JSON path, or a default value if not found.
    /// Translates to: ifNull("column"."path", default)
    /// </summary>
    /// <typeparam name="T">The expected type at the path.</typeparam>
    /// <param name="json">The JSON element.</param>
    /// <param name="path">Dot-separated path.</param>
    /// <param name="defaultValue">Value to return if path doesn't exist or is null.</param>
    /// <returns>The value at the path or the default value.</returns>
    /// <example>
    /// <code>
    /// var score = e.Metadata.GetPathOrDefault&lt;int&gt;("metrics.score", 0);
    /// // Translates to: ifNull("Metadata"."metrics"."score", 0)
    /// </code>
    /// </example>
    public static T GetPathOrDefault<T>(this JsonElement json, string path, T defaultValue) => Throw<T>();

    /// <summary>
    /// Gets a typed value at the specified JSON path, or a default value if not found.
    /// </summary>
    public static T GetPathOrDefault<T>(this JsonDocument json, string path, T defaultValue) => Throw<T>();

    #endregion

    #region HasPath - Check path existence

    /// <summary>
    /// Checks if a path exists in the JSON and is not null.
    /// Translates to: "column"."path" IS NOT NULL
    /// </summary>
    /// <param name="json">The JSON element.</param>
    /// <param name="path">Dot-separated path to check.</param>
    /// <returns>True if the path exists and is not null.</returns>
    /// <example>
    /// <code>
    /// context.Events.Where(e => e.Metadata.HasPath("premium.features"))
    /// // Translates to: WHERE "Metadata"."premium"."features" IS NOT NULL
    /// </code>
    /// </example>
    public static bool HasPath(this JsonElement json, string path) => Throw<bool>();

    /// <summary>
    /// Checks if a path exists in the JSON document and is not null.
    /// </summary>
    public static bool HasPath(this JsonDocument json, string path) => Throw<bool>();

    #endregion

    #region GetArray - Extract array at path

    /// <summary>
    /// Gets an array at the specified JSON path.
    /// Translates to: "column"."path"
    /// </summary>
    /// <typeparam name="T">The element type of the array.</typeparam>
    /// <param name="json">The JSON element.</param>
    /// <param name="path">Dot-separated path to the array.</param>
    /// <returns>The array at the path.</returns>
    /// <example>
    /// <code>
    /// var tags = e.Metadata.GetArray&lt;string&gt;("tags");
    /// // Translates to: "Metadata"."tags"
    /// </code>
    /// </example>
    public static T[] GetArray<T>(this JsonElement json, string path) => Throw<T[]>();

    /// <summary>
    /// Gets an array at the specified JSON path from a JsonDocument.
    /// </summary>
    public static T[] GetArray<T>(this JsonDocument json, string path) => Throw<T[]>();

    #endregion

    #region GetObject - Extract nested object

    /// <summary>
    /// Gets a nested object at the specified JSON path as JsonElement.
    /// Translates to: "column"."path"
    /// </summary>
    /// <param name="json">The JSON element.</param>
    /// <param name="path">Dot-separated path to the nested object.</param>
    /// <returns>The JsonElement at the path.</returns>
    /// <example>
    /// <code>
    /// var userInfo = e.Metadata.GetObject("user");
    /// // Then access: userInfo.GetPath&lt;string&gt;("email")
    /// </code>
    /// </example>
    public static JsonElement GetObject(this JsonElement json, string path) => Throw<JsonElement>();

    /// <summary>
    /// Gets a nested object at the specified JSON path from a JsonDocument.
    /// </summary>
    public static JsonElement GetObject(this JsonDocument json, string path) => Throw<JsonElement>();

    #endregion
}
