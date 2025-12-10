using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// ClickHouse dictionary function stubs for use in LINQ expressions.
/// These methods are translated to ClickHouse dictGet functions by the query translator.
/// </summary>
/// <remarks>
/// <para>
/// These methods should never be invoked directly - they are stub methods that the
/// EF Core query translator intercepts and converts to ClickHouse SQL.
/// </para>
/// <para>
/// Example usage:
/// <code>
/// var result = context.Orders
///     .Select(o => new {
///         o.OrderId,
///         ProductName = EF.Functions.DictGet&lt;string&gt;("products_dict", "product_name", o.ProductId)
///     });
/// // Translates to: SELECT OrderId, dictGet('products_dict', 'product_name', ProductId) ...
/// </code>
/// </para>
/// </remarks>
public static class ClickHouseDictionaryFunctions
{
    private static T Throw<T>() =>
        throw new InvalidOperationException(
            "This method is a ClickHouse dictionary function stub and should not be invoked directly. " +
            "It is intended for use in LINQ expressions that are translated to ClickHouse SQL.");

    #region dictGet functions

    /// <summary>
    /// Retrieves an attribute value from a dictionary by key.
    /// Translates to: dictGet('dictName', 'attrName', key)
    /// </summary>
    /// <typeparam name="TValue">The type of the attribute value.</typeparam>
    /// <param name="_">The DbFunctions instance (not used).</param>
    /// <param name="dictionaryName">The name of the dictionary.</param>
    /// <param name="attributeName">The name of the attribute to retrieve.</param>
    /// <param name="key">The key to look up.</param>
    /// <returns>The attribute value for the given key.</returns>
    /// <remarks>
    /// If the key is not found in the dictionary, ClickHouse returns the default value
    /// for the attribute type (e.g., empty string for String, 0 for integers).
    /// </remarks>
    public static TValue DictGet<TValue>(
        this DbFunctions _,
        string dictionaryName,
        string attributeName,
        object key)
        => Throw<TValue>();

    /// <summary>
    /// Retrieves an attribute value from a dictionary by composite key.
    /// Translates to: dictGet('dictName', 'attrName', (key1, key2, ...))
    /// </summary>
    /// <typeparam name="TValue">The type of the attribute value.</typeparam>
    /// <param name="_">The DbFunctions instance (not used).</param>
    /// <param name="dictionaryName">The name of the dictionary.</param>
    /// <param name="attributeName">The name of the attribute to retrieve.</param>
    /// <param name="key1">The first part of the composite key.</param>
    /// <param name="key2">The second part of the composite key.</param>
    /// <returns>The attribute value for the given composite key.</returns>
    public static TValue DictGet<TValue>(
        this DbFunctions _,
        string dictionaryName,
        string attributeName,
        object key1,
        object key2)
        => Throw<TValue>();

    /// <summary>
    /// Retrieves an attribute value from a dictionary by composite key (3 parts).
    /// Translates to: dictGet('dictName', 'attrName', (key1, key2, key3))
    /// </summary>
    public static TValue DictGet<TValue>(
        this DbFunctions _,
        string dictionaryName,
        string attributeName,
        object key1,
        object key2,
        object key3)
        => Throw<TValue>();

    #endregion

    #region dictGetOrDefault functions

    /// <summary>
    /// Retrieves an attribute value from a dictionary, returning a default if the key is not found.
    /// Translates to: dictGetOrDefault('dictName', 'attrName', key, defaultValue)
    /// </summary>
    /// <typeparam name="TValue">The type of the attribute value.</typeparam>
    /// <param name="_">The DbFunctions instance (not used).</param>
    /// <param name="dictionaryName">The name of the dictionary.</param>
    /// <param name="attributeName">The name of the attribute to retrieve.</param>
    /// <param name="key">The key to look up.</param>
    /// <param name="defaultValue">The value to return if the key is not found.</param>
    /// <returns>The attribute value, or defaultValue if the key is not found.</returns>
    public static TValue DictGetOrDefault<TValue>(
        this DbFunctions _,
        string dictionaryName,
        string attributeName,
        object key,
        TValue defaultValue)
        => Throw<TValue>();

    /// <summary>
    /// Retrieves an attribute value from a dictionary with a composite key, returning a default if not found.
    /// Translates to: dictGetOrDefault('dictName', 'attrName', (key1, key2), defaultValue)
    /// </summary>
    public static TValue DictGetOrDefault<TValue>(
        this DbFunctions _,
        string dictionaryName,
        string attributeName,
        object key1,
        object key2,
        TValue defaultValue)
        => Throw<TValue>();

    #endregion

    #region dictHas function

    /// <summary>
    /// Checks if a key exists in a dictionary.
    /// Translates to: dictHas('dictName', key)
    /// </summary>
    /// <param name="_">The DbFunctions instance (not used).</param>
    /// <param name="dictionaryName">The name of the dictionary.</param>
    /// <param name="key">The key to check.</param>
    /// <returns>True if the key exists in the dictionary, false otherwise.</returns>
    public static bool DictHas(
        this DbFunctions _,
        string dictionaryName,
        object key)
        => Throw<bool>();

    /// <summary>
    /// Checks if a composite key exists in a dictionary.
    /// Translates to: dictHas('dictName', (key1, key2))
    /// </summary>
    public static bool DictHas(
        this DbFunctions _,
        string dictionaryName,
        object key1,
        object key2)
        => Throw<bool>();

    #endregion

    #region dictGetHierarchy function

    /// <summary>
    /// Retrieves the hierarchy of ancestor IDs from a hierarchical dictionary.
    /// Translates to: dictGetHierarchy('dictName', key)
    /// </summary>
    /// <param name="_">The DbFunctions instance (not used).</param>
    /// <param name="dictionaryName">The name of the hierarchical dictionary.</param>
    /// <param name="key">The key to get hierarchy for.</param>
    /// <returns>An array of ancestor IDs from child to root.</returns>
    public static ulong[] DictGetHierarchy(
        this DbFunctions _,
        string dictionaryName,
        ulong key)
        => Throw<ulong[]>();

    #endregion

    #region dictIsIn function

    /// <summary>
    /// Checks if a key is a descendant of another key in a hierarchical dictionary.
    /// Translates to: dictIsIn('dictName', childKey, ancestorKey)
    /// </summary>
    /// <param name="_">The DbFunctions instance (not used).</param>
    /// <param name="dictionaryName">The name of the hierarchical dictionary.</param>
    /// <param name="childKey">The potential descendant key.</param>
    /// <param name="ancestorKey">The potential ancestor key.</param>
    /// <returns>True if childKey is a descendant of ancestorKey.</returns>
    public static bool DictIsIn(
        this DbFunctions _,
        string dictionaryName,
        ulong childKey,
        ulong ancestorKey)
        => Throw<bool>();

    #endregion

    #region Range dictionary functions

    /// <summary>
    /// Retrieves an attribute value from a RANGE_HASHED dictionary for a specific date.
    /// Translates to: dictGet('dictName', 'attrName', key, date)
    /// </summary>
    /// <typeparam name="TValue">The type of the attribute value.</typeparam>
    /// <param name="_">The DbFunctions instance (not used).</param>
    /// <param name="dictionaryName">The name of the range dictionary.</param>
    /// <param name="attributeName">The name of the attribute to retrieve.</param>
    /// <param name="key">The key to look up.</param>
    /// <param name="date">The date for range lookup.</param>
    /// <returns>The attribute value valid for the given key and date.</returns>
    public static TValue DictGetForDate<TValue>(
        this DbFunctions _,
        string dictionaryName,
        string attributeName,
        object key,
        DateOnly date)
        => Throw<TValue>();

    /// <summary>
    /// Retrieves an attribute value from a RANGE_HASHED dictionary for a specific datetime.
    /// Translates to: dictGet('dictName', 'attrName', key, datetime)
    /// </summary>
    /// <typeparam name="TValue">The type of the attribute value.</typeparam>
    /// <param name="_">The DbFunctions instance (not used).</param>
    /// <param name="dictionaryName">The name of the range dictionary.</param>
    /// <param name="attributeName">The name of the attribute to retrieve.</param>
    /// <param name="key">The key to look up.</param>
    /// <param name="dateTime">The datetime for range lookup.</param>
    /// <returns>The attribute value valid for the given key and datetime.</returns>
    public static TValue DictGetForDateTime<TValue>(
        this DbFunctions _,
        string dictionaryName,
        string attributeName,
        object key,
        DateTime dateTime)
        => Throw<TValue>();

    #endregion
}
