using System.Linq.Expressions;

namespace EF.CH.Dictionaries;

/// <summary>
/// Internal metadata for a ClickHouse dictionary configuration.
/// </summary>
/// <typeparam name="TDictionary">The dictionary entity type.</typeparam>
/// <typeparam name="TKey">The dictionary key type.</typeparam>
public sealed class DictionaryMetadata<TDictionary, TKey>
    where TDictionary : class
{
    /// <summary>
    /// Creates a new dictionary metadata instance.
    /// </summary>
    /// <param name="name">The dictionary name.</param>
    /// <param name="keyType">The key type.</param>
    /// <param name="entityType">The entity type.</param>
    /// <param name="keyPropertyName">The name of the key property.</param>
    public DictionaryMetadata(string name, Type keyType, Type entityType, string keyPropertyName)
    {
        Name = name;
        KeyType = keyType;
        EntityType = entityType;
        KeyPropertyName = keyPropertyName;
    }

    /// <summary>
    /// The dictionary name in ClickHouse.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// The CLR type of the dictionary key.
    /// </summary>
    public Type KeyType { get; }

    /// <summary>
    /// The CLR type of the dictionary entity.
    /// </summary>
    public Type EntityType { get; }

    /// <summary>
    /// The name of the property that serves as the dictionary key.
    /// </summary>
    public string KeyPropertyName { get; }
}

/// <summary>
/// Non-generic base class for dictionary metadata storage.
/// Used for annotation storage where generics are not available.
/// </summary>
internal sealed class DictionaryMetadataBase
{
    /// <summary>
    /// The dictionary name in ClickHouse.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// The CLR type of the dictionary entity.
    /// </summary>
    public required Type EntityType { get; init; }

    /// <summary>
    /// The CLR type of the source entity.
    /// </summary>
    public required Type SourceType { get; init; }

    /// <summary>
    /// The CLR type of the dictionary key.
    /// </summary>
    public required Type KeyType { get; init; }

    /// <summary>
    /// The names of the key columns.
    /// </summary>
    public required string[] KeyColumns { get; init; }

    /// <summary>
    /// Whether this is a composite key dictionary.
    /// </summary>
    public bool IsCompositeKey => KeyColumns.Length > 1;

    /// <summary>
    /// The dictionary layout type.
    /// </summary>
    public DictionaryLayout Layout { get; init; } = DictionaryLayout.Hashed;

    /// <summary>
    /// Layout-specific options.
    /// </summary>
    public Dictionary<string, object>? LayoutOptions { get; init; }

    /// <summary>
    /// Minimum lifetime in seconds for refresh.
    /// </summary>
    public int LifetimeMinSeconds { get; init; }

    /// <summary>
    /// Maximum lifetime in seconds for refresh.
    /// </summary>
    public int LifetimeMaxSeconds { get; init; } = 300;

    /// <summary>
    /// Default values for dictionary attributes.
    /// </summary>
    public Dictionary<string, object>? Defaults { get; init; }

    /// <summary>
    /// The projection expression for selecting columns from the source.
    /// </summary>
    public LambdaExpression? ProjectionExpression { get; init; }

    /// <summary>
    /// The filter expression for filtering source data.
    /// </summary>
    public LambdaExpression? FilterExpression { get; init; }
}
