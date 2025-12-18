using Microsoft.EntityFrameworkCore.Metadata;

namespace EF.CH.Dictionaries;

/// <summary>
/// Resolves external dictionary configuration at runtime.
/// Handles credential resolution from environment variables, configuration, and profiles.
/// </summary>
public interface IDictionaryConfigResolver
{
    /// <summary>
    /// Checks if an entity type is configured as a dictionary.
    /// </summary>
    /// <param name="entityType">The entity type to check.</param>
    /// <returns>True if the entity is configured as a dictionary.</returns>
    bool IsDictionary(IEntityType entityType);

    /// <summary>
    /// Checks if a dictionary has an external source (PostgreSQL, MySQL, HTTP)
    /// that requires runtime credential resolution.
    /// </summary>
    /// <param name="entityType">The entity type to check.</param>
    /// <returns>True if the dictionary uses an external source, false for ClickHouse table sources.</returns>
    bool IsExternalDictionary(IEntityType entityType);

    /// <summary>
    /// Gets the source provider type for a dictionary.
    /// </summary>
    /// <param name="entityType">The entity type to check.</param>
    /// <returns>The provider name: "clickhouse", "postgresql", "mysql", or "http".</returns>
    string GetSourceProvider(IEntityType entityType);

    /// <summary>
    /// Generates CREATE DICTIONARY DDL with resolved credentials.
    /// Credentials are resolved at call time from environment variables or IConfiguration.
    /// </summary>
    /// <param name="entityType">The dictionary entity type.</param>
    /// <param name="ifNotExists">Whether to use CREATE DICTIONARY IF NOT EXISTS.</param>
    /// <returns>The complete CREATE DICTIONARY SQL statement.</returns>
    /// <exception cref="InvalidOperationException">If configuration is incomplete or credentials cannot be resolved.</exception>
    string GenerateCreateDictionaryDdl(IEntityType entityType, bool ifNotExists = true);

    /// <summary>
    /// Generates DROP DICTIONARY DDL.
    /// </summary>
    /// <param name="entityType">The dictionary entity type.</param>
    /// <param name="ifExists">Whether to use DROP DICTIONARY IF EXISTS.</param>
    /// <returns>The DROP DICTIONARY SQL statement.</returns>
    string GenerateDropDictionaryDdl(IEntityType entityType, bool ifExists = true);

    /// <summary>
    /// Generates SYSTEM RELOAD DICTIONARY statement.
    /// </summary>
    /// <param name="entityType">The dictionary entity type.</param>
    /// <returns>The SYSTEM RELOAD DICTIONARY SQL statement.</returns>
    string GenerateReloadDictionaryDdl(IEntityType entityType);
}
