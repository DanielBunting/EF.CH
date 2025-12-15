using Microsoft.EntityFrameworkCore.Metadata;

namespace EF.CH.External;

/// <summary>
/// Resolves external table function configuration at runtime.
/// Handles credential resolution from environment variables, configuration, and profiles.
/// </summary>
public interface IExternalConfigResolver
{
    /// <summary>
    /// Builds the table function call for any external entity type.
    /// Dispatches to the appropriate provider-specific resolver based on the entity's provider annotation.
    /// </summary>
    /// <param name="entityType">The entity type with external configuration.</param>
    /// <returns>The complete function call string.</returns>
    /// <exception cref="NotSupportedException">If the provider is not supported.</exception>
    string ResolveTableFunction(IEntityType entityType);

    /// <summary>
    /// Builds the postgresql(...) table function call for an entity type.
    /// Resolves credentials from environment variables or configuration at call time.
    /// </summary>
    /// <param name="entityType">The entity type with external configuration.</param>
    /// <returns>The complete function call string (e.g., "postgresql('host:port', 'db', 'table', 'user', 'pass', 'schema')").</returns>
    /// <exception cref="InvalidOperationException">If configuration is incomplete or credentials cannot be resolved.</exception>
    string ResolvePostgresTableFunction(IEntityType entityType);

    /// <summary>
    /// Builds the mysql(...) table function call for an entity type.
    /// Resolves credentials from environment variables or configuration at call time.
    /// </summary>
    /// <param name="entityType">The entity type with external configuration.</param>
    /// <returns>The complete function call string (e.g., "mysql('host:port', 'db', 'table', 'user', 'pass')").</returns>
    string ResolveMySqlTableFunction(IEntityType entityType);

    /// <summary>
    /// Builds the odbc(...) table function call for an entity type.
    /// Resolves DSN from environment variables or configuration at call time.
    /// </summary>
    /// <param name="entityType">The entity type with external configuration.</param>
    /// <returns>The complete function call string (e.g., "odbc('DSN', 'database', 'table')").</returns>
    string ResolveOdbcTableFunction(IEntityType entityType);

    /// <summary>
    /// Builds the redis(...) table function call for an entity type.
    /// Resolves credentials from environment variables or configuration at call time.
    /// Auto-generates structure from entity properties if not explicitly specified.
    /// </summary>
    /// <param name="entityType">The entity type with external configuration.</param>
    /// <returns>The complete function call string (e.g., "redis('host:port', 'key', 'structure', db_index, 'password')").</returns>
    string ResolveRedisTableFunction(IEntityType entityType);

    /// <summary>
    /// Checks if an entity type is configured as an external table function.
    /// </summary>
    /// <param name="entityType">The entity type to check.</param>
    /// <returns>True if the entity uses a table function, false otherwise.</returns>
    bool IsExternalTableFunction(IEntityType entityType);

    /// <summary>
    /// Checks if inserts are enabled for an external entity.
    /// </summary>
    /// <param name="entityType">The entity type to check.</param>
    /// <returns>True if inserts are allowed (AllowInserts() was called), false if read-only.</returns>
    bool AreInsertsEnabled(IEntityType entityType);
}
