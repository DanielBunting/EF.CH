using System.Linq.Expressions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.External;

/// <summary>
/// Builder for configuring external remote ClickHouse entities that use the remote() or remoteSecure() table function.
/// External entities do not create ClickHouse tables - they query remote ClickHouse servers directly via table functions.
/// </summary>
/// <typeparam name="TEntity">The entity type.</typeparam>
public class ExternalRemoteEntityBuilder<TEntity> where TEntity : class
{
    private readonly EntityTypeBuilder<TEntity> _entityBuilder;
    private readonly RemoteConfig _config = new();

    internal ExternalRemoteEntityBuilder(ModelBuilder modelBuilder)
    {
        _entityBuilder = modelBuilder.Entity<TEntity>();
    }

    /// <summary>
    /// Specifies the remote server addresses.
    /// Can be a single address, comma-separated list, or expression like "{shard1,shard2}".
    /// </summary>
    /// <param name="addresses">The remote server address(es).</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalRemoteEntityBuilder<TEntity> FromAddresses(string addresses)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(addresses);
        _config.Addresses = addresses;
        return this;
    }

    /// <summary>
    /// Specifies the remote server addresses from an environment variable.
    /// </summary>
    /// <param name="envVar">The environment variable name.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalRemoteEntityBuilder<TEntity> FromAddressesEnv(string envVar)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(envVar);
        _config.AddressesEnv = envVar;
        return this;
    }

    /// <summary>
    /// Specifies the remote database and table.
    /// </summary>
    /// <param name="database">The database name.</param>
    /// <param name="table">The table name.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalRemoteEntityBuilder<TEntity> FromTable(string database, string table)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        _config.Database = database;
        _config.Table = table;
        return this;
    }

    /// <summary>
    /// Specifies the database from an environment variable.
    /// </summary>
    /// <param name="envVar">The environment variable name.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalRemoteEntityBuilder<TEntity> DatabaseEnv(string envVar)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(envVar);
        _config.DatabaseEnv = envVar;
        return this;
    }

    /// <summary>
    /// Configures the connection credentials.
    /// </summary>
    /// <param name="configure">Action to configure credentials.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalRemoteEntityBuilder<TEntity> Connection(Action<RemoteConnectionBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var builder = new RemoteConnectionBuilder(_config);
        configure(builder);
        return this;
    }

    /// <summary>
    /// Sets the sharding key for distributed queries.
    /// </summary>
    /// <param name="shardingKey">The sharding key expression.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalRemoteEntityBuilder<TEntity> WithShardingKey(string shardingKey)
    {
        _config.ShardingKey = shardingKey;
        return this;
    }

    /// <summary>
    /// Configures a property mapping with optional customization.
    /// </summary>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="propertyExpression">Expression selecting the property.</param>
    /// <param name="configure">Optional action to configure the property.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalRemoteEntityBuilder<TEntity> Property<TProperty>(
        Expression<Func<TEntity, TProperty>> propertyExpression,
        Action<PropertyBuilder<TProperty>>? configure = null)
    {
        var propBuilder = _entityBuilder.Property(propertyExpression);
        configure?.Invoke(propBuilder);
        return this;
    }

    /// <summary>
    /// Ignores a property (won't be included in queries).
    /// </summary>
    /// <param name="propertyName">The name of the property to ignore.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalRemoteEntityBuilder<TEntity> Ignore(string propertyName)
    {
        _entityBuilder.Ignore(propertyName);
        return this;
    }

    /// <summary>
    /// Applies the configuration to the entity type.
    /// Called automatically by ExternalRemoteEntity extension method.
    /// </summary>
    internal void Build()
    {
        _entityBuilder.HasNoKey();

        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction, true);
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalProvider, "remote");
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalReadOnly, true);

        if (!string.IsNullOrEmpty(_config.Addresses))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalRemoteAddresses, _config.Addresses);
        }

        if (!string.IsNullOrEmpty(_config.AddressesEnv))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalRemoteAddressesEnv, _config.AddressesEnv);
        }

        if (!string.IsNullOrEmpty(_config.Database))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalRemoteDatabase, _config.Database);
        }

        if (!string.IsNullOrEmpty(_config.DatabaseEnv))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalRemoteDatabaseEnv, _config.DatabaseEnv);
        }

        if (!string.IsNullOrEmpty(_config.Table))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalRemoteTable, _config.Table);
        }

        if (!string.IsNullOrEmpty(_config.UserEnv))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalRemoteUserEnv, _config.UserEnv);
        }

        if (!string.IsNullOrEmpty(_config.UserValue))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalRemoteUserValue, _config.UserValue);
        }

        if (!string.IsNullOrEmpty(_config.PasswordEnv))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalRemotePasswordEnv, _config.PasswordEnv);
        }

        if (!string.IsNullOrEmpty(_config.PasswordValue))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalRemotePasswordValue, _config.PasswordValue);
        }

        if (!string.IsNullOrEmpty(_config.ShardingKey))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalRemoteShardingKey, _config.ShardingKey);
        }
    }

    internal class RemoteConfig
    {
        public string? Addresses { get; set; }
        public string? AddressesEnv { get; set; }
        public string? Database { get; set; }
        public string? DatabaseEnv { get; set; }
        public string? Table { get; set; }
        public string? UserEnv { get; set; }
        public string? UserValue { get; set; }
        public string? PasswordEnv { get; set; }
        public string? PasswordValue { get; set; }
        public string? ShardingKey { get; set; }
    }
}

/// <summary>
/// Builder for configuring remote ClickHouse connection credentials.
/// </summary>
public class RemoteConnectionBuilder
{
    private readonly ExternalRemoteEntityBuilder<object>.RemoteConfig _config;

    internal RemoteConnectionBuilder(object config)
    {
        _config = (ExternalRemoteEntityBuilder<object>.RemoteConfig)config;
    }

    /// <summary>
    /// Sets the username from an environment variable.
    /// </summary>
    /// <param name="envVar">The environment variable name.</param>
    /// <returns>The builder for chaining.</returns>
    public RemoteConnectionBuilder User(string envVar)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(envVar);
        _config.UserEnv = envVar;
        return this;
    }

    /// <summary>
    /// Sets the username from a literal value or environment variable.
    /// </summary>
    /// <param name="env">The environment variable name (preferred).</param>
    /// <param name="value">The literal value.</param>
    /// <returns>The builder for chaining.</returns>
    public RemoteConnectionBuilder User(string? env = null, string? value = null)
    {
        if (!string.IsNullOrEmpty(env)) _config.UserEnv = env;
        if (!string.IsNullOrEmpty(value)) _config.UserValue = value;
        return this;
    }

    /// <summary>
    /// Sets the password from an environment variable.
    /// </summary>
    /// <param name="envVar">The environment variable name.</param>
    /// <returns>The builder for chaining.</returns>
    public RemoteConnectionBuilder Password(string envVar)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(envVar);
        _config.PasswordEnv = envVar;
        return this;
    }

    /// <summary>
    /// Sets the password from a literal value or environment variable.
    /// </summary>
    /// <param name="env">The environment variable name (preferred).</param>
    /// <param name="value">The literal value (not recommended for production).</param>
    /// <returns>The builder for chaining.</returns>
    public RemoteConnectionBuilder Password(string? env = null, string? value = null)
    {
        if (!string.IsNullOrEmpty(env)) _config.PasswordEnv = env;
        if (!string.IsNullOrEmpty(value)) _config.PasswordValue = value;
        return this;
    }

    /// <summary>
    /// Sets both user and password from environment variables.
    /// </summary>
    /// <param name="userEnv">The user environment variable name.</param>
    /// <param name="passwordEnv">The password environment variable name.</param>
    /// <returns>The builder for chaining.</returns>
    public RemoteConnectionBuilder Credentials(string userEnv, string passwordEnv)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(userEnv);
        ArgumentException.ThrowIfNullOrWhiteSpace(passwordEnv);
        _config.UserEnv = userEnv;
        _config.PasswordEnv = passwordEnv;
        return this;
    }
}
