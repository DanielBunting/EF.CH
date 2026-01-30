using System.Data;
using System.Data.Common;
using ClickHouse.Driver.ADO;
using EF.CH.Configuration;

namespace EF.CH.Infrastructure;

/// <summary>
/// Endpoint type for routing decisions.
/// </summary>
public enum EndpointType
{
    /// <summary>
    /// Use the write endpoint.
    /// </summary>
    Write,

    /// <summary>
    /// Use a read endpoint.
    /// </summary>
    Read
}

/// <summary>
/// A connection wrapper that routes commands to read or write endpoints.
/// </summary>
/// <remarks>
/// <para>
/// This connection implementation supports read/write splitting for multi-datacenter
/// ClickHouse deployments. Write operations (INSERT, ALTER, etc.) are routed to the
/// write endpoint while read operations (SELECT) are routed to read endpoints.
/// </para>
/// <para>
/// Note: This is a simple wrapper around DbConnection. For full EF Core integration,
/// you can use this with the <see cref="ClickHouseCommandInterceptor"/> to automatically
/// route commands based on their type.
/// </para>
/// </remarks>
public class ClickHouseRoutingConnection : DbConnection
{
    private readonly ConnectionConfig _config;
    private readonly ClickHouseConnectionPool _writePool;
    private readonly ClickHouseConnectionPool _readPool;
    private DbConnection? _currentConnection;
    private EndpointType _activeEndpoint = EndpointType.Write;

    /// <summary>
    /// Creates a new routing connection with the specified configuration.
    /// </summary>
    /// <param name="config">The connection configuration with read/write endpoints.</param>
    public ClickHouseRoutingConnection(ConnectionConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));

        // Initialize write pool with single endpoint
        _writePool = new ClickHouseConnectionPool(config);
        if (!string.IsNullOrEmpty(config.WriteEndpoint))
        {
            _writePool.AddEndpoint(config.WriteEndpoint);
        }

        // Initialize read pool with multiple endpoints
        _readPool = new ClickHouseConnectionPool(config);
        foreach (var endpoint in config.ReadEndpoints)
        {
            _readPool.AddEndpoint(endpoint);
        }
    }

    /// <summary>
    /// Gets or sets the active endpoint type for the next command.
    /// This is typically set by the command interceptor.
    /// </summary>
    public EndpointType ActiveEndpoint
    {
        get => _activeEndpoint;
        set
        {
            if (_activeEndpoint != value)
            {
                _activeEndpoint = value;
                // Close current connection so next command gets a new one
                _currentConnection?.Dispose();
                _currentConnection = null;
            }
        }
    }

    /// <inheritdoc />
    public override string ConnectionString
    {
        get => GetConnectionString();
        set => throw new NotSupportedException("Connection string is determined by routing configuration.");
    }

    /// <inheritdoc />
    public override string Database => _config.Database;

    /// <inheritdoc />
    public override string DataSource => _activeEndpoint == EndpointType.Write
        ? _config.WriteEndpoint
        : _config.ReadEndpoints.FirstOrDefault() ?? _config.WriteEndpoint;

    /// <inheritdoc />
    public override string ServerVersion => "ClickHouse";

    /// <inheritdoc />
    public override ConnectionState State => _currentConnection?.State ?? ConnectionState.Closed;

    /// <inheritdoc />
    public override void ChangeDatabase(string databaseName)
    {
        throw new NotSupportedException("ClickHouse doesn't support changing database on an open connection.");
    }

    /// <inheritdoc />
    public override void Close()
    {
        _currentConnection?.Close();
    }

    /// <inheritdoc />
    public override void Open()
    {
        EnsureConnection();
        _currentConnection?.Open();
    }

    /// <inheritdoc />
    public override async Task OpenAsync(CancellationToken cancellationToken)
    {
        EnsureConnection();
        if (_currentConnection != null)
        {
            await _currentConnection.OpenAsync(cancellationToken);
        }
    }

    /// <inheritdoc />
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        throw new NotSupportedException("ClickHouse doesn't support transactions.");
    }

    /// <inheritdoc />
    protected override DbCommand CreateDbCommand()
    {
        EnsureConnection();
        return _currentConnection!.CreateCommand();
    }

    private void EnsureConnection()
    {
        if (_currentConnection == null || _currentConnection.State == ConnectionState.Closed)
        {
            _currentConnection?.Dispose();
            _currentConnection = _activeEndpoint switch
            {
                EndpointType.Write => _writePool.GetConnection(ReadStrategy.PreferFirst),
                EndpointType.Read => _readPool.GetConnection(_config.ReadStrategy),
                _ => _writePool.GetConnection(ReadStrategy.PreferFirst)
            };
        }
    }

    private string GetConnectionString()
    {
        var endpoint = _activeEndpoint == EndpointType.Write
            ? _config.WriteEndpoint
            : _config.ReadEndpoints.FirstOrDefault() ?? _config.WriteEndpoint;

        if (string.IsNullOrEmpty(endpoint))
        {
            return $"Database={_config.Database}";
        }

        var parts = endpoint.Split(':');
        var host = parts[0];
        var port = parts.Length > 1 ? parts[1] : "8123";

        return $"Host={host};Port={port};Database={_config.Database}";
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _currentConnection?.Dispose();
        }
        base.Dispose(disposing);
    }
}
