using System.Collections.Concurrent;
using System.Data.Common;
using ClickHouse.Driver.ADO;
using EF.CH.Configuration;

namespace EF.CH.Infrastructure;

/// <summary>
/// Manages a pool of ClickHouse connections with health checking and failover.
/// </summary>
public class ClickHouseConnectionPool : IClickHouseConnectionPool
{
    private readonly ConcurrentDictionary<string, EndpointState> _endpoints = new();
    private readonly ConnectionConfig _config;
    private int _roundRobinIndex;
    private readonly object _roundRobinLock = new();

    /// <summary>
    /// Creates a new connection pool with the specified configuration.
    /// </summary>
    /// <param name="config">The connection configuration.</param>
    public ClickHouseConnectionPool(ConnectionConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
    }

    /// <inheritdoc />
    public int HealthyEndpointCount => _endpoints.Values.Count(e => e.IsHealthy);

    /// <inheritdoc />
    public void AddEndpoint(string endpoint)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);
        _endpoints.TryAdd(endpoint, new EndpointState(endpoint, _config));
    }

    /// <inheritdoc />
    public DbConnection GetConnection(ReadStrategy strategy)
    {
        var healthyEndpoints = _endpoints.Values
            .Where(e => e.IsHealthy)
            .ToList();

        if (healthyEndpoints.Count == 0)
        {
            // Fall back to all endpoints if none are healthy (might recover)
            healthyEndpoints = [.. _endpoints.Values];

            if (healthyEndpoints.Count == 0)
            {
                throw new ClickHouseConnectionException("No read endpoints configured.");
            }
        }

        var endpoint = strategy switch
        {
            ReadStrategy.PreferFirst => healthyEndpoints[0],
            ReadStrategy.RoundRobin => GetRoundRobinEndpoint(healthyEndpoints),
            ReadStrategy.Random => healthyEndpoints[Random.Shared.Next(healthyEndpoints.Count)],
            _ => healthyEndpoints[0]
        };

        return endpoint.CreateConnection();
    }

    private EndpointState GetRoundRobinEndpoint(List<EndpointState> endpoints)
    {
        lock (_roundRobinLock)
        {
            var index = _roundRobinIndex++ % endpoints.Count;
            return endpoints[index];
        }
    }

    /// <inheritdoc />
    public async Task HealthCheckAsync(CancellationToken cancellationToken = default)
    {
        var tasks = _endpoints.Values.Select(async endpoint =>
        {
            try
            {
                using var conn = endpoint.CreateConnection();
                await conn.OpenAsync(cancellationToken);
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT 1";
                await cmd.ExecuteScalarAsync(cancellationToken);
                endpoint.MarkHealthy();
            }
            catch
            {
                endpoint.MarkUnhealthy();
            }
        });

        await Task.WhenAll(tasks);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        // Connection pools typically don't need cleanup, but we implement the interface
        await Task.CompletedTask;
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Tracks the health state of an endpoint.
    /// </summary>
    private sealed class EndpointState
    {
        private readonly string _endpoint;
        private readonly ConnectionConfig _config;
        private volatile bool _isHealthy = true;
        private DateTime _lastHealthCheck = DateTime.UtcNow;
        private int _consecutiveFailures;

        public EndpointState(string endpoint, ConnectionConfig config)
        {
            _endpoint = endpoint;
            _config = config;
        }

        public bool IsHealthy => _isHealthy;

        public void MarkHealthy()
        {
            _isHealthy = true;
            _consecutiveFailures = 0;
            _lastHealthCheck = DateTime.UtcNow;
        }

        public void MarkUnhealthy()
        {
            _consecutiveFailures++;
            if (_consecutiveFailures >= _config.Failover.MaxRetries)
            {
                _isHealthy = false;
            }
            _lastHealthCheck = DateTime.UtcNow;
        }

        public DbConnection CreateConnection()
        {
            // Parse endpoint (host:port)
            var parts = _endpoint.Split(':');
            var host = parts[0];
            var port = parts.Length > 1 ? int.Parse(parts[1]) : 8123;

            var connString = $"Host={host};Port={port};Database={_config.Database}";
            if (!string.IsNullOrEmpty(_config.Username))
            {
                connString += $";Username={_config.Username}";
            }
            if (!string.IsNullOrEmpty(_config.Password))
            {
                connString += $";Password={_config.Password}";
            }

            return new ClickHouseConnection(connString);
        }
    }
}

/// <summary>
/// Exception thrown when no healthy ClickHouse endpoints are available.
/// </summary>
public class ClickHouseConnectionException : Exception
{
    public ClickHouseConnectionException(string message) : base(message) { }
    public ClickHouseConnectionException(string message, Exception innerException) : base(message, innerException) { }
}
