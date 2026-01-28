using System.Data.Common;
using EF.CH.Configuration;

namespace EF.CH.Infrastructure;

/// <summary>
/// Interface for managing a pool of ClickHouse connections with health checking.
/// </summary>
public interface IClickHouseConnectionPool : IAsyncDisposable
{
    /// <summary>
    /// Gets a connection based on the configured read strategy.
    /// </summary>
    /// <param name="strategy">The read strategy to use for endpoint selection.</param>
    /// <returns>A database connection to a healthy endpoint.</returns>
    DbConnection GetConnection(ReadStrategy strategy);

    /// <summary>
    /// Performs health checks on all endpoints.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task HealthCheckAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds an endpoint to the pool.
    /// </summary>
    /// <param name="endpoint">The endpoint in host:port format.</param>
    void AddEndpoint(string endpoint);

    /// <summary>
    /// Gets the count of healthy endpoints.
    /// </summary>
    int HealthyEndpointCount { get; }
}
