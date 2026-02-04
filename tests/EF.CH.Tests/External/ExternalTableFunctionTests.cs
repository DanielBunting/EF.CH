using EF.CH.Extensions;
using EF.CH.External;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace EF.CH.Tests.External;

/// <summary>
/// Tests for external table function configuration (S3, URL, Remote, File, Cluster).
/// These tests verify annotation storage and SQL generation without requiring actual external services.
/// </summary>
public class ExternalTableFunctionTests
{
    #region S3 Entity Tests

    [Fact]
    public void ExternalS3Entity_StoresAnnotationsCorrectly()
    {
        var modelBuilder = new ModelBuilder();

        modelBuilder.ExternalS3Entity<S3LogEntry>(ext => ext
            .FromPath("s3://bucket/logs/*.parquet")
            .WithFormat("Parquet")
            .Connection(c => c
                .AccessKey(env: "AWS_ACCESS_KEY")
                .SecretKey(env: "AWS_SECRET_KEY"))
            .WithCompression("gzip"));

        var model = modelBuilder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(S3LogEntry))!;

        Assert.True((bool)entityType.FindAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction)!.Value!);
        Assert.Equal("s3", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalProvider)!.Value);
        Assert.Equal("s3://bucket/logs/*.parquet", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalS3Path)!.Value);
        Assert.Equal("Parquet", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalS3Format)!.Value);
        Assert.Equal("AWS_ACCESS_KEY", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalS3AccessKeyEnv)!.Value);
        Assert.Equal("AWS_SECRET_KEY", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalS3SecretKeyEnv)!.Value);
        Assert.Equal("gzip", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalS3Compression)!.Value);
    }

    [Fact]
    public void ExternalS3Entity_ResolvesTableFunction()
    {
        var modelBuilder = new ModelBuilder();

        modelBuilder.ExternalS3Entity<S3LogEntry>(ext => ext
            .FromPath("s3://bucket/data.csv")
            .WithFormat("CSV"));

        var model = modelBuilder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(S3LogEntry))!;

        var resolver = new ExternalConfigResolver();
        var functionCall = resolver.ResolveS3TableFunction(entityType);

        Assert.Contains("s3(", functionCall);
        Assert.Contains("s3://bucket/data.csv", functionCall);
        Assert.Contains("CSV", functionCall);
    }

    [Fact]
    public void ExternalS3Entity_WithCredentials_ResolvesFromEnvironment()
    {
        var modelBuilder = new ModelBuilder();

        modelBuilder.ExternalS3Entity<S3LogEntry>(ext => ext
            .FromPath("s3://bucket/data.parquet")
            .WithFormat("Parquet")
            .Connection(c => c
                .AccessKey(value: "test-key")
                .SecretKey(value: "test-secret")));

        var model = modelBuilder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(S3LogEntry))!;

        var resolver = new ExternalConfigResolver();
        var functionCall = resolver.ResolveS3TableFunction(entityType);

        Assert.Contains("test-key", functionCall);
        Assert.Contains("test-secret", functionCall);
    }

    #endregion

    #region URL Entity Tests

    [Fact]
    public void ExternalUrlEntity_StoresAnnotationsCorrectly()
    {
        var modelBuilder = new ModelBuilder();

        modelBuilder.ExternalUrlEntity<UrlData>(ext => ext
            .FromUrl("https://api.example.com/data.json")
            .WithFormat("JSONEachRow")
            .WithHeader("Authorization", "Bearer token"));

        var model = modelBuilder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(UrlData))!;

        Assert.True((bool)entityType.FindAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction)!.Value!);
        Assert.Equal("url", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalProvider)!.Value);
        Assert.Equal("https://api.example.com/data.json", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalUrl)!.Value);
        Assert.Equal("JSONEachRow", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalUrlFormat)!.Value);
    }

    [Fact]
    public void ExternalUrlEntity_ResolvesTableFunction()
    {
        var modelBuilder = new ModelBuilder();

        modelBuilder.ExternalUrlEntity<UrlData>(ext => ext
            .FromUrl("https://example.com/data.csv")
            .WithFormat("CSVWithNames"));

        var model = modelBuilder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(UrlData))!;

        var resolver = new ExternalConfigResolver();
        var functionCall = resolver.ResolveUrlTableFunction(entityType);

        Assert.Contains("url(", functionCall);
        Assert.Contains("https://example.com/data.csv", functionCall);
        Assert.Contains("CSVWithNames", functionCall);
    }

    #endregion

    #region Remote Entity Tests

    [Fact]
    public void ExternalRemoteEntity_StoresAnnotationsCorrectly()
    {
        var modelBuilder = new ModelBuilder();

        modelBuilder.ExternalRemoteEntity<RemoteData>(ext => ext
            .FromAddresses("remote-host:9000")
            .FromTable("analytics", "events")
            .Connection(c => c
                .User(value: "default")
                .Password(value: "")));

        var model = modelBuilder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(RemoteData))!;

        Assert.True((bool)entityType.FindAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction)!.Value!);
        Assert.Equal("remote", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalProvider)!.Value);
        Assert.Equal("remote-host:9000", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalRemoteAddresses)!.Value);
        Assert.Equal("analytics", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalRemoteDatabase)!.Value);
        Assert.Equal("events", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalRemoteTable)!.Value);
    }

    [Fact]
    public void ExternalRemoteEntity_ResolvesTableFunction()
    {
        var modelBuilder = new ModelBuilder();

        modelBuilder.ExternalRemoteEntity<RemoteData>(ext => ext
            .FromAddresses("host1:9000,host2:9000")
            .FromTable("db", "table")
            .Connection(c => c
                .User(value: "user")
                .Password(value: "pass")));

        var model = modelBuilder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(RemoteData))!;

        var resolver = new ExternalConfigResolver();
        var functionCall = resolver.ResolveRemoteTableFunction(entityType);

        Assert.Contains("remote(", functionCall);
        Assert.Contains("host1:9000,host2:9000", functionCall);
        Assert.Contains("db", functionCall);
        Assert.Contains("table", functionCall);
        Assert.Contains("user", functionCall);
    }

    #endregion

    #region File Entity Tests

    [Fact]
    public void ExternalFileEntity_StoresAnnotationsCorrectly()
    {
        var modelBuilder = new ModelBuilder();

        modelBuilder.ExternalFileEntity<FileData>(ext => ext
            .FromPath("/data/*.csv")
            .WithFormat("CSVWithNames")
            .WithCompression("gzip"));

        var model = modelBuilder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(FileData))!;

        Assert.True((bool)entityType.FindAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction)!.Value!);
        Assert.Equal("file", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalProvider)!.Value);
        Assert.Equal("/data/*.csv", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalFilePath)!.Value);
        Assert.Equal("CSVWithNames", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalFileFormat)!.Value);
        Assert.Equal("gzip", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalFileCompression)!.Value);
    }

    [Fact]
    public void ExternalFileEntity_ResolvesTableFunction()
    {
        var modelBuilder = new ModelBuilder();

        modelBuilder.ExternalFileEntity<FileData>(ext => ext
            .FromPath("/var/data/file.parquet")
            .WithFormat("Parquet"));

        var model = modelBuilder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(FileData))!;

        var resolver = new ExternalConfigResolver();
        var functionCall = resolver.ResolveFileTableFunction(entityType);

        Assert.Contains("file(", functionCall);
        Assert.Contains("/var/data/file.parquet", functionCall);
        Assert.Contains("Parquet", functionCall);
    }

    #endregion

    #region Cluster Entity Tests

    [Fact]
    public void ExternalClusterEntity_StoresAnnotationsCorrectly()
    {
        var modelBuilder = new ModelBuilder();

        modelBuilder.ExternalClusterEntity<ClusterData>(ext => ext
            .FromCluster("my_cluster")
            .FromTable("default", "events")
            .WithShardingKey("UserId"));

        var model = modelBuilder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ClusterData))!;

        Assert.True((bool)entityType.FindAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction)!.Value!);
        Assert.Equal("cluster", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalProvider)!.Value);
        Assert.Equal("my_cluster", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalClusterName)!.Value);
        Assert.Equal("default", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalClusterDatabase)!.Value);
        Assert.Equal("events", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalClusterTable)!.Value);
        Assert.Equal("UserId", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalClusterShardingKey)!.Value);
    }

    [Fact]
    public void ExternalClusterEntity_WithCurrentDatabase_ResolvesCorrectly()
    {
        var modelBuilder = new ModelBuilder();

        modelBuilder.ExternalClusterEntity<ClusterData>(ext => ext
            .FromCluster("cluster_name")
            .FromCurrentDatabase("local_table"));

        var model = modelBuilder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ClusterData))!;

        var resolver = new ExternalConfigResolver();
        var functionCall = resolver.ResolveClusterTableFunction(entityType);

        Assert.Contains("cluster(", functionCall);
        Assert.Contains("cluster_name", functionCall);
        Assert.Contains("currentDatabase()", functionCall);
        Assert.Contains("local_table", functionCall);
    }

    [Fact]
    public void ExternalClusterEntity_ResolvesTableFunction()
    {
        var modelBuilder = new ModelBuilder();

        modelBuilder.ExternalClusterEntity<ClusterData>(ext => ext
            .FromCluster("prod_cluster")
            .FromTable("analytics", "metrics"));

        var model = modelBuilder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ClusterData))!;

        var resolver = new ExternalConfigResolver();
        var functionCall = resolver.ResolveClusterTableFunction(entityType);

        Assert.Contains("cluster(", functionCall);
        Assert.Contains("prod_cluster", functionCall);
        Assert.Contains("analytics", functionCall);
        Assert.Contains("metrics", functionCall);
    }

    #endregion
}

#region Test Entities

public class S3LogEntry
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string Message { get; set; } = string.Empty;
}

public class UrlData
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public double Value { get; set; }
}

public class RemoteData
{
    public long Id { get; set; }
    public string EventType { get; set; } = string.Empty;
    public DateTime EventTime { get; set; }
}

public class FileData
{
    public int Row { get; set; }
    public string Column1 { get; set; } = string.Empty;
    public string Column2 { get; set; } = string.Empty;
}

public class ClusterData
{
    public Guid UserId { get; set; }
    public string Metric { get; set; } = string.Empty;
    public double Value { get; set; }
}

#endregion
