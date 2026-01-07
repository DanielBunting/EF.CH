using EF.CH.Extensions;
using EF.CH.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.Tests.Sql;

/// <summary>
/// Tests for the ClickHouseUnsupportedOperationException class and identity validation.
/// </summary>
public class UnsupportedOperationTests
{
    #region Exception Property Tests

    [Fact]
    public void UpdateException_HasCorrectProperties()
    {
        var ex = ClickHouseUnsupportedOperationException.Update("TestTable");

        Assert.Equal(ClickHouseUnsupportedOperationException.OperationCategory.Update, ex.Category);
        Assert.Equal("TestTable", ex.TableName);
        Assert.NotNull(ex.Workaround);
        Assert.Contains("UPDATE", ex.Message);
        Assert.Contains("ReplacingMergeTree", ex.Message);
    }

    [Fact]
    public void TransactionException_HasCorrectProperties()
    {
        var ex = ClickHouseUnsupportedOperationException.Transaction();

        Assert.Equal(ClickHouseUnsupportedOperationException.OperationCategory.Transaction, ex.Category);
        Assert.Null(ex.TableName);
        Assert.NotNull(ex.Workaround);
        Assert.Contains("transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AddForeignKeyException_HasCorrectProperties()
    {
        var ex = ClickHouseUnsupportedOperationException.AddForeignKey("FK_Test", "TestTable");

        Assert.Equal(ClickHouseUnsupportedOperationException.OperationCategory.ForeignKey, ex.Category);
        Assert.Equal("TestTable", ex.TableName);
        Assert.Equal("FK_Test", ex.ObjectName);
        Assert.NotNull(ex.Workaround);
        Assert.Contains("foreign key", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AddPrimaryKeyException_HasCorrectProperties()
    {
        var ex = ClickHouseUnsupportedOperationException.AddPrimaryKey("TestTable");

        Assert.Equal(ClickHouseUnsupportedOperationException.OperationCategory.PrimaryKey, ex.Category);
        Assert.Equal("TestTable", ex.TableName);
        Assert.Contains("primary key", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ORDER BY", ex.Message);
    }

    [Fact]
    public void RenameColumnException_HasCorrectProperties()
    {
        var ex = ClickHouseUnsupportedOperationException.RenameColumn("TestTable", "OldColumn");

        Assert.Equal(ClickHouseUnsupportedOperationException.OperationCategory.ColumnRename, ex.Category);
        Assert.Equal("TestTable", ex.TableName);
        Assert.Equal("OldColumn", ex.ColumnName);
        Assert.NotNull(ex.Workaround);
        Assert.Contains("renaming", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void UniqueIndexException_HasCorrectProperties()
    {
        var ex = ClickHouseUnsupportedOperationException.UniqueIndex("IX_Test", "TestTable");

        Assert.Equal(ClickHouseUnsupportedOperationException.OperationCategory.UniqueConstraint, ex.Category);
        Assert.Equal("TestTable", ex.TableName);
        Assert.Equal("IX_Test", ex.ObjectName);
        Assert.Contains("unique", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void IdentityException_HasCorrectProperties()
    {
        var ex = ClickHouseUnsupportedOperationException.Identity("TestTable", "Id");

        Assert.Equal(ClickHouseUnsupportedOperationException.OperationCategory.Identity, ex.Category);
        Assert.Equal("TestTable", ex.TableName);
        Assert.Equal("Id", ex.ColumnName);
        Assert.NotNull(ex.Workaround);
        Assert.Contains("auto-increment", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Guid", ex.Workaround);
    }

    [Fact]
    public void Exception_IsNotSupportedException()
    {
        var ex = ClickHouseUnsupportedOperationException.Update("TestTable");

        Assert.IsAssignableFrom<NotSupportedException>(ex);
    }

    #endregion

    #region Model Validator Identity Tests

    [Fact]
    public void Identity_IntKey_ThrowsOnModelValidation()
    {
        var ex = Assert.Throws<ClickHouseUnsupportedOperationException>(() =>
        {
            using var context = new IntIdentityContext();
            _ = context.Model;
        });

        Assert.Equal(ClickHouseUnsupportedOperationException.OperationCategory.Identity, ex.Category);
        Assert.Contains("auto-increment", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Identity_LongKey_ThrowsOnModelValidation()
    {
        var ex = Assert.Throws<ClickHouseUnsupportedOperationException>(() =>
        {
            using var context = new LongIdentityContext();
            _ = context.Model;
        });

        Assert.Equal(ClickHouseUnsupportedOperationException.OperationCategory.Identity, ex.Category);
    }

    [Fact]
    public void Identity_GuidKey_DoesNotThrow()
    {
        using var context = new GuidKeyContext();
        // This should not throw
        var model = context.Model;
        Assert.NotNull(model);
    }

    [Fact]
    public void Identity_IntKeyWithDefaultValue_DoesNotThrow()
    {
        using var context = new IntWithDefaultContext();
        // This should not throw - has default value
        var model = context.Model;
        Assert.NotNull(model);
    }

    [Fact]
    public void Identity_IntKeyWithValueGeneratedNever_DoesNotThrow()
    {
        using var context = new IntValueGeneratedNeverContext();
        // This should not throw - explicitly disabled
        var model = context.Model;
        Assert.NotNull(model);
    }

    #endregion

    #region Test Contexts

    private class IntIdentityContext : DbContext
    {
        public DbSet<IntEntity> Entities => Set<IntEntity>();

        protected override void OnConfiguring(DbContextOptionsBuilder options)
        {
            options.UseClickHouse("Host=localhost;Database=test");
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<IntEntity>(e =>
            {
                e.UseMergeTree(x => x.Id);
                // Explicitly request value generation (simulating what SQL Server convention does)
                e.Property(x => x.Id).ValueGeneratedOnAdd();
            });
        }
    }

    private class IntEntity
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    private class LongIdentityContext : DbContext
    {
        public DbSet<LongEntity> Entities => Set<LongEntity>();

        protected override void OnConfiguring(DbContextOptionsBuilder options)
        {
            options.UseClickHouse("Host=localhost;Database=test");
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<LongEntity>(e =>
            {
                e.UseMergeTree(x => x.Id);
                // Explicitly request value generation (simulating what SQL Server convention does)
                e.Property(x => x.Id).ValueGeneratedOnAdd();
            });
        }
    }

    private class LongEntity
    {
        public long Id { get; set; }
        public string? Name { get; set; }
    }

    private class GuidKeyContext : DbContext
    {
        public DbSet<GuidEntity> Entities => Set<GuidEntity>();

        protected override void OnConfiguring(DbContextOptionsBuilder options)
        {
            options.UseClickHouse("Host=localhost;Database=test");
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<GuidEntity>()
                .UseMergeTree(e => e.Id);
        }
    }

    private class GuidEntity
    {
        public Guid Id { get; set; }
        public string? Name { get; set; }
    }

    private class IntWithDefaultContext : DbContext
    {
        public DbSet<IntWithDefaultEntity> Entities => Set<IntWithDefaultEntity>();

        protected override void OnConfiguring(DbContextOptionsBuilder options)
        {
            options.UseClickHouse("Host=localhost;Database=test");
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<IntWithDefaultEntity>(e =>
            {
                e.UseMergeTree(x => x.Id);
                e.Property(x => x.Id).HasDefaultValueSql("rand()");
            });
        }
    }

    private class IntWithDefaultEntity
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    private class IntValueGeneratedNeverContext : DbContext
    {
        public DbSet<IntNeverGeneratedEntity> Entities => Set<IntNeverGeneratedEntity>();

        protected override void OnConfiguring(DbContextOptionsBuilder options)
        {
            options.UseClickHouse("Host=localhost;Database=test");
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<IntNeverGeneratedEntity>(e =>
            {
                e.UseMergeTree(x => x.Id);
                e.Property(x => x.Id).ValueGeneratedNever();
            });
        }
    }

    private class IntNeverGeneratedEntity
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    #endregion
}
