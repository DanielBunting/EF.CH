using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

namespace EF.CH.Tests.Sql;

public class LeakTestDbContext : DbContext
{
    public LeakTestDbContext(DbContextOptions<LeakTestDbContext> options) : base(options)
    {
    }

    public DbSet<LeakTestEntity> LeakTestEntities => Set<LeakTestEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<LeakTestEntity>(entity =>
        {
            entity.ToTable("leak_test_entities");
            entity.HasKey(e => e.Id);
        });
    }
}

public class LeakTestEntity
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int Value { get; set; }
}
