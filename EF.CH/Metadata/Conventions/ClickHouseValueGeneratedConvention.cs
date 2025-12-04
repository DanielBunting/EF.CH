using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;

namespace EF.CH.Metadata.Conventions;

/// <summary>
/// Convention that removes ValueGeneratedOnAdd for integer types.
/// ClickHouse doesn't support auto-increment/identity columns.
/// </summary>
public class ClickHouseValueGeneratedConvention : IModelFinalizingConvention
{
    public void ProcessModelFinalizing(IConventionModelBuilder modelBuilder, IConventionContext<IConventionModelBuilder> context)
    {
        foreach (var entityType in modelBuilder.Metadata.GetEntityTypes())
        {
            // Skip owned types and views
            if (entityType.IsOwned() || entityType.GetViewName() != null)
                continue;

            foreach (var property in entityType.GetProperties())
            {
                // Only process properties that have ValueGeneratedOnAdd set by convention
                if (!property.ValueGenerated.HasFlag(ValueGenerated.OnAdd))
                    continue;

                // Skip if explicitly set by user (non-convention configuration)
                var valueGeneratedConfig = property.GetValueGeneratedConfigurationSource();
                if (valueGeneratedConfig == ConfigurationSource.Explicit)
                    continue;

                // Skip Guid - EF Core generates these client-side
                var clrType = property.ClrType;
                if (clrType == typeof(Guid) || clrType == typeof(Guid?))
                    continue;

                // Skip if there's a SQL default or value generator factory
                if (!string.IsNullOrEmpty(property.GetDefaultValueSql()) ||
                    property.GetValueGeneratorFactory() != null)
                    continue;

                // For integer types set by convention, remove ValueGeneratedOnAdd
                // since ClickHouse doesn't support identity columns
                if (IsIntegerType(clrType))
                {
                    property.Builder.ValueGenerated(ValueGenerated.Never);
                }
            }
        }
    }

    private static bool IsIntegerType(Type type)
    {
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;
        return underlyingType == typeof(int)
            || underlyingType == typeof(long)
            || underlyingType == typeof(short)
            || underlyingType == typeof(byte)
            || underlyingType == typeof(uint)
            || underlyingType == typeof(ulong)
            || underlyingType == typeof(ushort)
            || underlyingType == typeof(sbyte);
    }
}
