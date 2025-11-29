using System;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace MigrationSample.Migrations
{
    /// <inheritdoc />
    public partial class InitialCreate : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "EventLogs",
                columns: table => new
                {
                    Timestamp = table.Column<DateTime>(type: "DateTime64(3)", nullable: false),
                    EventType = table.Column<string>(type: "String", nullable: false),
                    Message = table.Column<string>(type: "String", nullable: false),
                    UserId = table.Column<string>(type: "String", nullable: true),
                    Metadata = table.Column<string>(type: "String", nullable: true)
                },
                constraints: table =>
                {
                });

            migrationBuilder.CreateTable(
                name: "Orders",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "UUID", nullable: false),
                    ProductId = table.Column<Guid>(type: "UUID", nullable: false),
                    Quantity = table.Column<int>(type: "Int32", nullable: false),
                    OrderDate = table.Column<DateTime>(type: "DateTime64(3)", nullable: false),
                    CustomerName = table.Column<string>(type: "String", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Orders", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Products",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "UUID", nullable: false),
                    Name = table.Column<string>(type: "String", nullable: false),
                    Price = table.Column<decimal>(type: "Decimal(18, 4)", nullable: false),
                    CreatedAt = table.Column<DateTime>(type: "DateTime64(3)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Products", x => x.Id);
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "EventLogs");

            migrationBuilder.DropTable(
                name: "Orders");

            migrationBuilder.DropTable(
                name: "Products");
        }
    }
}
