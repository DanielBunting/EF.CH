using System;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace KeylessSample.Migrations
{
    /// <inheritdoc />
    public partial class InitialCreate : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "ApiRequests",
                columns: table => new
                {
                    Timestamp = table.Column<DateTime>(type: "DateTime64(3)", nullable: false),
                    Endpoint = table.Column<string>(type: "String", nullable: false),
                    Method = table.Column<string>(type: "String", nullable: false),
                    StatusCode = table.Column<int>(type: "Int32", nullable: false),
                    ResponseTimeMs = table.Column<int>(type: "Int32", nullable: false),
                    RequestBody = table.Column<string>(type: "String", nullable: true)
                },
                constraints: table =>
                {
                });

            migrationBuilder.CreateTable(
                name: "PageViews",
                columns: table => new
                {
                    Timestamp = table.Column<DateTime>(type: "DateTime64(3)", nullable: false),
                    PageUrl = table.Column<string>(type: "String", nullable: false),
                    UserId = table.Column<string>(type: "String", nullable: true),
                    Referrer = table.Column<string>(type: "String", nullable: true),
                    UserAgent = table.Column<string>(type: "String", nullable: true),
                    DurationMs = table.Column<int>(type: "Int32", nullable: false)
                },
                constraints: table =>
                {
                });

            migrationBuilder.CreateTable(
                name: "Users",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "UUID", nullable: false),
                    Email = table.Column<string>(type: "String", nullable: false),
                    Name = table.Column<string>(type: "String", nullable: false),
                    CreatedAt = table.Column<DateTime>(type: "DateTime64(3)", nullable: false),
                    UpdatedAt = table.Column<DateTime>(type: "DateTime64(3)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Users", x => x.Id);
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "ApiRequests");

            migrationBuilder.DropTable(
                name: "PageViews");

            migrationBuilder.DropTable(
                name: "Users");
        }
    }
}
