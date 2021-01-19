using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;

namespace SqlWorker
{
    public class PostreSqlBulkCopySettings : SqlWorker.IBulkCopySettings
    {}

    static class BulkCopy
    {
        public static void PerformBulkCopy(this IDataReader dr, NpgsqlBinaryImporter writer, DataColumnCollection columns = null)
        {
            columns ??= dr.GetSchemaTable().Columns;
            
            while (dr.Read())
            {
                writer.StartRow();

                foreach (DataColumn col in columns)
                {
                    switch (dr[col.Ordinal])
                    {
                        case bool x:
                            writer.Write(x);
                            break;
                        case byte x:
                            writer.Write(x);
                            break;
                        case sbyte x:
                            writer.Write(x);
                            break;
                        case short x:
                            writer.Write(x);
                            break;
                        case ushort x:
                            writer.Write(x);
                            break;
                        case int x:
                            writer.Write(x);
                            break;
                        case uint x:
                            writer.Write(x);
                            break;
                        case long x:
                            writer.Write(x);
                            break;
                        case ulong x:
                            writer.Write(x);
                            break;
                        case Guid x:
                            writer.Write(x);
                            break;
                        case DateTime x:
                            writer.Write(x);
                            break;
                        case TimeSpan x:
                            writer.Write(x);
                            break;
                        case string x:
                            writer.Write(x);
                            break;
                        case null:
                            writer.WriteNull();
                            break;
                    }
                }
            }

            writer.Complete();
        }

        public static async Task PerformBulkCopyAsync(this DbDataReader dr, NpgsqlBinaryImporter writer, DataColumnCollection columns = null)
        {
            columns ??= dr.GetSchemaTable().Columns;
            
            while (await dr.ReadAsync())
            {
                await writer.StartRowAsync();

                foreach (DataColumn col in columns)
                {
                    switch (dr[col.Ordinal])
                    {
                        case bool x:
                            await writer.WriteAsync(x);
                            break;
                        case byte x:
                            await writer.WriteAsync(x);
                            break;
                        case sbyte x:
                            await writer.WriteAsync(x);
                            break;
                        case short x:
                            await writer.WriteAsync(x);
                            break;
                        case ushort x:
                            await writer.WriteAsync(x);
                            break;
                        case int x:
                            await writer.WriteAsync(x);
                            break;
                        case uint x:
                            await writer.WriteAsync(x);
                            break;
                        case long x:
                            await writer.WriteAsync(x);
                            break;
                        case ulong x:
                            await writer.WriteAsync(x);
                            break;
                        case Guid x:
                            await writer.WriteAsync(x);
                            break;
                        case DateTime x:
                            await writer.WriteAsync(x);
                            break;
                        case TimeSpan x:
                            await writer.WriteAsync(x);
                            break;
                        case string x:
                            await writer.WriteAsync(x);
                            break;
                        case null:
                            writer.WriteNull();
                            break;
                    }
                }
            }

            await writer.CompleteAsync();
        }

        public static string BulkCopyCommand(this DataColumnCollection cols) => $"COPY data ({string.Join(", ", cols.Cast<DataColumn>().Select(col => col.ColumnName))}) FROM STDIN (FORMAT BINARY)";

        public static string BulkCopyCommand(this IDataReader dr) => BulkCopyCommand(dr.GetSchemaTable().Columns);
    }
}
