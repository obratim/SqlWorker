using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace SqlWorker
{
    /// <summary>
    /// Settings for bulk copying in PostgreSQL. Used to specify types for some of properties of type being insert
    /// </summary>
    public class PostgreSqlBulkCopySettings : SqlWorker.IBulkCopySettings, IEnumerable<KeyValuePair<string, NpgsqlDbType>>
    {
        private Dictionary<string, NpgsqlDbType> typeMapping = new Dictionary<string, NpgsqlDbType>();

        public void Add(string key, NpgsqlDbType value) => typeMapping.Add(key, value);

        public NpgsqlDbType this[string column] => typeMapping[column];
        public IEnumerator<KeyValuePair<string, NpgsqlDbType>> GetEnumerator() =>
            typeMapping.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public bool TryGetValue(string column, out NpgsqlDbType type) => typeMapping.TryGetValue(column, out type);
    }

    internal static class BulkCopy
    {
        public static NpgsqlDbType GetDbType(this Array array) => array switch
        {
            short [] _ => NpgsqlDbType.Array | NpgsqlDbType.Smallint,
            int [] _ => NpgsqlDbType.Array | NpgsqlDbType.Integer,
            long [] _ => NpgsqlDbType.Array | NpgsqlDbType.Bigint,
            bool [] _ => NpgsqlDbType.Array | NpgsqlDbType.Bit,
            string [] _ => NpgsqlDbType.Array | NpgsqlDbType.Varchar,
            char [] _ => NpgsqlDbType.Array | NpgsqlDbType.Char,
            double [] _ => NpgsqlDbType.Array | NpgsqlDbType.Double,
            decimal [] _ => NpgsqlDbType.Array | NpgsqlDbType.Money,
            float [] _ => NpgsqlDbType.Array | NpgsqlDbType.Double,
            _ => throw new NpgsqlException($"Wrong type of array ({array.GetType().FullName})")
        };
        
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
                        case double x:
                            writer.Write(x);
                            break;
                        case float x:
                            writer.Write(x);
                            break;
                        case decimal x:
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
                        case char x:
                            writer.Write(x);
                            break;
                        case string x:
                            writer.Write(x);
                            break;
                        case Array _:
                            writer.Write(dr[col.Ordinal]);
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
                        case double x:
                            await writer.WriteAsync(x);
                            break;
                        case float x:
                            await writer.WriteAsync(x);
                            break;
                        case decimal x:
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
                        case Array _:
                            await writer.WriteAsync(dr[col.Ordinal]);
                            break;
                        case null:
                            await writer.WriteNullAsync();
                            break;
                    }
                }
            }

            await writer.CompleteAsync();
        }

        private static readonly System.Text.RegularExpressions.Regex TableNameChecker = new System.Text.RegularExpressions.Regex(@"\w[\w\d]+", System.Text.RegularExpressions.RegexOptions.Compiled);
        public static string BulkCopyCommand(this DataColumnCollection cols, string tableName)
        {
            if (!TableNameChecker.IsMatch(tableName))
                throw new ArgumentException("Incorrect table name: " + tableName);
            return $"COPY {tableName} ({string.Join(", ", cols.Cast<DataColumn>().Select(col => col.ColumnName))}) FROM STDIN (FORMAT BINARY)";
        }

        public static string BulkCopyCommand(this IDataReader dr, string tableName) => BulkCopyCommand(dr.GetSchemaTable().Columns, tableName);
    }
}
