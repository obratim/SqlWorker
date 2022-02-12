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
        private Dictionary<string, NpgsqlDbType> _typeMapping = new Dictionary<string, NpgsqlDbType>();

        public void Add(string key, NpgsqlDbType value) => _typeMapping.Add(key, value);

        public NpgsqlDbType this[string column] => _typeMapping[column];
        public IEnumerator<KeyValuePair<string, NpgsqlDbType>> GetEnumerator() =>
            _typeMapping.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public bool TryGetValue(string column, out NpgsqlDbType type) => _typeMapping.TryGetValue(column, out type);
    }

    internal static class BulkCopy
    {
        public static void PerformBulkCopy(
            this IDataReader dr,
            NpgsqlBinaryImporter writer, 
            DataColumnCollection columns = null, 
            PostgreSqlBulkCopySettings settings = null)
        {
            settings ??= new PostgreSqlBulkCopySettings();
            columns ??= dr.GetSchemaTable().Columns;
            
            while (dr.Read())
            {
                writer.StartRow();

                foreach (DataColumn col in columns)
                {                
                    void Write<T>(T value)
                    {
                        if (settings.TryGetValue(col.Caption, out var type))
                        {
                            writer.Write(value, type);
                            return;
                        }
                        
                        writer.Write(value);
                    }
                    
                    switch (dr[col.Ordinal])
                    {
                        case bool x:
                            Write(x);
                            break;
                        case byte x:
                            Write(x);
                            break;
                        case sbyte x:
                            Write(x);
                            break;
                        case short x:
                            Write(x);
                            break;
                        case ushort x:
                            Write(x);
                            break;
                        case int x:
                            Write(x);
                            break;
                        case uint x:
                            Write(x);
                            break;
                        case long x:
                            Write(x);
                            break;
                        case ulong x:
                            Write(x);
                            break;
                        case double x:
                            Write(x);
                            break;
                        case float x:
                            Write(x);
                            break;
                        case decimal x:
                            Write(x);
                            break;
                        case Guid x:
                            Write(x);
                            break;
                        case DateTime x:
                            Write(x);
                            break;
                        case TimeSpan x:
                            Write(x);
                            break;
                        case char x:
                            Write(x);
                            break;
                        case string x:
                            Write(x);
                            break;
                        case Array _:
                            Write(dr[col.Ordinal]);
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
