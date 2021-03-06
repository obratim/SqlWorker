using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Linq.Expressions;
using Npgsql;

namespace SqlWorker
{
    static class BulcCopyGeneric<T>
    {
        private static Action<NpgsqlBinaryImporter, T> PerformBulkCopyDataRow;
        private static readonly List<string> Columns;

        static BulcCopyGeneric()
        {
            var copyParameterWriter = Expression.Parameter(typeof(NpgsqlBinaryImporter), "writer");
            var copyParameterData = Expression.Parameter(typeof(T), "data");

            var properties = TypeDescriptor.GetProperties(typeof(T));
            Columns = new List<string>(properties.Count);

            var writeSteps = new List<Expression>();
            var mapperType = typeof(NpgsqlParameter).Assembly.GetType("Npgsql.TypeMapping.GlobalTypeMapper");
            var mapper = mapperType.GetProperty("Instance", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public).GetValue(null);
            var mappings = mapperType.GetProperty("Mappings", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance).GetValue(mapper) as Dictionary<string, Npgsql.TypeMapping.NpgsqlTypeMapping>;

            foreach (PropertyDescriptor property in properties)
            {
                var propertyAccess = Expression.Property(copyParameterData, property.Name);
                switch (property.PropertyType)
                {
                    case {} when mappings.Any(m => m.Value.ClrTypes.Contains(property.PropertyType)):
                    {
                        writeSteps.Add(Expression.Call(copyParameterWriter, nameof(NpgsqlBinaryImporter.Write), new [] {property.PropertyType}, propertyAccess));
                        Columns.Add(property.Name);
                        break;
                    }
                    case {} when property.PropertyType.IsGenericType && property.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>) && mappings.Any(m => m.Value.ClrTypes.Contains(Nullable.GetUnderlyingType(property.PropertyType))):
                    {
                        var valueAccess = Expression.Property(propertyAccess, nameof(Nullable<int>.Value));
                        writeSteps.Add(
                            Expression.Condition(
                                Expression.Equal(propertyAccess, Expression.Default(property.PropertyType)),
                                Expression.Call(copyParameterWriter, typeof(NpgsqlBinaryImporter).GetMethod(nameof(NpgsqlBinaryImporter.WriteNull))),
                                Expression.Call(copyParameterWriter, nameof(NpgsqlBinaryImporter.Write), new [] {Nullable.GetUnderlyingType(property.PropertyType)}, valueAccess)));
                        Columns.Add(property.Name);
                        break;
                    }
                    case {} when property.PropertyType.IsEnum:
                    {
                        var underlyingType = Enum.GetUnderlyingType(property.PropertyType);
                        var valueAccess = Expression.Convert(propertyAccess, underlyingType);
                        writeSteps.Add(Expression.Call(copyParameterWriter, nameof(NpgsqlBinaryImporter.Write), new [] {underlyingType}, valueAccess));
                        Columns.Add(property.Name);
                        break;
                    }
                    case {} when property.PropertyType.IsGenericType && property.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>) && Nullable.GetUnderlyingType(property.PropertyType).IsEnum:
                    {
                        var underlyingType = Enum.GetUnderlyingType(Nullable.GetUnderlyingType(property.PropertyType));
                        var valueAccess = Expression.Convert(Expression.Property(propertyAccess, nameof(Nullable<int>.Value)), underlyingType);
                        writeSteps.Add(
                            Expression.Condition(
                                Expression.Equal(propertyAccess, Expression.Default(property.PropertyType)),
                                Expression.Call(copyParameterWriter, typeof(NpgsqlBinaryImporter).GetMethod(nameof(NpgsqlBinaryImporter.WriteNull))),
                                Expression.Call(copyParameterWriter, nameof(NpgsqlBinaryImporter.Write), new [] {underlyingType}, valueAccess)));
                        Columns.Add(property.Name);
                        break;
                    }
                }
            }

            var body = Expression.Block(writeSteps);

            var lambda = Expression.Lambda<Action<NpgsqlBinaryImporter, T>>(body, copyParameterWriter, copyParameterData);

            PerformBulkCopyDataRow = lambda.Compile();
        }

        public static void BulkCopy(IEnumerable<T> source, NpgsqlBinaryImporter writer)
        {
            foreach (var row in source)
            {
                writer.StartRow();
                PerformBulkCopyDataRow(writer, row);
            }

            writer.Complete();
        }


        private static readonly System.Text.RegularExpressions.Regex TableNameChecker = new System.Text.RegularExpressions.Regex(@"\w[\w\d]+", System.Text.RegularExpressions.RegexOptions.Compiled);
        public static string BulkCopyCommand(string tableName)
        {
            if (!TableNameChecker.IsMatch(tableName))
                throw new ArgumentException("Incorrect table name: " + tableName);
            return $"COPY {tableName} ({string.Join(", ", Columns)}) FROM STDIN (FORMAT BINARY)";
        }

    }
}
