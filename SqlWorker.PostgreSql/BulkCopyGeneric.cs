using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Linq.Expressions;
using Npgsql;
using NpgsqlTypes;

namespace SqlWorker
{
    internal static class PostgresBulkCopyHelpers
    {
        public static NpgsqlDbType? GetNpgsqlDbType(string name, PostgreSqlBulkCopySettings settings) =>
            settings.TryGetValue(name, out var type)
                ? type
                : (NpgsqlDbType?)null;
    }
    static class BulkCopyGeneric<T>
    {
        private static readonly Action<NpgsqlBinaryImporter, T, PostgreSqlBulkCopySettings> PerformBulkCopyDataRow;
        private static readonly List<string> Columns;

        static BulkCopyGeneric()
        {
            var copyParameterWriter = Expression.Parameter(typeof(NpgsqlBinaryImporter), "writer");
            var copyParameterData = Expression.Parameter(typeof(T), "data");
            var copyParameterSettings = Expression.Parameter(typeof(PostgreSqlBulkCopySettings), "settings");

            var properties = TypeDescriptor.GetProperties(typeof(T));
            Columns = new List<string>(properties.Count);

            var writeSteps = new List<Expression>();
            var variables = new List<ParameterExpression>();
            var mapperType = typeof(NpgsqlParameter).Assembly.GetType("Npgsql.TypeMapping.GlobalTypeMapper");
            var mapper = mapperType
                .GetProperty("Instance", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public)
                !.GetValue(null);
            var mappings = mapperType
                .GetProperty("Mappings", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
                !.GetValue(mapper) as Dictionary<string, Npgsql.TypeMapping.NpgsqlTypeMapping>;

            foreach (PropertyDescriptor property in properties)
            {
                var propertyAccess = Expression.Property(copyParameterData, property.Name);
                        
                var dbTypeDeclaration = Expression.Variable(typeof(NpgsqlDbType?), $"{property.Name}DbType");
                        
                variables.Add(dbTypeDeclaration);
                        
                var dbTypeAssign = Expression.Assign(
                    dbTypeDeclaration,
                    Expression.Call(
                        null,
                        typeof(PostgresBulkCopyHelpers).GetMethod(nameof(PostgresBulkCopyHelpers.GetNpgsqlDbType))!,
                        Expression.Constant(property.Name),
                        copyParameterSettings));
                writeSteps.Add(dbTypeAssign);
                
                var notNullDbType = Expression.Convert(dbTypeDeclaration, typeof(NpgsqlDbType));
                var ifDbTypeExistsInSettingsUseIt =
                    Expression.Condition(
                        Expression.Equal(dbTypeDeclaration, Expression.Constant(null)),
                        Expression.Call(copyParameterWriter, nameof(NpgsqlBinaryImporter.Write), new[] { property.PropertyType }, propertyAccess),
                        Expression.Call(copyParameterWriter, nameof(NpgsqlBinaryImporter.Write), new[] { property.PropertyType }, propertyAccess, notNullDbType));
                
                switch (property.PropertyType)
                {
                    case {} when mappings!.Any(m => m.Value.ClrTypes.Contains(property.PropertyType)):
                    {
                        writeSteps.Add(ifDbTypeExistsInSettingsUseIt);
                        Columns.Add(property.Name);
                        break;
                    }
                    case { IsGenericType: true } when 
                        property.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>) && 
                        mappings.Any(m => m.Value.ClrTypes.Contains(Nullable.GetUnderlyingType(property.PropertyType))):
                    {
                        var valueAccess = Expression.Property(propertyAccess, nameof(Nullable<int>.Value));
                        
                        ifDbTypeExistsInSettingsUseIt =
                            Expression.Condition(
                                Expression.Equal(dbTypeDeclaration, Expression.Constant(null)),
                                Expression.Call(copyParameterWriter, nameof(NpgsqlBinaryImporter.Write), new[] { Nullable.GetUnderlyingType(property.PropertyType) }, valueAccess),
                                Expression.Call(copyParameterWriter, nameof(NpgsqlBinaryImporter.Write), new[] { Nullable.GetUnderlyingType(property.PropertyType) }, valueAccess, notNullDbType));
                        
                        writeSteps.Add(
                            Expression.Condition(
                                Expression.Equal(propertyAccess, Expression.Default(property.PropertyType)),
                                Expression.Call(copyParameterWriter, typeof(NpgsqlBinaryImporter).GetMethod(nameof(NpgsqlBinaryImporter.WriteNull))!),
                                ifDbTypeExistsInSettingsUseIt));
                        Columns.Add(property.Name);
                        break;
                    }
                    case { IsEnum: true }:
                    {
                        var underlyingType = Enum.GetUnderlyingType(property.PropertyType);
                        var valueAccess = Expression.Convert(propertyAccess, underlyingType);
                        
                        ifDbTypeExistsInSettingsUseIt =
                            Expression.Condition(
                                Expression.Equal(dbTypeDeclaration, Expression.Constant(null)),
                                Expression.Call(copyParameterWriter, nameof(NpgsqlBinaryImporter.Write), new[] { underlyingType }, valueAccess),
                                Expression.Call(copyParameterWriter, nameof(NpgsqlBinaryImporter.Write), new[] { underlyingType }, valueAccess, notNullDbType));
                        
                        writeSteps.Add(ifDbTypeExistsInSettingsUseIt);
                        Columns.Add(property.Name);
                        break;
                    }
                    case { IsGenericType: true } when 
                        property.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>) && 
                        Nullable.GetUnderlyingType(property.PropertyType)!.IsEnum:
                    {
                        var underlyingType = Enum.GetUnderlyingType(Nullable.GetUnderlyingType(property.PropertyType)!);
                        var valueAccess = Expression.Convert(Expression.Property(propertyAccess, nameof(Nullable<int>.Value)), underlyingType);
                        
                        ifDbTypeExistsInSettingsUseIt =
                            Expression.Condition(
                                Expression.Equal(dbTypeDeclaration, Expression.Constant(null)),
                                Expression.Call(copyParameterWriter, nameof(NpgsqlBinaryImporter.Write), new[] { underlyingType }, valueAccess),
                                Expression.Call(copyParameterWriter, nameof(NpgsqlBinaryImporter.Write), new[] { underlyingType }, valueAccess, notNullDbType));
                        
                        writeSteps.Add(
                            Expression.Condition(
                                Expression.Equal(propertyAccess, Expression.Default(property.PropertyType)),
                                Expression.Call(copyParameterWriter, typeof(NpgsqlBinaryImporter).GetMethod(nameof(NpgsqlBinaryImporter.WriteNull))!),
                                ifDbTypeExistsInSettingsUseIt));
                        
                        Columns.Add(property.Name);
                        break;
                    }

                    case { IsArray: true }:
                    {
                        writeSteps.Add(ifDbTypeExistsInSettingsUseIt);
                        Columns.Add(property.Name);
                        break;
                    }
                }
            }

            var body = Expression.Block(variables, writeSteps);

            var lambda = Expression.Lambda<Action<NpgsqlBinaryImporter, T, PostgreSqlBulkCopySettings>>(
                body, 
                copyParameterWriter, copyParameterData, copyParameterSettings);

            PerformBulkCopyDataRow = lambda.Compile();

            // throw new Exception(lambda.ToReadableString());
        }

        public static void BulkCopy(IEnumerable<T> source, NpgsqlBinaryImporter writer, PostgreSqlBulkCopySettings settings = null)
        {
            settings ??= new PostgreSqlBulkCopySettings();
            foreach (var row in source)
            {
                writer.StartRow();
                PerformBulkCopyDataRow(writer, row, settings);
            }

            writer.CompleteAsync().GetAwaiter().GetResult();
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
