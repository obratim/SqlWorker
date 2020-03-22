using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.ComponentModel;
using System.Data.Common;
using System.Reflection;

namespace SqlWorker
{
    /// <summary>
    /// Helpers for batching Enumerable and null workarounds
    /// </summary>
	public static class Helpers
	{
        /// <summary>
        /// Generates DataTable from generic IEnumerable using reflection
        /// </summary>
        /// <typeparam name="T">The generic type of collection</typeparam>
        /// <param name="source">The source collection</param>
        /// <returns>DataTable object with columns based on properties reflected from generic type</returns>
		public static DataTable AsDataTable<T>(this IEnumerable<T> source)
		{
            //PropertyDescriptorCollection properties = TypeDescriptor.GetProperties(typeof(T));
            var properties = GetFlatProperties<T>().ToList();
			var table = new DataTable();
			foreach (var prop in properties)
				table.Columns.Add(prop.Item1, Nullable.GetUnderlyingType(prop.Item2) ?? prop.Item2);
			foreach (T item in source)
			{
				DataRow row = table.NewRow();
			    for (var i = 0; i < properties.Count; ++i)
			    {
			        var prop = properties[i];
			        row[i] = prop.Item3(item) ?? DBNull.Value;
			    }
			    table.Rows.Add(row);
			}
			return table;
		}

        /// <summary>
        /// Generates multiple DataTable objects from generic IEnumerable using reflection
        /// </summary>
        /// <typeparam name="T">The generic type of collection</typeparam>
        /// <param name="source">The source collection</param>
        /// <param name="chunkSize">Maximum count of rows in each returned DataTable</param>
        /// <returns>DataTable objects with columns based on properties reflected from generic type and rows count less or equal then chunkSize</returns>
        public static IEnumerable<DataTable> AsDataTable<T>(this IEnumerable<T> source, int chunkSize)
	    {
	        //var properties = TypeDescriptor.GetProperties(typeof (T));
            var properties = GetFlatProperties<T>().ToList();
	        using (var table = new DataTable())
            {
	            foreach (var property in properties)
                    table.Columns.Add(property.Item1, Nullable.GetUnderlyingType(property.Item2) ?? property.Item2);

                using (var enumerator = source.Batch(chunkSize).GetEnumerator())
                {
                    if (!enumerator.MoveNext()) yield break;

                    // initially fill table
                    foreach (T item in enumerator.Current)
                    {
                        DataRow row = table.NewRow();
                        for (var i = 0; i < properties.Count; ++i)
                        {
                            var prop = properties[i];
                            row[i] = prop.Item3(item) ?? DBNull.Value;
                        }
                        table.Rows.Add(row);
                    }
                    yield return table;

                    // replace values
                    while (enumerator.MoveNext())
                    {
                        int rowNumber = 0;
                        foreach (var item in enumerator.Current)
                        {
                            for (var i = 0; i < properties.Count; ++i)
                            {
                                var prop = properties[i];
                                table.Rows[rowNumber][i] = prop.Item3(item) ?? DBNull.Value;
                            }
                            ++rowNumber;
                        }

                        if (rowNumber < chunkSize)
                            while (table.Rows.Count > rowNumber)
                                table.Rows.RemoveAt(rowNumber);

                        yield return table;
                    }
                }
	        }
	    }

        /// <summary>
        /// Splits the source set to number of batches, each with length less or equals to batchSize
        /// </summary>
        /// <typeparam name="T">Type of sequence</typeparam>
        /// <param name="source">The source sequence</param>
        /// <param name="batchSize">The max size of a result batch</param>
        /// <returns>The set of batches, each with length less or equals to batchSize</returns>
        public static IEnumerable<IEnumerable<T>> Batch<T>(
            this IEnumerable<T> source, int batchSize)
        {
            using (var enumerator = source.GetEnumerator())
                while (enumerator.MoveNext())
                    yield return YieldBatchElements(enumerator, batchSize - 1);
        }

        private static IEnumerable<T> YieldBatchElements<T>(
            IEnumerator<T> source, int batchSize)
        {
            yield return source.Current;
            for (int i = 0; i < batchSize && source.MoveNext(); i++)
                yield return source.Current;
        }


        private static IEnumerable<Tuple<string, Type, Func<T, object>>> GetFlatProperties<T>()
        {
            return GetFlatPropertyInfos<T>(typeof(T), "", x => x, x => { })
                .Select(x => new Tuple<string, Type, Func<T, object>>(
                    x.Key,
                    x.Value.Item1.PropertyType,
                    (T obj) =>
                    {
                        var subObj = x.Value.Item2(obj);
                        if (subObj == null) return null;
                        return x.Value.Item1.GetValue(subObj, null);
                    }));
        }

        /// <summary>
        /// Create flat list of properties from nested object; recursive
        /// </summary>
        /// <remarks>Nested type must have parameterless constructor</remarks>
        /// <param name="type">The type</param>
        /// <param name="prefix">Property name prefix</param>
        /// <param name="getter">Func for getting child value of specified type from root TelemetryData value</param>
        /// <param name="initializer">Action for creating child value of specified type in root TelemetryData value</param>
        /// <param name="getterAttributeName">Func for getting retreive property name from <see cref="TPropertyAttribute"/></param>
        /// <returns>IEnumerable of KeyValuePair where Key - full name of a property and Value - tuple of instruments for working with the property, getting and setting values</returns>
        public static IEnumerable<KeyValuePair<string, Tuple<PropertyInfo, Func<T, object>, Action<T>>>>
            GetFlatPropertyInfos<T>(
                Type type,
                string prefix,
                Func<T, object> getter,
                Action<T> initializer)
        {
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                foreach (var p in GetFlatPropertyInfos(Nullable.GetUnderlyingType(type), prefix, getter, initializer))
                    yield return p;
                yield break;
            }

            foreach (PropertyInfo p in type.GetProperties(BindingFlags.Instance | BindingFlags.Public))
            {
                // var dma = p.GetCustomAttribute<TPropertyAttribute>();
                // if (dma == null) continue;

                var name = /*getterAttributeName(dma) ??*/ p.Name;

                if (p.PropertyType.IsEnum || SupportedBasicTypesDictionary.Contains(p.PropertyType))
                {
                    yield return new KeyValuePair<string, Tuple<PropertyInfo, Func<T, object>, Action<T>>>(
                        prefix + name,
                        new Tuple<PropertyInfo, Func<T, object>, Action<T>>(p, getter, initializer));
                }
                else
                {
                    foreach (var pn in GetFlatPropertyInfos<T>(
                        p.PropertyType,
                        prefix + name + '.',
                        x =>
                        {
                            var subObj = getter(x);
                            return subObj == null ? null : p.GetValue(subObj, null);
                        },
                        x =>
                        {
                            var subVal = getter(x);
                            if (subVal == null)
                            {
                                initializer(x);
                                subVal = getter(x);
                            }

                            p.SetValue(
                                subVal,
                                TypeDescriptor.CreateInstance(
                                    null,
                                    p.PropertyType,
                                    new Type[0],
                                    new object[0]),
                                null);
                        }))
                        yield return pn;
                }
            }
        }

        public static readonly HashSet<Type> SupportedBasicTypesDictionary =
        new HashSet<Type>
        {
            typeof(string),
            typeof(bool),
            typeof(short),
            typeof(int),
            typeof(long),
            typeof(ushort),
            typeof(uint),
            typeof(ulong),
            typeof(decimal),
            typeof(double),
            typeof(DateTime),
            typeof(TimeSpan),
            typeof(Guid),
            typeof(bool?),
            typeof(short?),
            typeof(int?),
            typeof(long?),
            typeof(ushort?),
            typeof(uint?),
            typeof(ulong?),
            typeof(decimal?),
            typeof(double?),
            typeof(DateTime?),
            typeof(TimeSpan?),
            typeof(Guid?),
        };

        /// <summary>
        /// Obtains bool? variable from NULL-able column
        /// </summary>
        /// <param name="dr">The DataReader with results</param>
        /// <param name="ordinal">The index of the column</param>
        /// <returns>The result of `bool?` type</returns>
        public static bool?	GetNullableBool(this IDataReader dr,	int ordinal) { return dr[ordinal] != DBNull.Value ? (bool?)dr[ordinal]	: null; }

        /// <summary>
        /// Obtains byte? variable from NULL-able column
        /// </summary>
        /// <param name="dr">The DataReader with results</param>
        /// <param name="ordinal">The index of the column</param>
        /// <returns>The result of `byte?` type</returns>
		public static byte?	GetNullableByte(this IDataReader dr,	int ordinal) { return dr[ordinal] != DBNull.Value ? (byte?)dr[ordinal]	: null; }

        /// <summary>
        /// Obtains short? variable from NULL-able column
        /// </summary>
        /// <param name="dr">The DataReader with results</param>
        /// <param name="ordinal">The index of the column</param>
        /// <returns>The result of `short?` type</returns>
		public static short?	GetNullableInt16(this IDataReader dr,	int ordinal) { return dr[ordinal] != DBNull.Value ? (short?)dr[ordinal]	: null; }

        /// <summary>
        /// Obtains int? variable from NULL-able column
        /// </summary>
        /// <param name="dr">The DataReader with results</param>
        /// <param name="ordinal">The index of the column</param>
        /// <returns>The result of `int?` type</returns>
		public static int?	GetNullableInt32(this IDataReader dr,	int ordinal) { return dr[ordinal] != DBNull.Value ? (int?)(dr[ordinal])	: null; }

        /// <summary>
        /// Obtains long? variable from NULL-able column
        /// </summary>
        /// <param name="dr">The DataReader with results</param>
        /// <param name="ordinal">The index of the column</param>
        /// <returns>The result of `long?` type</returns>
		public static long?	GetNullableInt64(this IDataReader dr,	int ordinal) { return dr[ordinal] != DBNull.Value ? (long?)dr[ordinal]	: null; }

        /// <summary>
        /// Obtains float? variable from NULL-able column
        /// </summary>
        /// <param name="dr">The DataReader with results</param>
        /// <param name="ordinal">The index of the column</param>
        /// <returns>The result of `float?` type</returns>
		public static float?	GetNullableFloat(this IDataReader dr,	int ordinal) { return dr[ordinal] != DBNull.Value ? (float?)dr[ordinal]	: null; }

        /// <summary>
        /// Obtains double? variable from NULL-able column
        /// </summary>
        /// <param name="dr">The DataReader with results</param>
        /// <param name="ordinal">The index of the column</param>
        /// <returns>The result of `double?` type</returns>
		public static double?	GetNullableDouble(this IDataReader dr,	int ordinal) { return dr[ordinal] != DBNull.Value ? (double?)dr[ordinal]	: null; }

        /// <summary>
        /// Obtains decimal? variable from NULL-able column
        /// </summary>
        /// <param name="dr">The DataReader with results</param>
        /// <param name="ordinal">The index of the column</param>
        /// <returns>The result of `decimal?` type</returns>
        public static decimal? GetNullableDecimal(this IDataReader dr, int ordinal) { return dr[ordinal] != DBNull.Value ? (decimal?)dr[ordinal] : null; }

        /// <summary>
        /// Obtains Guid? variable from NULL-able column
        /// </summary>
        /// <param name="dr">The DataReader with results</param>
        /// <param name="ordinal">The index of the column</param>
        /// <returns>The result of `Guid?` type</returns>
		public static Guid?	GetNullableGuid(this IDataReader dr,	int ordinal) { return dr[ordinal] != DBNull.Value ? (Guid?)dr[ordinal]	: null; }

        /// <summary>
        /// Obtains DateTime? variable from NULL-able column
        /// </summary>
        /// <param name="dr">The DataReader with results</param>
        /// <param name="ordinal">The index of the column</param>
        /// <returns>The result of `DateTime?` type</returns>
		public static DateTime?	GetNullableDateTime(this IDataReader dr,	int ordinal) { return dr[ordinal] != DBNull.Value ? (DateTime?)dr[ordinal]	: null; }

        /// <summary>
        /// Obtains string variable from NULL-able column
        /// </summary>
        /// <param name="dr">The DataReader with results</param>
        /// <param name="ordinal">The index of the column</param>
        /// <returns>The result of `string` type</returns>
		public static string	GetNullableString(this IDataReader dr,	int ordinal) { return dr[ordinal] != DBNull.Value ? dr[ordinal].ToString()	: null; }
        
		public static Dictionary<string, string> BuildMapping<T>(Dictionary<string, string> irregular, Func<string, string> transform = null)
		{
			transform = transform ?? (str => str);
			PropertyDescriptorCollection properties = TypeDescriptor.GetProperties(typeof(T));

			var result = new Dictionary<string, string>(properties.Count);
			foreach (var kv in irregular)
				if (kv.Value != null)
					result.Add(kv.Key, kv.Value);

			foreach (PropertyDescriptor prop in properties)
			{
				if (irregular.ContainsKey(prop.Name)) continue;
				var value = transform(prop.Name);
				if (value == null) continue;
				result.Add(prop.Name, value);
			}

			return result;
		}
	}
}
