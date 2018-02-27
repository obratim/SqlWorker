using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.ComponentModel;
using System.Data.Common;

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
			PropertyDescriptorCollection properties = TypeDescriptor.GetProperties(typeof(T));
			var table = new DataTable();
			foreach (PropertyDescriptor prop in properties)
				table.Columns.Add(prop.Name, Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType);
			foreach (T item in source)
			{
				DataRow row = table.NewRow();
			    for (var i = 0; i < properties.Count; ++i)
			    {
			        PropertyDescriptor prop = properties[i];
			        row[i] = prop.GetValue(item) ?? DBNull.Value;
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
	        var properties = TypeDescriptor.GetProperties(typeof (T));
	        using (var table = new DataTable())
	        {
	            foreach (PropertyDescriptor property in properties)
                    table.Columns.Add(property.Name,
	                    Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType);

                using (var enumerator = source.Batch(chunkSize).GetEnumerator())
                {
                    if (!enumerator.MoveNext()) yield break;

                    // initially fill table
                    foreach (T item in enumerator.Current)
                    {
                        DataRow row = table.NewRow();
                        for (var i = 0; i < properties.Count; ++i)
                        {
                            PropertyDescriptor prop = properties[i];
                            row[i] = prop.GetValue(item) ?? DBNull.Value;
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
                                PropertyDescriptor prop = properties[i];
                                table.Rows[rowNumber][i] = prop.GetValue(item) ?? DBNull.Value;
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
