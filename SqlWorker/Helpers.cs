using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.ComponentModel;
using System.Data.Common;

namespace SqlWorker
{
	public static class Helpers
	{
		public static DataTable AsDataTable<T>(this IEnumerable<T> data)
		{
			PropertyDescriptorCollection properties = TypeDescriptor.GetProperties(typeof(T));
			var table = new DataTable();
			foreach (PropertyDescriptor prop in properties)
				table.Columns.Add(prop.Name, Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType);
			foreach (T item in data)
			{
				DataRow row = table.NewRow();
				foreach (PropertyDescriptor prop in properties)
					row[prop.Name] = prop.GetValue(item) ?? DBNull.Value;
				table.Rows.Add(row);
			}
			return table;
		}

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

        public static bool?	GetNullableBool(this DbDataReader dr,	int ordinal) { return dr[ordinal] != DBNull.Value ? (bool?)dr[ordinal]	: null; }
		public static byte?	GetNullableByte(this DbDataReader dr,	int ordinal) { return dr[ordinal] != DBNull.Value ? (byte?)dr[ordinal]	: null; }
		public static short?	GetNullableInt16(this DbDataReader dr,	int ordinal) { return dr[ordinal] != DBNull.Value ? (short?)dr[ordinal]	: null; }
		public static int?	GetNullableInt32(this DbDataReader dr,	int ordinal) { return dr[ordinal] != DBNull.Value ? (int?)(dr[ordinal])	: null; }
		public static long?	GetNullableInt64(this DbDataReader dr,	int ordinal) { return dr[ordinal] != DBNull.Value ? (long?)dr[ordinal]	: null; }
		public static float?	GetNullableFloat(this DbDataReader dr,	int ordinal) { return dr[ordinal] != DBNull.Value ? (float?)dr[ordinal]	: null; }
		public static double?	GetNullableDouble(this DbDataReader dr,	int ordinal) { return dr[ordinal] != DBNull.Value ? (double?)dr[ordinal]	: null; }
		public static Guid?	GetNullableGuid(this DbDataReader dr,	int ordinal) { return dr[ordinal] != DBNull.Value ? (Guid?)dr[ordinal]	: null; }
		public static DateTime?	GetNullableDateTime(this DbDataReader dr,	int ordinal) { return dr[ordinal] != DBNull.Value ? (DateTime?)dr[ordinal]	: null; }
		public static String	GetNullableString(this DbDataReader dr,	int ordinal) { return dr[ordinal] != DBNull.Value ? dr[ordinal].ToString()	: null; }
	}
}
