using System;
using System.Data;

namespace SqlWorker
{
	/// <summary>
	/// Helpers specific to working with PostgreSQL
	/// </summary>
    public static class DataReaderExtensions
    {
        /// <summary>
        /// Gets array from DataReader by ordinal
        /// </summary>
        public static T[] GetArray<T>(this IDataReader dr, int n)
        {
            var v =  dr[n];
            if (v == null || v == DBNull.Value)
                return null;
            return (T[]) v;
        }
    }
}