using System.Data;

namespace SqlWorker
{
    public static class DataReaderExtensions
    {
        public static T[] GetArray<T>(this IDataReader dr, int n)
        {
            var v =  dr[n];
            return (T[]) v;
        }
    }
}