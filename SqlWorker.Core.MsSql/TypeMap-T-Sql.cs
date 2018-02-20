using System;
using System.Collections.Generic;
using System.Data;

using System.Linq;

namespace SqlWorker
{
    public partial class MsSqlWorker
	{
        /// <summary>
        /// Dictionary that maps c# types to SqlDbTypes
        /// </summary>
        protected static readonly Dictionary<Type, SqlDbType> TypeMapTsql = new Dictionary<Type, SqlDbType> {
	        { typeof(byte) , SqlDbType.TinyInt },
	        //{ typeof(sbyte) , SqlDbType.SByte },
	        { typeof(short) , SqlDbType.SmallInt },
	        //{ typeof(ushort) , SqlDbType.UInt16 },
	        { typeof(int) , SqlDbType.Int },
	        //{ typeof(uint) , SqlDbType.UInt32 },
	        { typeof(long) , SqlDbType.BigInt },
	        //{ typeof(ulong) , SqlDbType.UInt64 },
	        { typeof(float) , SqlDbType.Real },
	        { typeof(double) , SqlDbType.Float },
	        { typeof(decimal) , SqlDbType.Decimal },
	        { typeof(bool) , SqlDbType.Bit },
	        { typeof(string) , SqlDbType.NVarChar },
	        { typeof(char) , SqlDbType.NChar },
	        { typeof(Guid) , SqlDbType.UniqueIdentifier },
	        { typeof(DateTime) , SqlDbType.DateTime },
	        { typeof(DateTimeOffset) , SqlDbType.DateTimeOffset },
	        { typeof(byte[]) , SqlDbType.VarBinary },
	        { typeof(byte?) , SqlDbType.TinyInt },
	        //{ typeof(sbyte?) , SqlDbType.SByte },
	        { typeof(short?) , SqlDbType.SmallInt },
	        //{ typeof(ushort?) , SqlDbType.UInt16 },
	        { typeof(int?) , SqlDbType.Int },
	        //{ typeof(uint?) , SqlDbType.UInt32 },
	        { typeof(long?) , SqlDbType.BigInt },
	        //{ typeof(ulong?) , SqlDbType.UInt64 },
	        { typeof(float?) , SqlDbType.Real },
	        { typeof(double?) , SqlDbType.Float },
	        { typeof(decimal?) , SqlDbType.Decimal },
	        { typeof(bool?) , SqlDbType.Bit },
	        { typeof(char?) , SqlDbType.NChar },
	        { typeof(Guid?) , SqlDbType.UniqueIdentifier },
	        { typeof(DateTime?) , SqlDbType.DateTime },
	        { typeof(DateTimeOffset?) , SqlDbType.DateTimeOffset },
            { typeof(TimeSpan), SqlDbType.Timestamp },
            { typeof(TimeSpan?), SqlDbType.Timestamp },
        };
    }
}