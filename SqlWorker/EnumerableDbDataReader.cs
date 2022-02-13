using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace SqlWorker
{
	/// <summary>
	/// Converts sequence of typed elements to DbDataReader
	/// </summary>
	/// <typeparam name="T">Type of elements</typeparam>
	public class EnumerableDbDataReader<T> : System.Data.Common.DbDataReader
	{
		private static readonly DataTable DataTable;
		private static readonly System.ComponentModel.PropertyDescriptorCollection Properties;

		static EnumerableDbDataReader()
		{
			DataTable = new DataTable();
			Properties = System.ComponentModel.TypeDescriptor.GetProperties(typeof(T));
			
			foreach (System.ComponentModel.PropertyDescriptor prop in Properties)
			{
				DataTable.Columns.Add(prop.Name, Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType);
			}
		}

		/// <summary>
		/// Creates DbDataReader out of specific sequence
		/// </summary>
		/// <param name="source">The sequence, out of what the DbDataReader must be created</param>
		public EnumerableDbDataReader(IEnumerable<T> source)
		{
			_enumerator = source.GetEnumerator();
		}

		private IEnumerator<T> _enumerator;


		/// <summary>
		/// Get i-th value of current record
		/// </summary>
		/// <returns>object, i-th value of current record</returns>
		public override object this[int i] => Properties[i].GetValue(_enumerator.Current);

		/// <summary>
		/// Get value of current record by name
		/// </summary>
		/// <returns>object, value of current record from named field</returns>
		public override object this[string name] => Properties[name].GetValue(_enumerator.Current);

		/// <summary>
		/// Always 0
		/// </summary>
		public override int Depth => 0;

		private bool _isClosed = false;
		
		/// <summary>
		/// Shows if reading is possible, if true - no more elements left in sequence
		/// </summary>
		public override bool IsClosed => _isClosed;

		/// <summary>
		/// Alwayys -1
		/// </summary>
		public override int RecordsAffected => -1;

		/// <summary>
		/// Columns count (columns are created based on what TypeDescriptor.GetProperties got from type of elements)
		/// </summary>
		public override int FieldCount => DataTable.Columns.Count;

		/// <summary>
		/// Shows if reading is possible, if false - no more elements left in sequence
		/// </summary>
		public override bool HasRows => !_isClosed;

		/// <summary>
		/// Stop reading
		/// </summary>
		public override void Close()
		{
			NextResult();
		}
		
		/// <summary>
		/// Get i-th value of current record and converts to <c cref="bool">bool</c>
		/// </summary>
		/// <returns><c cref="bool">bool</c>, i-th value of current record</returns>
		public override bool GetBoolean(int i) => (bool)this[i];
		
		/// <summary>
		/// Get i-th value of current record and converts to <c cref="byte">byte</c>
		/// </summary>
		/// <returns><c cref="byte">byte</c>, i-th value of current record</returns>
		public override byte GetByte(int i) => (byte)this[i];

		/// <summary>
		/// Not implemented, returns 0
		/// </summary>
		public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
		{
			return 0;
			throw new NotImplementedException("GetBytes mthod not implemented for EnumerableDbDataReader");
		}

		/// <summary>
		/// Get i-th value of current record and converts to <c cref="char">char</c>
		/// </summary>
		/// <returns><c cref="char">char</c>, i-th value of current record</returns>
		public override char GetChar(int i) => (char)this[i];

		/// <summary>
		/// Not implemented, returns 0
		/// </summary>
		public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
		{
			return 0;
			throw new NotImplementedException();
		}
		
		/// <summary>
		/// Get type name of i-th field
		/// </summary>
		/// <returns>Type's FullName</returns>
		public override string GetDataTypeName(int i) => Properties[i].PropertyType.FullName;
		
		/// <summary>
		/// Get i-th value of current record and converts to <c cref="DateTime">DateTime</c>
		/// </summary>
		/// <returns><c cref="DateTime">DateTime</c>, i-th value of current record</returns>
		public override DateTime GetDateTime(int i) => (DateTime)this[i];
		
		/// <summary>
		/// Get i-th value of current record and converts to <c cref="decimal">decimal</c>
		/// </summary>
		/// <returns><c cref="decimal">decimal</c>, i-th value of current record</returns>
		public override decimal GetDecimal(int i) => (decimal)this[i];
		
		/// <summary>
		/// Get i-th value of current record and converts to <c cref="double">double</c>
		/// </summary>
		/// <returns><c cref="double">double</c>, i-th value of current record</returns>
		public override double GetDouble(int i) => (double)this[i];
		
		/// <summary>
		/// Get type of i-th field
		/// </summary>
		/// <returns><c cref="System.Type">Type</c></returns>
		public override Type GetFieldType(int i) => Properties[i].PropertyType;
		
		/// <summary>
		/// Get i-th value of current record and converts to <c cref="float">float</c>
		/// </summary>
		/// <returns><c cref="float">float</c>, i-th value of current record</returns>
		public override float GetFloat(int i) => (float)this[i];
		
		/// <summary>
		/// Get i-th value of current record and converts to <c cref="int">int</c>
		/// </summary>
		/// <returns><c cref="int">int</c>, i-th value of current record</returns>
		public override Guid GetGuid(int i) => (Guid)this[i];
		
		/// <summary>
		/// Get i-th value of current record and converts to <c cref="short">short</c>
		/// </summary>
		/// <returns><c cref="short">short</c>, i-th value of current record</returns>
		public override short GetInt16(int i) => (short)this[i];
		
		/// <summary>
		/// Get i-th value of current record and converts to int
		/// </summary>
		/// <returns><c cref="int">int</c>, i-th value of current record</returns>
		public override int GetInt32(int i) => (int)this[i];
		
		/// <summary>
		/// Get i-th value of current record and converts to long
		/// </summary>
		/// <returns><c cref="long">long</c>, i-th value of current record</returns>
		public override long GetInt64(int i) => (long)this[i];
		
		/// <summary>
		/// Get i-th value of current record and converts to string
		/// </summary>
		/// <returns><c cref="string">string</c>, i-th value of current record</returns>
		public override string GetString(int i) => (string)this[i];
		
		/// <summary>
		/// Get i-th value of current record
		/// </summary>
		/// <returns>object, i-th value of current record</returns>
		public override object GetValue(int i) => this[i];

		/// <summary>
		/// Not implemented, returns 0
		/// </summary>
		public override int GetValues(object[] values)
		{
			return 0;
			throw new NotImplementedException();
		}

		public override bool IsDBNull(int i) => this[i] == DBNull.Value;

		/// <summary>
		/// Get name of i-th field
		/// </summary>
		/// <returns>Name of property</returns>
		public override string GetName(int i) => DataTable.Columns[i].ColumnName;

		/// <summary>
		/// Get ordinal of column with specified name
		/// </summary>
		/// <returns>Column ordinal by name</returns>
		public override int GetOrdinal(string name) => DataTable.Columns.IndexOf(name);

		/// <summary>
		/// DataTable whith columns and without rows, that is generated in static constructor
		/// </summary>
		public override DataTable GetSchemaTable() => DataTable;

		/// <summary>
		/// Stop reading
		/// </summary>
		/// <returns>false</returns>
		public override bool NextResult()
		{
			_enumerator.Dispose();
			_enumerator = System.Linq.Enumerable.Empty<T>().GetEnumerator();
			_isClosed = true;
			return false;
		}

		/// <summary>
		/// Tries to receive next value from sequence
		/// </summary>
		/// <returns>True if sequence had yet another element, else - false</returns>
		public override bool Read() => !(_isClosed = !_enumerator.MoveNext());

		/// <summary>
		/// Current IEnumerator, that is used to receive values from sequence
		/// </summary>
		public override IEnumerator GetEnumerator()
		{
			return _enumerator;
		}

		private bool _disposed = false;

		/// <summary>
		/// Stop reading and dispose current IEnumerator
		/// </summary>
		protected override void Dispose(bool disposing)
		{
			_isClosed = true;
			if (_disposed)
				return;
			if (disposing)
			{
				_enumerator?.Dispose();
			}
			_disposed = true;
			base.Dispose(disposing);
		}


		/// <summary>
		/// Destructor, performs <c>Dispose(false);</c>
		/// </summary>
		~EnumerableDbDataReader()
		{
			Dispose(false);
		}
	}
}
