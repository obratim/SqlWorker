using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace SqlWorker
{
	public class EnumerableDataReader<T> : IDataReader
	{
		public EnumerableDataReader(IEnumerable<T> source)
		{
			_enumerator = source.GetEnumerator();
			_properties = System.ComponentModel.TypeDescriptor.GetProperties(typeof(T));
			_dataTable = new DataTable();

			foreach (System.ComponentModel.PropertyDescriptor prop in _properties)
			{
				_dataTable.Columns.Add(prop.Name, Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType);
			}
		}

		private DataTable _dataTable;
		private IEnumerator<T> _enumerator;

		private System.ComponentModel.PropertyDescriptorCollection _properties;

		public object this[int i]
		{
			get
			{
				Console.WriteLine("...getting property [{0}]", i);
				return _properties[i].GetValue(_enumerator.Current);
			}
		}

		public object this[string name]
		{
			get
			{
				Console.WriteLine("...getting property ['{0}']", name);
				return _properties[name].GetValue(_enumerator.Current);
			}
		}

		public int Depth => 0;

		public bool IsClosed { get; private set; } = false;

		public int RecordsAffected => -1;

		public int FieldCount
		{
			get
			{
				Console.WriteLine("...getting fields count");
				Console.WriteLine(_dataTable);
				Console.WriteLine(_dataTable?.Columns);
				Console.WriteLine(_dataTable?.Columns?.Count);
				var result = _dataTable.Columns.Count;
				Console.WriteLine("...got columns count");
				return result;
			}
		}

		public void Close()
		{
		}

		public void Dispose()
		{
			Console.WriteLine("...disposing");
			_enumerator.Dispose();
			IsClosed = true;
			Console.WriteLine("...disposed");
		}

		public bool GetBoolean(int i) => (bool)this[i];
		public byte GetByte(int i) => (byte)this[i];

		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
		{
			return 0;
			throw new NotImplementedException();
		}

		public char GetChar(int i) => (char)this[i];

		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
		{
			return 0;
			throw new NotImplementedException();
		}

		public IDataReader GetData(int i)
		{
			Console.WriteLine("...getting 'data'");
			return null;
			throw new NotImplementedException();
		}

		public string GetDataTypeName(int i) => _properties[i].PropertyType.Name;
		public DateTime GetDateTime(int i) => (DateTime)this[i];
		public decimal GetDecimal(int i) => (decimal)this[i];
		public double GetDouble(int i) => (double)this[i];
		public Type GetFieldType(int i) => _properties[i].PropertyType;
		public float GetFloat(int i) => (float)this[i];
		public Guid GetGuid(int i) => (Guid)this[i];
		public short GetInt16(int i) => (short)this[i];
		public int GetInt32(int i) => (int)this[i];
		public long GetInt64(int i) => (long)this[i];
		public string GetString(int i) => (string)this[i];
		public object GetValue(int i) => this[i];

		public int GetValues(object[] values)
		{
			return 0;
			throw new NotImplementedException();
		}

		public bool IsDBNull(int i) => this[i] == DBNull.Value;

		public string GetName(int i) => _dataTable.Columns[i].ColumnName;

		public int GetOrdinal(string name) => _dataTable.Columns.IndexOf(name);

		public DataTable GetSchemaTable() => _dataTable;

		public bool NextResult()
		{
			Console.WriteLine("...next result");
			_enumerator.Dispose();
			_enumerator = System.Linq.Enumerable.Empty<T>().GetEnumerator();
			IsClosed = true;
			Console.WriteLine("...next result");
			return false;
		}

		public bool Read()
		{
			Console.WriteLine("...reading next value");
			return !(IsClosed = !_enumerator.MoveNext());
		}
	}
}
