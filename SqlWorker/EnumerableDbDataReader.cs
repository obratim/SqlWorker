﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace SqlWorker
{
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

		public EnumerableDbDataReader(IEnumerable<T> source)
		{
			_enumerator = source.GetEnumerator();
		}

		private IEnumerator<T> _enumerator;


		public override object this[int i] => Properties[i].GetValue(_enumerator.Current);

		public override object this[string name] => Properties[name].GetValue(_enumerator.Current);

		public override int Depth => 0;

		private bool _isClosed = false;
		public override bool IsClosed => _isClosed;

		public override int RecordsAffected => -1;

		public override int FieldCount => DataTable.Columns.Count;

		public override bool HasRows => throw new NotImplementedException();

		public override void Close()
		{
			NextResult();
		}
		
		public override bool GetBoolean(int i) => (bool)this[i];
		public override byte GetByte(int i) => (byte)this[i];

		public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
		{
			return 0;
			throw new NotImplementedException();
		}

		public override char GetChar(int i) => (char)this[i];

		public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
		{
			return 0;
			throw new NotImplementedException();
		}
		
		public override string GetDataTypeName(int i) => Properties[i].PropertyType.Name;
		public override DateTime GetDateTime(int i) => (DateTime)this[i];
		public override decimal GetDecimal(int i) => (decimal)this[i];
		public override double GetDouble(int i) => (double)this[i];
		public override Type GetFieldType(int i) => Properties[i].PropertyType;
		public override float GetFloat(int i) => (float)this[i];
		public override Guid GetGuid(int i) => (Guid)this[i];
		public override short GetInt16(int i) => (short)this[i];
		public override int GetInt32(int i) => (int)this[i];
		public override long GetInt64(int i) => (long)this[i];
		public override string GetString(int i) => (string)this[i];
		public override object GetValue(int i) => this[i];

		public override int GetValues(object[] values)
		{
			return 0;
			throw new NotImplementedException();
		}

		public override bool IsDBNull(int i) => this[i] == DBNull.Value;

		public override string GetName(int i) => DataTable.Columns[i].ColumnName;

		public override int GetOrdinal(string name) => DataTable.Columns.IndexOf(name);

		public override DataTable GetSchemaTable() => DataTable;

		public override bool NextResult()
		{
			_enumerator.Dispose();
			_enumerator = System.Linq.Enumerable.Empty<T>().GetEnumerator();
			_isClosed = true;
			return false;
		}

		public override bool Read() => !(_isClosed = !_enumerator.MoveNext());

		public override IEnumerator GetEnumerator()
		{
			return _enumerator;
		}

		private bool disposed = false;

		protected override void Dispose(bool disposing)
		{
			if (disposed)
				return;
			if (disposing)
			{
				_enumerator.Dispose();
			}
			disposed = true;
			base.Dispose(disposing);
		}

		~EnumerableDbDataReader()
		{
			Dispose(false);
		}
	}
}
