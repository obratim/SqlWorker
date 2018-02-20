using System;
using System.Collections.Generic;
using System.Data;

using System.Linq;

namespace SqlWorker
{
	/// <summary>
	/// Interface for creation IDataParameter
	/// </summary>
	public interface IDbParameterCreator
	{
		/// <summary>
		/// Abstract method for creating DbParameter
		/// </summary>
		/// <param name="name">Parameter name</param>
		/// <param name="value">Parameter value</param>
		/// <param name="type">Parameter DBType, optional</param>
		/// <param name="direction">Parameter direction (Input / Output / InputOutput / ReturnValue), optional</param>
		/// <returns></returns>
		IDataParameter Create(string name, object value, DbType? type = null, ParameterDirection? direction = null);
	}

	/// <summary>
	/// Generic method for creating DbParameter
	/// </summary>
	/// <typeparam name="T">Parameter type for target DBMS</typeparam>
	public abstract class ADbParameterCreator<T> : IDbParameterCreator
		where T : IDataParameter, new()
	{
		/// <summary>
		/// Method for creating parameters of type T
		/// </summary>
		/// <param name="name">Parameter name</param>
		/// <param name="value">Parameter value</param>
		/// <param name="type">Parameter DBType, optional</param>
		/// <param name="direction">Parameter direction (Input / Output / InputOutput / ReturnValue), optional</param>
		/// <returns></returns>
		public IDataParameter Create(string name, object value, DbType? type = null, ParameterDirection? direction = null)
		{
			var result = new T();
			result.ParameterName = name;
			if (type.HasValue) result.DbType = type.Value;
			result.Value = value;
			if (direction.HasValue) result.Direction = direction.Value;
			return result;
		}
	}
}
