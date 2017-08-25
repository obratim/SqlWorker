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
}
