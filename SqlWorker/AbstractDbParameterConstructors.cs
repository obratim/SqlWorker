using System;
using System.Collections.Generic;
using System.Data.Common;

using System.Linq;

namespace SqlWorker
{
    /// <summary>
    /// Abstract class for creation DbParameter
    /// </summary>
    public abstract class AbstractDbParameterConstructors
    {
        /// <summary>
        /// Abstract method for creating DbParameter
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <param name="value">Parameter value</param>
        /// <param name="type">Parameter DBType, optional</param>
        /// <param name="direction">Parameter direction (Input / Output / InputOutput / ReturnValue), optional</param>
        /// <returns></returns>
        public abstract DbParameter Create(string name, object value, System.Data.DbType? type = null, System.Data.ParameterDirection? direction = null);
    }
}