using System;
using System.Collections.Generic;
using System.Data;

using System.Linq;

namespace SqlWorker
{
    /// <summary>
    /// Class that helps declarate parameters
    /// </summary>
    public class SwParameters : List<(string name, object value, DbType? type, ParameterDirection? direction, int? size)>
    {
        /// <summary>
        /// method for declaring another one parameter
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <param name="value">Parameter value</param>
        /// <param name="type">Parameter DBType, optional</param>
        /// <param name="direction">Parameter direction (Input / Output / InputOutput / ReturnValue), optional</param>
        /// <param name="size">Parameter size (for types with variable size)</param>
        public void Add(string name, object value, DbType? type = null, ParameterDirection? direction = null, int? size = null)
        {
            this.Add((name, value, type, direction, size));
        }
    }
}