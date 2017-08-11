using System;
using System.Collections.Generic;
using System.Data;

using System.Linq;

namespace SqlWorker
{
    /// <summary>
    /// Class that helps declarate parameters
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class SWParameters<T> : List<T>
    {
    }

    /// <summary>
    /// Class that helps declarate parameters
    /// </summary>
    public class SWParameters : SWParameters<Tuple<string, object, DbType?, ParameterDirection?>>
    {
        /// <summary>
        /// method for declaring another one parameter
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <param name="value">Parameter value</param>
        /// <param name="type">Parameter DBType, optional</param>
        /// <param name="direction">Parameter direction (Input / Output / InputOutput / ReturnValue), optional</param>
        public void Add(string name, object value, DbType? type = null, ParameterDirection? direction = null)
        {
            this.Add(new Tuple<string, object, DbType?, ParameterDirection?>(name, value, type, direction));
        }
    }
}