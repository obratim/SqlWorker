using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace SqlWorker
{
    partial class ASqlWorker<TPC>
    {
        /// <summary>
        /// Replaces null-values to DBNull.Value constant
        /// </summary>
        /// <param name="param">Parameters, that will be sent to sql command</param>
        protected static void SqlParameterNullWorkaround(IDataParameter[] param)
        {
            foreach (var p in param)
                if (p.Value == null) p.Value = DBNull.Value;
        }

        /// <summary>
        /// Provides some implicit conversions to DbParameter[]
        /// </summary>
        public class DbParametersConstructor
        {
            private static readonly TPC Generator = new TPC();

            private static readonly IDataParameter[] _emptyParams = new IDataParameter[0];

            /// <summary>
            /// Constant that represents empty parameters array
            /// </summary>
            public static IDataParameter[] EmptyParams { get { return _emptyParams; } }

            private readonly IDataParameter[] _parameters;

            /// <summary>
            /// Returns array of DpParameter that are represented by current object
            /// </summary>
            public IDataParameter[] Parameters { get { return _parameters; } }

            /// <summary>
            /// Initialises new parameters set
            /// </summary>
            /// <param name="parameters">The array of parameters</param>
            private DbParametersConstructor(IDataParameter[] parameters)
            {
                _parameters = parameters ?? EmptyParams;
            }

            /// <summary>
            /// Returns the number of parameters
            /// </summary>
            /// <returns>The number of parameters</returns>
            public int Count()
            {
                return Parameters.Length;
            }

            /// <summary>
            /// Returns specified element of parameters's set
            /// </summary>
            /// <param name="i">The index of requested parameter</param>
            /// <returns>The requested parameter</returns>
            public IDataParameter this[int i] { get { return Parameters[i]; } }

            /// <summary>
            /// Returns specified element of parameters's set
            /// </summary>
            /// <param name="name">The name of requested parameter</param>
            /// <returns>The requested parameter</returns>
            /// <exception cref="T:System.InvalidOperationException">No parameters has specified <paramref name="name" />.-or-More than one parameter has specified <paramref name="name" />.-or-The source sequence is empty.</exception>
            public IDataParameter this[string name] { get { return Parameters.Single(p => p.ParameterName == name); } }

            /// <summary>
            /// Implicitly converts current object to DbParameter[]
            /// </summary>
            /// <param name="dbParametersConstructorObject">The current object</param>
            public static implicit operator IDataParameter[](DbParametersConstructor dbParametersConstructorObject)
            {
                return dbParametersConstructorObject.Parameters;
            }

            /// <summary>
            /// Implicitly converts from DbParameter[] to DbParametersConstructor
            /// </summary>
            /// <param name="vals">The source elements</param>
            public static implicit operator DbParametersConstructor(IDataParameter[] vals)
            {
                return new DbParametersConstructor(vals ?? EmptyParams);
            }

            /// <summary>
            /// Implicitly converts from a single DbParameter to DbParametersConstructor
            /// </summary>
            /// <param name="parameter">The single parameter for query</param>
            public static implicit operator DbParametersConstructor(System.Data.Common.DbParameter parameter)
            {
                return new DbParametersConstructor(new IDataParameter[1] { parameter });
            }

            /// <summary>
            /// Implicitly converts from Dictionary with param names and values to DbParametersConstructor
            /// </summary>
            /// <param name="vals">The source elements</param>
            public static implicit operator DbParametersConstructor(Dictionary<string, object> vals)
            {
                if (vals == null)
                    return new DbParametersConstructor(EmptyParams);

                var result = new IDataParameter[vals.Count];
                int i = 0;
                foreach (var kv in vals)
                {
                    result[i] = Generator.Create(kv.Key, kv.Value, null);
                    ++i;
                }
                return new DbParametersConstructor(result);
            }

            /// <summary>
            /// Implicitly converts from SwParameters to DbParametersConstructor
            /// </summary>
            /// <param name="vals">The source elements</param>
            public static implicit operator DbParametersConstructor(SwParameters vals)
            {
                if (vals == null)
                    return new DbParametersConstructor(EmptyParams);

                var result = new IDataParameter[vals.Count];
                for (int i = 0; i < vals.Count; ++i)
                {
                    result[i] = Generator.Create(vals[i].Item1, vals[i].Item2, vals[i].Item3, vals[i].Item4, vals[i].Item5);
                }
                return new DbParametersConstructor(result);
            }
        }
    }
}
