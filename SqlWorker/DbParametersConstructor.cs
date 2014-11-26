using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Common;

namespace SqlWorker
{
    public abstract partial class ASqlWorker<T> where T : AbstractDbParameterConstructors, new()
    {
        public class DbParametersConstructor
        {
            private static T x = new T();

            public static readonly DbParameter[] emptyParams = new DbParameter[0];

            public static DbParameter[] DictionaryToDbParameters(Dictionary<String, Object> input)
            {
                var result = new DbParameter[input.Count];
                int i = 0;
                foreach (var kv in input)
                {
                    result[i] = x.By2(kv.Key, kv.Value);
                    ++i;
                }
                return result;
            }

            private DbParameter[] _parameters;
            public DbParameter[] parameters { get { return _parameters; } }

            public DbParametersConstructor(DbParameter[] parameters)
            {
                _parameters = parameters;
            }

            public int Count() { return parameters.Count(); }

            public DbParameter this[int i] { get { return parameters[i]; } }

            public static implicit operator DbParameter[](DbParametersConstructor DbParametersConstructorObject) { return DbParametersConstructorObject.parameters; }

            public static implicit operator DbParametersConstructor(DbParameter[] vals) { return new DbParametersConstructor(vals); }
            public static implicit operator DbParametersConstructor(DbParameter vals) { return new DbParametersConstructor(new DbParameter[1] { vals }); }
            public static implicit operator DbParametersConstructor(Dictionary<String, Object> vals)
            {
                return new DbParametersConstructor(DictionaryToDbParameters(vals));
            }
            public static implicit operator DbParametersConstructor(ValueNameList vals)
            {
                var result = new DbParameter[vals.Count];
                int j = 0;
                foreach (var i in vals)
                {
                    result[j] = x.By2(i.Item1, i.Item2);
                    ++j;
                }
                return new DbParametersConstructor(result);
            }
            public static implicit operator DbParametersConstructor(ValueNameTypeList vals)
            {
                var result = new DbParameter[vals.Count];
                int j = 0;
                foreach (var i in vals)
                {
                    result[j] = x.By3(i.Item1, i.Item2, i.Item3);
                    ++j;
                }
                return new DbParametersConstructor(result);
            }
        }
    }
}