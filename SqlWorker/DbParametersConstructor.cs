using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Common;

namespace SqlWorker
{
    public abstract partial class ASqlWorker<TPC> where TPC : AbstractDbParameterConstructors, new()
    {
        #region parameters management

        //useless?
        protected static String QueryWithParams(String Query, DbParameter[] Params)
        {
            if (Params == null) return Query;

            String newq = Query;
            bool firstParam = true;

            if (newq.IndexOf('@') != -1) firstParam = false;
            foreach (var p in Params)
            {
                if (newq.IndexOf("@" + p.ParameterName) == -1) newq += (firstParam ? " @" : ", @") + p.ParameterName;
                firstParam = false;
            }
            return newq;
        }

        protected static void SqlParameterNullWorkaround(DbParameter[] param)
        {
            foreach (var p in param)
                if (p.Value == null) p.Value = DBNull.Value;
        }

        protected static DbParameter[] NotNullParams(DbParameter[] param)
        {
            return (from DbParameter p in param
                    where p.Value != null
                    select p).ToArray();
        }

        protected bool IsNullableParams(params Type[] types)
        {
            bool result = true;
            foreach (var i in types)
                result = result && i.IsGenericType && i.GetGenericTypeDefinition() == typeof(Nullable<>);
            return result;
        }

        #endregion

        public class DbParametersConstructor
        {
            private static TPC generator = new TPC();

            static readonly DbParameter[] _emptyParams = new DbParameter[0];
            public static DbParameter[] emptyParams { get { return _emptyParams; } }

            public static DbParameter[] DictionaryToDbParameters(Dictionary<String, Object> input)
            {
                var result = new DbParameter[input.Count];
                int i = 0;
                foreach (var kv in input)
                {
                    result[i] = generator.Create(kv.Key, kv.Value, null);
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
            public static implicit operator DbParametersConstructor(SWParameters vals)
            {
                var result = new DbParameter[vals.Count];
                int j = 0;
                for (int i = 0; i < vals.Count; ++i)
                {
                    result[j] = generator.Create(vals[i].Item1, vals[i].Item2, vals[i].Item3, vals[i].Item4);
                    ++j;
                }
                return new DbParametersConstructor(result);
            }
        }
    }
}