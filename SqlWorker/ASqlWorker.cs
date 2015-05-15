using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Common;
using System.Data;

namespace SqlWorker
{
    public abstract partial class ASqlWorker<TPC> : IDisposable where TPC : AbstractDbParameterConstructors, new()
    {
        protected abstract DbConnection Conn { get; }

        public TimeSpan ReConnectPause { get; set; }
        protected TimeSpan DefaultReconnectPause = new TimeSpan(0, 2, 0);
        protected DateTime? LastDisconnect;

        /// <summary>
        /// constructor
        /// </summary>
        /// <param name="reconnectPause">if null, default will be setted</param>
        public ASqlWorker(TimeSpan? reconnectPause = null)
        {
            ReConnectPause = reconnectPause ?? DefaultReconnectPause;
            LastDisconnect = DateTime.Now - reconnectPause;
        }

        /// <summary>
        /// serves to reopen connection after <ReConnectPause> time after last disconnect
        /// </summary>
        /// <param name="ReopenOnlyIfNotInTransaction">connection will be reopenned only if ReopenOnlyIfNotInTransaction=true and transaction is not openned</param>
        /// <returns>true - connection was opened</returns>
        virtual public bool OpenConnection(bool ReopenIfNotInTransaction = true)
        {
            if (Conn.State != ConnectionState.Open && ReConnectPause.Ticks > 0)
            {
                if (LastDisconnect == null)
                {
                    LastDisconnect = DateTime.Now;
                    return false;
                }

                if (DateTime.Now - LastDisconnect < ReConnectPause) return false;
            }
            else
            {
                if (TransactionIsOpened || !ReopenIfNotInTransaction) return true;
            }

            LastDisconnect = null;
            if (Conn.State != ConnectionState.Closed) Conn.Close();
            Conn.Open();
            _transactionIsOpened = false;
            return Conn.State == ConnectionState.Open;
        }


        //useless?
        virtual protected String QueryWithParams(String Query, DbParameter[] Params)
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

        #region Transactions
        virtual public void TransactionBegin()
        {
            if (TransactionIsOpened)
            {
                throw new Exception("transaction exists!");
            }
            if (Conn.State != ConnectionState.Open) Conn.Open();
            _transaction = Conn.BeginTransaction();
            _transactionIsOpened = true;
        }

        virtual public void TransactionCommit(bool closeConn = true)
        {
            if (!TransactionIsOpened) throw new Exception("transaction doesnt exist!");
            foreach (var i in Readers) if (i != null) { if (!i.IsClosed) { i.Close(); } i.Dispose(); }
            _transaction.Commit();
            if (closeConn) Conn.Close();
            _transactionIsOpened = false;
        }

        virtual public void TransactionRollback(bool closeConn = true)
        {
            if (!TransactionIsOpened) throw new Exception("transaction doesnt exist!");
            foreach (var i in Readers) if (i != null) { if (!i.IsClosed) { i.Close(); } i.Dispose(); }
            _transaction.Rollback();
            if (closeConn) Conn.Close();
            _transactionIsOpened = false;
        }

        public void DoInTransaction(Action todo, bool closeConn = true)
        {
            TransactionBegin();
            todo();
            TransactionCommit(closeConn);
        }

        private DbTransaction _transaction = null;
        private bool _transactionIsOpened = false;
        public bool TransactionIsOpened { get { return _transactionIsOpened; } }
        #endregion

        protected List<DbDataReader> Readers = new List<DbDataReader>();

        virtual public int ExecuteNonQuery(String Command, DbParametersConstructor vals = null, int? timeout = null, System.Data.CommandType? cmdtype = null)
        {
            try
            {
                vals = vals ?? DbParametersConstructor.emptyParams;
                SqlParameterNullWorkaround(vals);
                DbCommand cmd = Conn.CreateCommand();
                cmd.CommandText = cmdtype != System.Data.CommandType.StoredProcedure ? QueryWithParams(Command, vals) : Command;
                cmd.Parameters.AddRange(vals);
                if (cmdtype.HasValue) cmd.CommandType = cmdtype.Value;
                cmd.Transaction = _transaction;
                if (timeout != null) cmd.CommandTimeout = timeout.Value;
                if (Conn.State != ConnectionState.Open) Conn.Open();
                int result = cmd.ExecuteNonQuery();
                if (!TransactionIsOpened) cmd.Dispose();
                if (!TransactionIsOpened) Conn.Close();
                return result;
            }
            catch (Exception e)
            {
                if (Conn.State != ConnectionState.Closed)
                {
                    try { _transaction.Rollback(); _transactionIsOpened = false; }
                    catch { }
                    try { Conn.Close(); _transactionIsOpened = false; }
                    catch { }
                }
                throw e;
            }
        }

        virtual public int InsertValues(String TableName, DbParametersConstructor vals = null, bool ReturnIdentity = false, int? timeout = null)
        {
            SqlParameterNullWorkaround(vals);

            String q = "INSERT INTO " + TableName + " (" + vals[0].ParameterName;

            for (int i = 1; i < vals.Count(); ++i)
                q += ", " + vals[i].ParameterName;

            q += ") VALUES (@" + vals[0].ParameterName;

            for (int i = 1; i < vals.Count(); ++i)
                q += ", @" + vals[i].ParameterName;

            q += ");";

            return !ReturnIdentity ?
                ExecuteNonQuery(q, vals, timeout) :
                Decimal.ToInt32(GetStructFromDB<Decimal>(q + " select SCOPE_IDENTITY()", vals, r => { r.Read(); return r.GetDecimal(0); }));
        }

        virtual public int UpdateValues(String TableName, DbParametersConstructor Values, DbParametersConstructor Condition = null, int? timeout = null)
        {
            SqlParameterNullWorkaround(Values);
            Condition = Condition ?? DbParametersConstructor.emptyParams;

            String q = "UPDATE " + TableName + " SET " + Values[0].ParameterName + " = @" + Values[0].ParameterName;

            for (int i = 1; i < Values.Count(); ++i)
                q += ", " + Values[i].ParameterName + " = @" + Values[i].ParameterName;

            if (Condition.Count() > 0)
                q += " WHERE " + Condition[0].ParameterName + " = @" + Condition[0].ParameterName;

            for (int i = 1; i < Condition.Count(); ++i)
                q += " AND " + Condition[i].ParameterName + " = @" + Condition[i].ParameterName;

            List<DbParameter> param = new List<DbParameter>(Values.parameters);
            param.AddRange(Condition.parameters);
            return ExecuteNonQuery(q, param.ToArray(), timeout);
        }

        virtual public int UpdateValues(String TableName, DbParametersConstructor vals, String Condition, int? timeout = null)
        {
            SqlParameterNullWorkaround(vals);

            String q = "UPDATE " + TableName + " SET " + vals[0].ParameterName + " = @" + vals[0].ParameterName;

            for (int i = 1; i < vals.Count(); ++i)
                q += ", " + vals[i].ParameterName + " = @" + vals[i].ParameterName;

            if (!String.IsNullOrWhiteSpace(Condition))
                q += " WHERE " + Condition;

            return ExecuteNonQuery(q, vals, timeout);
        }


        virtual public T GetStructFromDB<T>(String Command, Func<DbDataReader, T> todo, int? timeout = null)
        { return GetStructFromDB<T>(Command, DbParametersConstructor.emptyParams, todo, timeout); }

        virtual public T GetStructFromDB<T>(String Command, DbParametersConstructor vals, Func<DbDataReader, T> todo, int? timeout = null)
        {
            try
            {
                vals = vals ?? DbParametersConstructor.emptyParams;
                SqlParameterNullWorkaround(vals);
                DbCommand cmd = Conn.CreateCommand();
                if (timeout.HasValue) cmd.CommandTimeout = timeout.Value;
                cmd.CommandText = QueryWithParams(Command, vals);
                cmd.Parameters.AddRange(vals);
                cmd.Transaction = _transaction;
                if (Conn.State != ConnectionState.Open) Conn.Open();
                DbDataReader dr = cmd.ExecuteReader();

                int drid = Readers.Count;
                Readers.Add(dr);

                T result = todo(dr);
                dr.Close();
                dr.Dispose();

                Readers.Remove(dr);

                cmd.Dispose();
                if (!TransactionIsOpened) Conn.Close();

                return result;
            }
            catch (Exception e)
            {
                if (Conn.State != ConnectionState.Closed)
                {
                    try { _transaction.Rollback(); _transactionIsOpened = false; }
                    catch { }
                    try { Conn.Close(); _transactionIsOpened = false; }
                    catch { }
                }
                throw e;
            }
        }


        virtual public List<T> GetListFromDBSingleProcessing<T>(String Command, Func<DbDataReader, T> todo, int? timeout = null)
        { return GetListFromDBSingleProcessing<T>(Command, DbParametersConstructor.emptyParams, todo, timeout); }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="Command"></param>
        /// <param name="vals"></param>
        /// <param name="todo">Delegate operates with single DataReader's record and return single T object</param>
        /// <returns></returns>
        virtual public List<T> GetListFromDBSingleProcessing<T>(string Command, DbParametersConstructor vals, Func<DbDataReader, T> todo, int? timeout = null)
        {
            return GetStructFromDB<List<T>>(Command, vals, delegate(DbDataReader dr)
            {
                List<T> output = new List<T>();
                while (dr.Read())
                {
                    output.Add(todo(dr));
                }
                return output;
            }, timeout);
        }

        virtual public List<T> GetListFromDB<T>(String procname, DbParametersConstructor vals = null, List<String> Exceptions = null, int? timeout = null) where T : new()
        {
            if (Exceptions != null) return GetStructFromDB<List<T>>(procname, vals, delegate(DbDataReader dr)
            {
                List<T> result = new List<T>();
                while (dr.Read())
                {
                    result.Add(DataReaderToObj<T>(dr, Exceptions));
                }
                return result;
            }, timeout);
            else return GetStructFromDB<List<T>>(procname, vals, delegate(DbDataReader dr)
            {
                List<T> result = new List<T>();
                while (dr.Read())
                {
                    result.Add(DataReaderToObj<T>(dr));
                }
                return result;
            }, timeout);
        }

        virtual public List<T> GetScalarsListFromDB<T>(String table, String column, DbParametersConstructor vals = null, String whereCondition = null, int? timeout = null)
        {
            vals = vals ?? DbParametersConstructor.emptyParams;

            if (String.IsNullOrWhiteSpace(whereCondition))
                whereCondition = vals.Count() == 0 ? ""
                    : vals.parameters.Aggregate<DbParameter, String, String>(
                        " WHERE "
                        , (value, i) => value + i.ParameterName + " = @" + i.ParameterName + "    AND\n\t" //aggregate constructions "<paramname> = @<paramname>    AND\n\t"
                        , (value) => value.Substring(0, value.Length - 6)           // then cut last " AND\n\t"
                    );

            return GetScalarsListFromDB<T>(String.Format("SELECT {0} FROM {1} {2}", column, table, whereCondition), vals, timeout);
        }

        virtual public List<T> GetScalarsListFromDB<T>(String query, DbParametersConstructor vals = null, int? timeout = null)
        {
            bool IncludingNulls = IsNullableParams(typeof(T));

            if (IncludingNulls)
                return GetListFromDBSingleProcessing<T>(
                    query,
                    vals,
                    (DbDataReader dr) => dr[0] == DBNull.Value ? (T)(Object)null : (T)dr[0],
                    timeout
                    );
            else return GetStructFromDB<List<T>>(query, vals, (dr) =>
            {
                List<T> result = new List<T>();
                while (dr.Read())
                    if (dr[0] != DBNull.Value) result.Add((T)dr[0]);
                return result;
            }, timeout);
        }

        virtual public List<Tuple<T0, T1>> GetTupleFromDB<T0, T1>(String query, DbParametersConstructor vals = null, int? timeout = null)
        {
            bool[] IncludingNulls = new bool[] { IsNullableParams(typeof(T0)), IsNullableParams(typeof(T1)) };
            return GetStructFromDB<List<Tuple<T0, T1>>>(query, vals,
                (dr) =>
                {
                    var result = new List<Tuple<T0, T1>>();
                    while (dr.Read())
                    {
                        var x0 = dr[0];
                        var x1 = dr[1];
                        if ((IncludingNulls[0] || x0 != DBNull.Value)
                            &&
                            (IncludingNulls[1] || x1 != DBNull.Value)
                           )
                            result.Add(new Tuple<T0, T1>((T0)(x0 == DBNull.Value ? null : x0), (T1)(x1 == DBNull.Value ? null : x1)));
                    }
                    return result;
                }, timeout);
        }

        virtual public T DataReaderToObj<T>(DbDataReader dr, List<String> Errors) where T : new()
        {
            T result = new T();
            foreach (System.ComponentModel.PropertyDescriptor i in System.ComponentModel.TypeDescriptor.GetProperties(result))
            {
                try { if (dr[i.Name] != DBNull.Value) i.SetValue(result, dr[i.Name]); }
                catch (Exception e) { Errors.Add(e.ToString()); }
            }

            return result;
        }

        virtual public T DataReaderToObj<T>(DbDataReader dr) where T : new()
        {
            T result = new T();
            foreach (System.ComponentModel.PropertyDescriptor i in System.ComponentModel.TypeDescriptor.GetProperties(result))
            {
                if (dr[i.Name] != DBNull.Value) i.SetValue(result, dr[i.Name]);
            }

            return result;
        }

        virtual public DataTable GetDataTable(String query, DbParametersConstructor vals = null, int? timeout = null)
        {
            return GetStructFromDB<DataTable>(query, vals, (dr) =>
            {
                var dt = new DataTable();
                dt.Load(dr);
                return dt;
            }
            , timeout);
        }

        #region Члены IDisposable

        public abstract void Dispose(bool commit);
        public virtual void Dispose() { Dispose(false); }

        #endregion
    }
}
