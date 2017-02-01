using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

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
        virtual public bool ReOpenConnection(bool reopenIfNotInTransaction = true)
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
                if (TransactionIsOpened || !reopenIfNotInTransaction) return true;
            }

            LastDisconnect = null;
            if (TransactionIsOpened) TransactionRollback();
            if (Conn.State != ConnectionState.Closed)
            {
                Conn.Close();
            }
            Conn.Open();
            
            return Conn.State == ConnectionState.Open;
        }

        #region Transactions

        /// <summary>
        /// Only single one transaction is supported!
        /// </summary>
        virtual public void TransactionBegin(IsolationLevel SpecificIsolationLevel = IsolationLevel.ReadCommitted)
        {
            if (TransactionIsOpened)
            {
                throw new Exception("transaction exists!");
            }
            if (Conn.State != ConnectionState.Open) Conn.Open();
            _transaction = Conn.BeginTransaction(SpecificIsolationLevel);
            _transactionIsOpened = true;
        }

        virtual public void TransactionCommit(bool closeConn = true)
        {
            if (!TransactionIsOpened) throw new Exception("transaction doesnt exist!");
            _transaction.Commit();
            _transaction.Dispose();
            if (closeConn) Conn.Close();
            _transactionIsOpened = false;
        }

        virtual public void TransactionRollback(bool closeConn = true)
        {
            if (!TransactionIsOpened) throw new Exception("transaction doesnt exist!");
            _transaction.Rollback();
            _transaction.Dispose();
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

        #endregion Transactions

        virtual public int Exec(String command, DbParametersConstructor vals = null, int? timeout = null, CommandType commandType = CommandType.Text)
        {
            return ExecuteNonQuery(command, vals, timeout, commandType);
        }

        virtual public int ExecuteNonQuery(String command, DbParametersConstructor vals = null, int? timeout = null, CommandType commandType = CommandType.Text)
        {
            try
            {
                int result;
                vals = vals ?? DbParametersConstructor.emptyParams;
                SqlParameterNullWorkaround(vals);
                using (DbCommand cmd = Conn.CreateCommand())
                {
                    cmd.CommandText = command;
                    cmd.Parameters.AddRange(vals);
                    cmd.CommandType = commandType;
                    cmd.Transaction = _transaction;
                    if (timeout != null) cmd.CommandTimeout = timeout.Value;
                    if (Conn.State != ConnectionState.Open) Conn.Open();
                    result = cmd.ExecuteNonQuery();
                }
                if (!TransactionIsOpened) Conn.Close();
                return result;
            }
            catch
            {
                if (Conn.State != ConnectionState.Closed)
                {
                    try { _transaction.Rollback(); _transactionIsOpened = false; }
                    catch { }
                    try { Conn.Close(); _transactionIsOpened = false; }
                    catch { }
                }
                throw;
            }
        }

        virtual public int InsertValues(String tableName, DbParametersConstructor vals = null, bool returnIdentity = false, int? timeout = null)
        {
            SqlParameterNullWorkaround(vals);

            String q = "INSERT INTO " + tableName + " (" + vals[0].ParameterName;

            for (int i = 1; i < vals.Count(); ++i)
                q += ", " + vals[i].ParameterName;

            q += ") VALUES (@" + vals[0].ParameterName;

            for (int i = 1; i < vals.Count(); ++i)
                q += ", @" + vals[i].ParameterName;

            q += ");";

            return !returnIdentity ?
                ExecuteNonQuery(q, vals, timeout) :
                Decimal.ToInt32(ManualProcessing(
                q + " select SCOPE_IDENTITY()",
                r => { r.Read(); return r.GetDecimal(0); },
                vals));
        }

        virtual public int UpdateValues(String tableName, DbParametersConstructor values, DbParametersConstructor condition = null, int? timeout = null)
        {
            SqlParameterNullWorkaround(values);
            condition = condition ?? DbParametersConstructor.emptyParams;

            String q = "UPDATE " + tableName + " SET " + values[0].ParameterName + " = @" + values[0].ParameterName;

            for (int i = 1; i < values.Count(); ++i)
                q += ", " + values[i].ParameterName + " = @" + values[i].ParameterName;

            if (condition.Count() > 0)
                q += " WHERE " + condition[0].ParameterName + " = @" + condition[0].ParameterName;

            for (int i = 1; i < condition.Count(); ++i)
                q += " AND " + condition[i].ParameterName + " = @" + condition[i].ParameterName;

            List<DbParameter> param = new List<DbParameter>(values.parameters);
            param.AddRange(condition.parameters);
            return ExecuteNonQuery(q, param.ToArray(), timeout);
        }

        virtual public int UpdateValues(String tableName, DbParametersConstructor vals, String condition, int? timeout = null)
        {
            SqlParameterNullWorkaround(vals);

            String q = "UPDATE " + tableName + " SET " + vals[0].ParameterName + " = @" + vals[0].ParameterName;

            for (int i = 1; i < vals.Count(); ++i)
                q += ", " + vals[i].ParameterName + " = @" + vals[i].ParameterName;

            if (!String.IsNullOrWhiteSpace(condition))
                q += " WHERE " + condition;

            return ExecuteNonQuery(q, vals, timeout);
        }

        virtual public T ManualProcessing<T>(String command, Func<DbDataReader, T> todo, DbParametersConstructor vals = null, int? timeout = null, CommandType commandType = CommandType.Text)
        {
            try
            {
                vals = vals ?? DbParametersConstructor.emptyParams;
                SqlParameterNullWorkaround(vals);
                T result;
                using (DbCommand cmd = Conn.CreateCommand())
                {
                    cmd.CommandType = commandType;
                    if (timeout.HasValue) cmd.CommandTimeout = timeout.Value;
                    cmd.CommandText = command;
                    cmd.Parameters.AddRange(vals);
                    cmd.Transaction = _transaction;
                    if (Conn.State != ConnectionState.Open) Conn.Open();
                    using (DbDataReader dr = cmd.ExecuteReader())
                    {
                        result = todo(dr);
                    }
                }
                if (!TransactionIsOpened) Conn.Close();

                return result;
            }
            catch
            {
                if (Conn.State != ConnectionState.Closed)
                {
                    try { _transaction.Rollback(); _transactionIsOpened = false; }
                    catch { }
                    try { Conn.Close(); _transactionIsOpened = false; }
                    catch { }
                }
                throw;
            }
        }
        
        private bool defaultMoveNext(DbDataReader dr) { return dr.Read(); }
        /// <summary>
        /// Return IEnumerable with results
        /// </summary>
        /// <typeparam name="T">Generic resulting type</typeparam>
        /// <param name="command">SQL command</param>
        /// <param name="todo">delegate to recive T from DataReader</param>
        /// <param name="vals">values of parameters (if necessary)</param>
        /// <param name="timeout">timeout</param>
        /// <param name="moveNextModifier">rules for obtaining next row</param>
        /// <returns>consequentially readed data</returns>
        virtual public IEnumerable<T> Select<T>(String command, Func<DbDataReader, T> todo, DbParametersConstructor vals = null, int? timeout = null, CommandType commandType = CommandType.Text, Func<DbDataReader, bool> moveNextModifier = null)
        {
            vals = vals ?? DbParametersConstructor.emptyParams;
            ASqlWorker<TPC>.SqlParameterNullWorkaround(vals);
            using (var cmd = Conn.CreateCommand())
            {
                if (timeout.HasValue) cmd.CommandTimeout = timeout.Value;
                cmd.CommandText = command;
                cmd.CommandType = commandType;
                cmd.Parameters.AddRange(vals);
                cmd.Transaction = this._transaction;
                if (this.Conn.State != ConnectionState.Open) this.Conn.Open();
                moveNextModifier = moveNextModifier ?? defaultMoveNext;
                using (var dr = cmd.ExecuteReader())
                {
                    while (moveNextModifier(dr))
                    {
                        yield return todo(dr);
                    }
                }
            }
        }

        /// <summary>
        /// Obtain objects from DataReader using reflection
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="command"></param>
        /// <param name="vals"></param>
        /// <param name="exceptions"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        virtual public IEnumerable<T> SelectWithReflection<T>(String command, DbParametersConstructor vals = null, List<String> exceptions = null, int? timeout = null, CommandType commandType = CommandType.Text) where T : new()
        {
            if (exceptions != null) return Select(command, dr => DataReaderToObj<T>(dr, exceptions), vals, timeout, commandType);
            else return Select(command, dr => DataReaderToObj<T>(dr), vals, timeout);
        }

        /// <summary>
        /// Select values from single column
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table"></param>
        /// <param name="column"></param>
        /// <param name="vals"></param>
        /// <param name="whereCondition"></param>
        /// <param name="includingNulls"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        virtual public IEnumerable<T> SelectScalar<T>(String table, String column, DbParametersConstructor vals = null, String whereCondition = null, bool includingNulls = false, int? timeout = null, CommandType commandType = CommandType.Text)
        {
            vals = vals ?? DbParametersConstructor.emptyParams;

            if (String.IsNullOrWhiteSpace(whereCondition))
                whereCondition = vals.Count() == 0 ? ""
                    : vals.parameters.Aggregate<DbParameter, String, String>(
                        " WHERE "
                        , (value, i) => value + i.ParameterName + " = @" + i.ParameterName + "    AND\n\t" //aggregate constructions "<paramname> = @<paramname>    AND\n\t"
                        , (value) => value.Substring(0, value.Length - 6)           // then cut last " AND\n\t"
                    );

            return SelectScalar<T>(String.Format("SELECT {0} FROM {1} {2}", column, table, whereCondition), vals, includingNulls, timeout, commandType);
        }

        virtual public IEnumerable<T> SelectScalar<T>(String query, DbParametersConstructor vals = null, bool includingNulls = false, int? timeout = null, CommandType commandType = CommandType.Text)
        {
            bool includingNulls_checked = includingNulls && IsNullableParams(typeof(T));

            if (includingNulls_checked)
                return Select(query, dr => dr[0] == DBNull.Value ? (T)(Object)null : (T)dr[0], vals, timeout, commandType);
            else
                return Select(query, dr => (T)dr[0], vals, timeout, commandType, dr =>
                {
                    do
                    {
                        if (!dr.Read()) return false;
                    }
                    while (dr[0] == DBNull.Value);
                    return true;
                });
        }

        virtual public IEnumerable<Tuple<TX, TY>> SelectTuple<TX, TY>(String query, DbParametersConstructor vals = null, int? timeout = null, CommandType commandType = CommandType.Text)
        {
            bool[] IncludingNulls = new bool[] { IsNullableParams(typeof(TX)), IsNullableParams(typeof(TY)) };
            return Select(query,
                dr => new Tuple<TX, TY>((TX)(dr[0] == DBNull.Value ? null : dr[0]), (TY)(dr[1] == DBNull.Value ? null : dr[1])),
                vals, timeout, commandType,
                dr =>
                {
                    do
                    {
                        if (!dr.Read()) return false;
                    }
                    while ((!IncludingNulls[0] && dr[0] == DBNull.Value)
                         ||
                         (!IncludingNulls[1] && dr[1] == DBNull.Value));
                    return true;
                });
        }

        /// <summary>
        /// Converts DataRow to T with reflection, writing exceptions in list
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dr"></param>
        /// <param name="errors"></param>
        /// <returns></returns>
        virtual public T DataReaderToObj<T>(DbDataReader dr, List<String> errors) where T : new()
        {
            T result = new T();
            foreach (System.ComponentModel.PropertyDescriptor i in System.ComponentModel.TypeDescriptor.GetProperties(result))
            {
                try { if (dr[i.Name] != DBNull.Value) i.SetValue(result, dr[i.Name]); }
                catch (Exception e) { errors.Add(e.ToString()); }
            }

            return result;
        }

        /// <summary>
        /// Converts DataRow to T with reflection, throws!
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dr"></param>
        /// <returns></returns>
        virtual public T DataReaderToObj<T>(DbDataReader dr) where T : new()
        {
            T result = new T();
            foreach (System.ComponentModel.PropertyDescriptor i in System.ComponentModel.TypeDescriptor.GetProperties(result))
            {
                if (dr[i.Name] != DBNull.Value) i.SetValue(result, dr[i.Name]);
            }

            return result;
        }

        virtual public DataTable GetDataTable(String query, DbParametersConstructor vals = null, int? timeout = null, CommandType commandType = CommandType.Text)
        {
            return ManualProcessing(query, (dr) =>
            {
                var dt = new DataTable();
                dt.Load(dr);
                return dt;
            },
            vals, timeout, commandType);
        }

        #region Члены IDisposable

        public virtual void Dispose(bool commit)
        {
            if (!commit && TransactionIsOpened) TransactionRollback();
            if (Conn.State != ConnectionState.Closed) Conn.Close();
            Conn.Dispose();
            GC.SuppressFinalize(this);
        }

        public virtual void Dispose()
        {
            Dispose(false);
        }

        #endregion Члены IDisposable
    }
}