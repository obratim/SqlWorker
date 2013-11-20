using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Data.Common;
using Npgsql;

namespace SqlWorker {
    public class NpgSqlWorker : ISqlWorker {
        private String _connectionStr;
        private NpgsqlTransaction _tran;

        public NpgSqlWorker(String ConnectionString) { _connectionStr = ConnectionString; }

        private NpgsqlConnection _conn;
        protected NpgsqlConnection Conn {
            get {
                if (_conn == null) _conn = new NpgsqlConnection(_connectionStr);
                return _conn;
            }
        }

        public static DbParameter[] NotNullParams(DbParameter[] param) {
            return (from DbParameter p in param
                    where p.Value != null
                    select p).ToArray();
        }

        protected String QueryWithParams(String Query, DbParameter[] Params) {
            if (Params == null) return Query;

            String newq = Query;
            bool firstParam = true;

            if (newq.IndexOf('@') != -1) firstParam = false;
            foreach (var p in Params) {
                if (newq.IndexOf("@" + p.ParameterName) == -1) newq += (firstParam ? " @" : ", @") + p.ParameterName;
                firstParam = false;
            }
            return newq;
        }

        public bool OpenConnection() {
            if (Conn.State == ConnectionState.Open && _tran != null) return true;
            Conn.Close();
            Conn.Open();
            return Conn.State == ConnectionState.Open;
        }

        #region Transactions
        public void TransactionBegin() {
            if (_tran != null) {
                throw new Exception("transaction exists!");
                //_tran.Rollback();
                //_tran.Dispose();
            }
            if (Conn.State != ConnectionState.Open) Conn.Open();
            _tran = Conn.BeginTransaction();
        }

        public void TransactionCommit() {
            if (_tran == null) throw new Exception("transaction dont exists!");
            _tran.Commit();
            _tran.Dispose();
            _tran = null;
        }

        public void TransactionRollback() {
            if (_tran == null) throw new Exception("transaction dont exists!");
            _tran.Rollback();
            _tran.Dispose();
            _tran = null;
        }

        public NpgsqlTransaction TransactionState { get { return _tran; } }
        #endregion

        public int ExecuteNonQuery(String Command) { return ExecuteNonQuery(Command, new DbParameter[0]); }
        public int ExecuteNonQuery(String Command, DbParameter[] param) {
            NpgsqlCommand cmd = Conn.CreateCommand();
            cmd.CommandText = QueryWithParams(Command, param);
            cmd.Parameters.AddRange(param);
            cmd.Transaction = _tran;
            if (Conn.State != ConnectionState.Open) Conn.Open();
            int result = cmd.ExecuteNonQuery();
            cmd.Dispose();
            if (_tran == null) Conn.Close();
            return result;
        }

        public int InsertValues(String TableName, DbParameter[] param) {
            DbParameter[] _param = NotNullParams(param);

            String q = "INSERT INTO " + TableName + " (" + _param[0].ParameterName;

            for (int i = 1; i < _param.Count(); ++i)
                q += ", " + _param[i].ParameterName;

            q += ") VALUES (@" + _param[0].ParameterName;

            for (int i = 1; i < _param.Count(); ++i)
                q += ", @" + _param[i].ParameterName;

            q += ")";

            return ExecuteNonQuery(q, _param);
        }

        public int UpdateValues(String TableName, DbParameter[] Values, DbParameter Condition) { return UpdateValues(TableName, Values, new DbParameter[1] { Condition }); }
        public int UpdateValues(String TableName, DbParameter[] Values, DbParameter[] Condition) {
            DbParameter[] _param = NotNullParams(Values);

            String q = "UPDATE " + TableName + " SET " + _param[0].ParameterName + " = @" + _param[0].ParameterName;

            for (int i = 1; i < _param.Count(); ++i)
                q += ", " + _param[i].ParameterName + " = @" + _param[i].ParameterName;

            if (Condition.Count() > 0)
                q += " WHERE " + Condition[0].ParameterName + " = @" + Condition[0].ParameterName;

            for (int i = 1; i < Condition.Count(); ++i)
                q += " AND " + Condition[i].ParameterName + " = @" + Condition[i].ParameterName;

            List<DbParameter> param = new List<DbParameter>(_param);
            param.AddRange(Condition);
            return ExecuteNonQuery(q, param.ToArray());
        }
        public int UpdateValues(String TableName, DbParameter[] Values, String Condition) {
            DbParameter[] _param = NotNullParams(Values);

            String q = "UPDATE " + TableName + " SET " + _param[0].ParameterName + " = @" + _param[0].ParameterName;

            for (int i = 1; i < _param.Count(); ++i)
                q += ", " + _param[i].ParameterName + " = @" + _param[i].ParameterName;

            if (!String.IsNullOrWhiteSpace(Condition))
                q += " WHERE " + Condition;

            return ExecuteNonQuery(q, _param);
        }

        public T GetStructFromDB<T>(String Command, GetterDelegate<T> todo) { return GetStructFromDB<T>(Command, new DbParameter[0], todo); }
        public T GetStructFromDB<T>(String Command, DbParameter[] param, GetterDelegate<T> todo) {
            NpgsqlCommand cmd = Conn.CreateCommand();
            //cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandText = QueryWithParams(Command, param);
            cmd.Parameters.AddRange(param);
            cmd.Transaction = _tran;
            if (Conn.State != ConnectionState.Open) Conn.Open();
            DbDataReader dr = cmd.ExecuteReader();
            T result = todo(dr);
            dr.Close();
            dr.Dispose();
            cmd.Dispose();
            if (_tran == null) Conn.Close();

            return result;
        }

        public List<T> GetListFromDB<T>(String procname, DbParameter[] param) where T : new() {
            return GetStructFromDB<List<T>>(procname, param, delegate(DbDataReader dr) {
                List<T> result = new List<T>();
                while (dr.Read()) {
                    result.Add(DataReaderToObj<T>(dr));
                }
                return result;
            });
        }

        public List<T> GetListFromDB<T>(String procname, DbParameter[] param, List<String> Exceptions) where T : new() {
            return GetStructFromDB<List<T>>(procname, param, delegate(DbDataReader dr) {
                List<T> result = new List<T>();
                while (dr.Read()) {
                    result.Add(DataReaderToObj<T>(dr, Exceptions));
                }
                return result;
            });
        }

        public T DataReaderToObj<T>(DbDataReader dr, List<String> Errors) where T : new() {
            T result = new T();
            foreach (System.ComponentModel.PropertyDescriptor i in System.ComponentModel.TypeDescriptor.GetProperties(result)) {
                try { i.SetValue(result, dr[i.Name]); }
                catch (Exception e) { Errors.Add(e.ToString()); }
            }

            return result;
        }

        public T DataReaderToObj<T>(DbDataReader dr) where T : new() {
            T result = new T();
            foreach (System.ComponentModel.PropertyDescriptor i in System.ComponentModel.TypeDescriptor.GetProperties(result)) {
                /*try {*/
                i.SetValue(result, dr[i.Name]); /*}*/
                /*catch (Exception) { }*/
            }

            return result;
        }

        public List<T> GetListFromDB<T>(String Command, DbParameter[] param, GetterDelegate<T> todo) {
            // todo
            return null;
        }

        public List<T> GetListFromDBSingleProcessing<T>(string Command, DbParameter[] param, GetterDelegate<T> todo) {
            return GetStructFromDB<List<T>>(Command, param, delegate(DbDataReader dr) {
                List<T> output = new List<T>();
                while (dr.Read()) {
                    output.Add(todo(dr));
                }
                return output;
            });
        }
    }
}
