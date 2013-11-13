using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data.Common;

namespace SqlWorker {
    public delegate T GetterDelegate<T>(DbDataReader dr);

    public interface ISqlWorker {

        //public ISqlWorker(String connectionString);
        //String QueryWithParams(String Query, DbParameter[] Params);
        bool OpenConnection();
        int ExecuteNonQuery(String Command);
        int ExecuteNonQuery(String Command, DbParameter[] param);
        int InsertValues(String TableName, DbParameter[] param);
        int UpdateValues(String TableName, DbParameter[] Values, DbParameter Condition);
        int UpdateValues(String TableName, DbParameter[] Values, DbParameter[] Condition);
        int UpdateValues(String TableName, DbParameter[] Values, String Condition);
        T GetStructFromDB<T>(String Command, GetterDelegate<T> todo);
        T GetStructFromDB<T>(String Command, DbParameter[] param, GetterDelegate<T> todo);

        /// <summary>
        /// Делегат должен подготавливать один объект из DataReader'а, полностью его создавать и возвращать
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="Command"></param>
        /// <param name="param"></param>
        /// <param name="todo"></param>
        /// <returns></returns>
        List<T> GetListFromDBSingleProcessing<T>(String Command, DbParameter[] param, GetterDelegate<T> todo); 

        List<T> GetListFromDB<T>(String procname, DbParameter[] param) where T : new();
        List<T> GetListFromDB<T>(String procname, DbParameter[] param, List<String> Exceptions) where T : new();
        T DataReaderToObj<T>(DbDataReader dr, List<String> Errors) where T : new();
        T DataReaderToObj<T>(DbDataReader dr) where T : new();
    }
}
