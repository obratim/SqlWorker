using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SqlWorker;
using System.Data.Common;
using System.Data.SqlClient;

namespace Testing {
    class Program {
        static void Main(string[] args) {
            ASqlWorker worker = (ASqlWorker)new NpgSqlWorker("Server=devel;Port=5432;Database=bibliography;User Id=postgres;Password=mamayanekurulapshu;");
            worker.OpenConnection();

            //List<String> objs = worker.GetListFromDBSingleProcessing("select * from wos.\"Record\"", null, delegate(DbDataReader dr) {
            //    String tableName = dr.GetString(0);
            //    return tableName;
            //});
            

            worker.InsertValues("wos.\"Record\"", new Npgsql.NpgsqlParameter[3]{ 
                //new Npgsql.NpgsqlParameter("Title", "путин-краб"),
                new Npgsql.NpgsqlParameter("ImportDate", DateTime.Now),
                new Npgsql.NpgsqlParameter("ImportUrl", "http://ru.wikipedia.org/wiki/%D0%A1%D1%83%D0%BF%D0%B5%D1%80%D0%BC%D0%B5%D0%BD"),
                new Npgsql.NpgsqlParameter("ID", Guid.NewGuid())
            });
            
        }
    }
}
