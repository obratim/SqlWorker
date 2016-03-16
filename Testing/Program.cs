using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using SqlWorker;

namespace Testing {

    internal class Program {

        private static void Main(string[] args) {
            /*             var connection = new OleDbConnection("Provider=VFPOLEDB.1;Data Source=D:\\temp\\arhob\\;Codepage=1251");              var cmd = connection.CreateCommand();             cmd.CommandText = "SELECT * FROM tmpk.dbf";             connection.Open();              var sw = new OledbSqlWorker(@"Provider=vfpoledb;Data Source=D:\docs\Hostel\extern_db;Codepage=1251");             var datax = sw.GetDataTable("Select * from tmpk.dbf");             */

            var SW = new SqlWorker.MSSqlWorker("G202-comp1", "EDU");

            var data = SW.Select("select top 100 wpid from umkd.workprogram where wpgosn = @wpgosn and gosplus = @gosplus",
                                 dr => new { id = dr.GetGuid(0) },
                                    new SWParameters() { { "wpgosn", 3 }, { "gosplus", 1, DbType.Int32 } });

            //Console.WriteLine (data.Aggregate ("", (str, i) => string.Format ("{0}{1}\n", str, i)));
            foreach (var i in data)
                Console.WriteLine(i);

            return;

            ASqlWorker<ParameterConstructor_NPG> worker = (ASqlWorker<ParameterConstructor_NPG>)new NpgSqlWorker("Server=devel;Port=5432;Database=bibliography;User Id=postgres;Password=mamayanekurulapshu;");
            worker.ReOpenConnection();

            //List<String> objs = worker.GetListFromDBSingleProcessing("select * from wos.\"Record\"", null, delegate(DbDataReader dr) {
            //    String tableName = dr.GetString(0);
            //    return tableName;
            //});

            worker.InsertValues("wos.\"Record\"", new Dictionary<String, Object>{                 {"ImportDate", DateTime.Now},                 {"ImportUrl", "http://ru.wikipedia.org/wiki/%D0%A1%D1%83%D0%BF%D0%B5%D1%80%D0%BC%D0%B5%D0%BD"},                 {"ID", Guid.NewGuid()}             });

            worker.InsertValues("wos.\"Record\"", new Npgsql.NpgsqlParameter[3]{                 //new Npgsql.NpgsqlParameter("Title", "путин-краб"),                 new Npgsql.NpgsqlParameter("ImportDate", DateTime.Now),                 new Npgsql.NpgsqlParameter("ImportUrl", "http://ru.wikipedia.org/wiki/%D0%A1%D1%83%D0%BF%D0%B5%D1%80%D0%BC%D0%B5%D0%BD"),                 new Npgsql.NpgsqlParameter("ID", Guid.NewGuid())             });
        }
    }
}