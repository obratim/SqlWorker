using System;
using System.Collections.Generic;
using SqlWorker;
using System.Linq;

namespace Testing {
    class Program {
        static void Main (string[] args)
		{
			var SW = new SqlWorker.SqlWorker ("eis.mephi.ru,7099", "EDU");
			
			var data = SW.Select("select top 100 wpid from umkd.workprogram where wpgosn = @wpgosn and gosplus = @gosplus",
			                     dr => new { id = dr.GetGuid(0) },
									new SWParameters () { { "wpgosn", 3 }, { "gosplus", 1 } });
			
			//Console.WriteLine (data.Aggregate ("", (str, i) => string.Format ("{0}{1}\n", str, i)));
			foreach (var i in data)
				Console.WriteLine (i);
			
			return;
			
            ASqlWorker<NpgParameterConstructor> worker = (ASqlWorker<NpgParameterConstructor>)new NpgSqlWorker("Server=devel;Port=5432;Database=bibliography;User Id=postgres;Password=mamayanekurulapshu;");
            worker.OpenConnection();

            //List<String> objs = worker.GetListFromDBSingleProcessing("select * from wos.\"Record\"", null, delegate(DbDataReader dr) {
            //    String tableName = dr.GetString(0);
            //    return tableName;
            //});

            worker.InsertValues("wos.\"Record\"", new Dictionary<String, Object>{
                {"ImportDate", DateTime.Now},
                {"ImportUrl", "http://ru.wikipedia.org/wiki/%D0%A1%D1%83%D0%BF%D0%B5%D1%80%D0%BC%D0%B5%D0%BD"},
                {"ID", Guid.NewGuid()}
            });

            worker.InsertValues("wos.\"Record\"", new Npgsql.NpgsqlParameter[3]{ 
                //new Npgsql.NpgsqlParameter("Title", "путин-краб"),
                new Npgsql.NpgsqlParameter("ImportDate", DateTime.Now),
                new Npgsql.NpgsqlParameter("ImportUrl", "http://ru.wikipedia.org/wiki/%D0%A1%D1%83%D0%BF%D0%B5%D1%80%D0%BC%D0%B5%D0%BD"),
                new Npgsql.NpgsqlParameter("ID", Guid.NewGuid())
            });

        }
    }
}
