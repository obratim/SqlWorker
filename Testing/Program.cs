using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SqlWorker;
using System.Data.Common;

namespace Testing {
    class Program {
        static void Main(string[] args) {
            ISqlWorker worker = (ISqlWorker)new NpgSqlWorker("Server=devel;Port=5432;Database=bibliography;User Id=postgres;Password=mamayanekurulapshu;");
            worker.OpenConnection();

            List<String> objs = worker.GetListFromDBSingleProcessing("select * from wos.\"Record\"", null, delegate(DbDataReader dr) {
                String tableName = dr.GetString(0);
                return tableName;
            });

            foreach(String a in objs){
                Console.WriteLine(a);
            }
            
        }
    }
}
