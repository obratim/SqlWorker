using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using SqlWorker;

namespace Testing
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            using (var SW = new SqlWorker.MSSqlWorker("<server>", "<database>", "<user>", "<password>")) // connecting to sql server
            {
                using (var tran = SW.TransactionBegin())
                {
                    var n = SW.InsertValues("<the table>", new SWParameters { { "email", "ivan@example.com" }, { "msg", "Hello, world!" } }); // insert example
                    Console.WriteLine(n);
                    var jsonSerializer = new Newtonsoft.Json.JsonSerializer();
                    using (var writer = new System.IO.StreamWriter(Console.OpenStandardOutput()))
                    {
                        jsonSerializer.Serialize(writer,
                            SW.Select("select * from <the table>", delegate (System.Data.Common.DbDataReader dr) { return new[] { dr[0], dr[1], }; }) // select example
                        );
                    }
                    tran.Commit();
                }
            }
        }
    }
}