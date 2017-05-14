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
            using (var SW = new SqlWorker.MSSqlWorker("G202-comp1,14331", "HOSTEL")) // connecting to sql server
            {
                var data1 = SW.Select("select top 100 * from [log] l order by l.id desc", dr => new[] { dr[0], dr[1], dr[2], dr[3], dr[4], dr[5], dr[6], dr[7], dr[8] });
                var x1 = data1.Take(100).ToArray();
                var x2 = data1.Take(20).ToArray();
                var y = data1.First();
                foreach (var xx in data1)
                {
                    Console.WriteLine(String.Join("; ", xx.Select(i => i.ToString())));
                }
                return;

                SW.TransactionBegin();
                var n = SW.InsertValues("<the table>", new SWParameters { { "email", "ivan@example.com" }, { "msg", "Hello, world!" } }); // insert example
                Console.WriteLine(n);
                var jsonSerializer = new Newtonsoft.Json.JsonSerializer();
                using (var writer = new System.IO.StreamWriter(Console.OpenStandardOutput()))
                {
                    jsonSerializer.Serialize(writer,
                        SW.Select("select * from <the table>", delegate(System.Data.Common.DbDataReader dr) { return new[] { dr[0], dr[1], }; }) // select example
                    );
                }
                SW.TransactionCommit();

            }
        }
    }
}