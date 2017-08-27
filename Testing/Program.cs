// ***********************************************************************
// Copyright (c) 2015 Charlie Poole
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
// 
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
// ***********************************************************************

using NUnit.Framework;
using NUnitLite;
using SqlWorker;
using System.Linq;
using System.Data.SqlClient;
using System.Collections.Generic;
using System;

namespace NUnitLite.Tests
{
    public class Program
    {
        private static string _dbName = "db";
        private static string _connectionString = $@"Data Source=(LocalDB)\MSSQLLocalDB;AttachDbFilename=""{System.IO.Path.GetFullPath(_dbName)}.mdf"";Integrated Security=True;Connect Timeout=30";

		private static HashSet<int> _primes = new HashSet<int>(Enumerable.Range(1, 1000).Where(i => !Enumerable.Range(2, (int)Math.Sqrt(i)).Any(j => i % j == 0)));

        /// <summary>
        /// The main program executes the tests. Output may be routed to
        /// various locations, depending on the arguments passed.
        /// </summary>
        /// <remarks>Run with --help for a full list of arguments supported</remarks>
        /// <param name="args"></param>
        public static int Main(string[] args)
        {
            return new AutoRun().Execute(args);
        }

        [Test, Order(0)]
        public static void CanInicialise()
        {
            using (var sw = new MsSqlWorker(_connectionString))
            {
            }
        }

        [Test, Order(1)]
        public static void CanExec()
        {
            System.IO.File.Delete($"{_dbName}.mdf");
            System.IO.File.Delete($"{_dbName}_log.ldf");
            using (var sw = new MsSqlWorker(@"Data Source=.\sqlexpress;Initial Catalog=tempdb; Integrated Security=true;User Instance=True;"))
            {
                Assert.AreEqual(-1, sw.Exec($"CREATE DATABASE {_dbName} ON PRIMARY (NAME='{_dbName}', FILENAME='{System.IO.Path.GetFullPath(_dbName)}.mdf')"));
                Assert.AreEqual(-1, sw.Exec($"EXEC sp_detach_db '{_dbName}', 'true'"));
            }
        }
        
        [Test, Order(2)]
        public static void CanQueryConst()
        {
            using (var sw = new MsSqlWorker(_connectionString))
            {
                Assert.AreEqual("hello", sw.Query("select 'hello'", dr => dr[0]).Single());
            }
        }

        [Test, Order(3)]
        public static void CanCreateTableFromDataTable()
        {
			using (var sw = new MsSqlWorker(_connectionString))
			using (var dt = new System.Data.DataTable("numbers"))
			{
				dt.Columns.Add("number", typeof(int));
				dt.Columns.Add("square", typeof(long));
				dt.Columns.Add("sqrt", typeof(double));
				dt.Columns.Add("is_prime", typeof(bool));
				dt.Columns.Add(new System.Data.DataColumn("as_text", typeof(string)) { MaxLength = 400 });

				sw.CreateTableByDataTable(dt);
			}
        }

		[Test, Order(4)]
		public static void CanExecWithParametersArray()
		{
			using (var sw = new MsSqlWorker(_connectionString))
			{
				var insertsCount = sw.Exec(
					command: @"insert into numbers values (@number, @square, @sqrt, @is_prime, @as_text)",
					vals: new []{
							new SqlParameter("number", 1),
							new SqlParameter("square", 1L),
							new SqlParameter("sqrt", 1.0),
							new SqlParameter("is_prime", true),
							new SqlParameter("as_text", "one"),
						});
				Assert.AreEqual(1, insertsCount);
				
				var inserted = sw.Query(
					command: @"select * from numbers where number = 1",
					jobToDo: dr => (
						number: (int)dr[0],
						square: (long)dr[1],
						sqrt: (double)dr[2],
						is_prime: (bool)dr[3],
						as_text: dr.GetNullableString(4)
						))
						.Single();
				Assert.AreEqual(inserted, (1, 1L, 1.0, true, "one"));
			}
		}

		[Test, Order(4)]
		public static void CanExecWithParametersDictionary()
		{
			using (var sw = new MsSqlWorker(_connectionString))
			{
				var insertsCount = sw.Exec(
					command: @"insert into numbers values (@number, @square, @sqrt, @is_prime, @as_text)",
					vals: new Dictionary<string, object> {
						{ "number", 2 },
						{ "square", 4L },
						{ "sqrt", Math.Sqrt(2) },
						{ "is_prime", true },
						{ "as_text", "two" },
						});
				Assert.AreEqual(1, insertsCount);
				
				var inserted = sw.Query(
					command: @"select * from numbers where number = 2",
					jobToDo: dr => (
						number: (int)dr[0],
						square: (long)dr[1],
						sqrt: (double)dr[2],
						is_prime: (bool)dr[3],
						as_text: dr.GetNullableString(4)
						))
						.Single();
				Assert.AreEqual(inserted, (2, 4L, 1.4142135623730951, true, "two"));
			}
		}

		[Test, Order(4)]
		public static void CanExecWithSWParameters()
		{
			using (var sw = new MsSqlWorker(_connectionString))
			{
				var insertsCount = sw.Exec(
					command: @"insert into numbers values (@number, @square, @sqrt, @is_prime, @as_text)",
					vals: new SWParameters {
						{ "number", 3 },
						{ "square", 9, System.Data.DbType.Int64 },
						{ "sqrt", Math.Sqrt(3) },
						{ "is_prime", true },
						{ "as_text", "three" },
						});
				Assert.AreEqual(1, insertsCount);
				
				var inserted = sw.Query(
					command: @"select * from numbers where number = 3",
					jobToDo: dr => (
						number: (int)dr[0],
						square: (long)dr[1],
						sqrt: (double)dr[2],
						is_prime: (bool)dr[3],
						as_text: dr.GetNullableString(4)
						))
						.Single();
				Assert.AreEqual(inserted, (3, 9L, 1.7320508075688773, true, "three"));
			}
		}

		[Test, Order(4)]
		public static void CanCommitTransaction()
		{
			using (var sw = new MsSqlWorker(_connectionString))
			using (var tran = sw.TransactionBegin())
			{
				var insertsCount = sw.InsertValues(
					tableName: "numbers",
					vals: new SWParameters
					{
						{ "number", 4 },
						{ "square", 16L },
						{ "sqrt", 2.0 },
						{ "is_prime", false },
						{ "as_text", "fore" },
					},
					transaction: tran);
				Assert.AreEqual(1, insertsCount);
				tran.Commit();
			}
			using(var sw = new MsSqlWorker(_connectionString))
			{
				Assert.AreEqual(
					expected: (4, 16L, 2.0, false, "fore"),
					actual: sw.Query("select * from numbers where number = 4", dr => ((int)dr[0], (long)dr[1], (double)dr[2], (bool)dr[3], (string)dr[4])).Single());
			}
		}

		[Test, Order(4)]
		public static void CanRollBackTransaction()
		{
			using (var sw = new MsSqlWorker(_connectionString))
			using (var tran = sw.TransactionBegin())
			{
				var insertsCount = sw.InsertValues(
					tableName: "numbers",
					vals: new SWParameters
					{
						{ "number", 100500 },
						{ "square", -1 },
						{ "sqrt", 0.0 },
						{ "is_prime", false },
						{ "as_text", (string)null },
					},
					transaction: tran);
				Assert.AreEqual(1, insertsCount);
				tran.Rollback();
			}
			using (var sw = new MsSqlWorker(_connectionString))
			{
				Assert.AreEqual(
					expected: 0,
					actual: sw.Query("select COUNT(*) from numbers where number = 100500", dr => (int)dr[0]).Single());
			}
		}

		[Test, Order(4)]
		public static void TransactionRolledBackIfNotCommitted()
		{
			using (var sw = new MsSqlWorker(_connectionString))
			using (var tran = sw.TransactionBegin())
			{
				var insertsCount = sw.InsertValues(
					tableName: "numbers",
					vals: new SWParameters
					{
						{ "number", 100500 },
						{ "square", -1 },
						{ "sqrt", 0.0 },
						{ "is_prime", false },
						{ "as_text", (string)null },
					},
					transaction: tran);
				Assert.AreEqual(1, insertsCount);
			}
			using (var sw = new MsSqlWorker(_connectionString))
			{
				Assert.AreEqual(
					expected: 0,
					actual: sw.Query("select COUNT(*) from numbers where number = 100500", dr => (int)dr[0]).Single());
			}
		}

		[Test, Order(4)]
		public static void CanBulkInsert()
		{
			using (var sw = new MsSqlWorker(_connectionString))
			using (var tran = sw.SqlTransactionBegin())
			{
				var rangeToInsert = Enumerable
						.Range(5, 10)
						.Select(i => new { number = i, square = (long)i * i, sqrt = Math.Sqrt(i), is_prime = _primes.Contains(i), as_text = (string)null });

				sw.BulkCopyWithReflection(
					source: rangeToInsert,
					targetTableName: "numbers",
					transaction: tran);
				tran.Commit();

				CollectionAssert.AreEquivalent(
					expected: rangeToInsert,
					actual: sw.Query(
						"select * from numbers where number >= @min_number",
						dr => new { number = (int)dr[0], square = (long)dr[1], sqrt = (double)dr[2], is_prime = (bool)dr[3], as_text = dr.GetNullableString(4) },
						vals: new SqlParameter("min_number", 5)));
			}
		}

		[Test, Order(5)]
		public static void CanUpdate()
		{
			using (var sw = new MsSqlWorker(_connectionString))
			{
				var updatesCount = sw.UpdateValues(
					tableName: "numbers",
					values: new SWParameters { { "as_text", "five" } },
					condition: new SWParameters { { "number", 5 } });
				Assert.AreEqual(1, updatesCount);
				Assert.AreEqual(
					expected: (5, "five"),
					actual: sw.Query("select number, as_text from numbers where number = 5", dr => ((int)dr[0], (string)dr[1])).Single());
			}
		}

		[Test, Order(5)]
		public static void CanUpdateWithStringCondition()
		{
			using (var sw = new MsSqlWorker(_connectionString))
			{
				var updatesCount = sw.UpdateValues(
					tableName: "numbers",
					values: new SWParameters { { "as_text", "six" } },
					condition: "number = 6");
				Assert.AreEqual(1, updatesCount);
				Assert.AreEqual(
					expected: (6, "six"),
					actual: sw.Query("select number, as_text from numbers where number = 6", dr => ((int)dr[0], (string)dr[1])).Single());
			}
		}
    }
}