using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using Microsoft.Extensions.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlWorker;

namespace Tests.SqlWorker.MsSql
{
    [TestClass]
    public class TestMsSqlWorker
    {
        private readonly IConfigurationRoot Config;
        private string ConnectionString => Config["connectionString"];
        private string ConnectionStringMaster => Config["connectionStringMaster"];

		private static HashSet<int> _primes = new HashSet<int>(Enumerable.Range(1, 1000).Where(i => !Enumerable.Range(2, (int)Math.Sqrt(i)).Any(j => i % j == 0)));

        public TestMsSqlWorker()
        {
            Config = new ConfigurationBuilder().AddJsonFile("config.json").Build();
        }

        [TestInitialize]
        public void TestConfig()
        {
            Assert.IsNotNull(ConnectionStringMaster);
            Assert.IsNotNull(ConnectionString);
            using (var sw = new MsSqlWorker(ConnectionStringMaster))
            {
                Assert.AreEqual("hello", sw.Query("select 'hello'", dr => dr[0]).Single());
            }

            if (Config["recreateDb"]?.ToLower() == "true")
                using (var sw = new MsSqlWorker(ConnectionStringMaster))
                {
                    sw.Exec(@"
                        IF DB_ID('sqlworker_test') IS NOT NULL
                            ALTER DATABASE sqlworker_test SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                        DROP DATABASE IF EXISTS sqlworker_test;
                        CREATE DATABASE sqlworker_test;
                        ALTER DATABASE sqlworker_test SET RECOVERY SIMPLE;"
                    );
            }
            
            using (var sw = new MsSqlWorker(ConnectionString))
            {
                Assert.AreEqual("hello", sw.Query("select 'hello'", dr => dr[0]).Single());
            }

            using (var sw = new MsSqlWorker(ConnectionString))
            using (var dt = new System.Data.DataTable("numbers"))
            {
                dt.Columns.Add("number", typeof(int));
                dt.Columns.Add("square", typeof(long));
                dt.Columns.Add("sqrt", typeof(double));
                dt.Columns.Add("is_prime", typeof(bool));
                dt.Columns.Add(new System.Data.DataColumn("as_text", typeof(string)) { MaxLength = 400 });

                sw.CreateTableByDataTable(dt, true);

                sw.Exec(@"
                    CREATE UNIQUE CLUSTERED INDEX [PK_number] ON [dbo].[numbers]([number] ASC)
                    WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
                ");
                sw.Exec(@"
                    CREATE UNIQUE NONCLUSTERED INDEX [IX_square] ON [dbo].[numbers]([square] ASC)
                    WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
                ");
                sw.Exec(@"
                    CREATE UNIQUE NONCLUSTERED INDEX [IX_sqrt] ON [dbo].[numbers]([sqrt] ASC)
                    WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
                ");
            }
        }

        [TestMethod]
        public void CanExecWithParametersArray()
        {
            using (var sw = new MsSqlWorker(ConnectionString))
            {
                var insertsCount = sw.Exec(
                    command: @"insert into numbers values (@number, @square, @sqrt, @is_prime, @as_text)",
                    parameters: new []
                    {
                        new SqlParameter("number", 1),
                        new SqlParameter("square", 1L),
                        new SqlParameter("sqrt", 1.0),
                        new SqlParameter("is_prime", true),
                        new SqlParameter("as_text", "one"),
                    });
                Assert.AreEqual(1, insertsCount);
                
                var inserted = sw.Query(
                    command: @"select * from numbers where number = 1",
                    transformFunction: dr => (
                        number: (int)dr[0],
                        square: (long)dr[1],
                        sqrt: (double)dr[2],
                        is_prime: (bool)dr[3],
                        as_text: dr.GetNullableString(4)
                    ))
                    .Single();
                Assert.AreEqual((1, 1L, 1.0, true, "one"), inserted);
            }
        }

        [TestMethod]
        public void CanExecWithParametersDictionary()
        {
            using (var sw = new MsSqlWorker(ConnectionString))
            {
                var insertsCount = sw.Exec(
                    command: @"insert into numbers values (@number, @square, @sqrt, @is_prime, @as_text)",
                    parameters: new Dictionary<string, object> {
                        { "number", 2 },
                        { "square", 4L },
                        { "sqrt", Math.Sqrt(2) },
                        { "is_prime", true },
                        { "as_text", "two" },
                    });
                Assert.AreEqual(1, insertsCount);
                
                var inserted = sw.Query(
                    command: @"select * from numbers where number = 2",
                    transformFunction: dr => (
                        number: (int)dr[0],
                        square: (long)dr[1],
                        sqrt: (double)dr[2],
                        is_prime: (bool)dr[3],
                        as_text: dr.GetNullableString(4)
                        ))
                        .Single();
                Assert.AreEqual((2, 4L, 1.4142135623730951, true, "two"), inserted);
            }
        }

        [TestMethod]
        public void CanExecWithSwParameters()
        {
            using (var sw = new MsSqlWorker(ConnectionString))
            {
                var insertsCount = sw.Exec(
                    command: @"insert into numbers values (@number, @square, @sqrt, @is_prime, @as_text)",
                    parameters: new SwParameters {
                        { "number", 3 },
                        { "square", 9, System.Data.DbType.Int64 },
                        { "sqrt", Math.Sqrt(3), System.Data.DbType.Double, System.Data.ParameterDirection.Input },
                        { "is_prime", true },
                        { "as_text", "three" },
                        });
                Assert.AreEqual(1, insertsCount);
                
                var inserted = sw.Query(
                    command: @"select * from numbers where number = 3",
                    transformFunction: dr => (
                        number: (int)dr[0],
                        square: (long)dr[1],
                        sqrt: (double)dr[2],
                        is_prime: (bool)dr[3],
                        as_text: dr.GetNullableString(4)
                        ))
                        .Single();
                Assert.AreEqual((3, 9L, 1.7320508075688773, true, "three"), inserted);
            }
        }

        [TestMethod]
        public void CanCommitTransaction()
        {
            using (var sw = new MsSqlWorker(ConnectionString))
            using (var tran = sw.TransactionBegin())
            {
                var insertsCount = sw.Exec(
                    command: @"insert into numbers values (@number, @square, @sqrt, @is_prime, @as_text)",
                    parameters: new SwParameters
                    {
                        { "number", 4 },
                        { "square", 16L },
                        { "sqrt", 2.0 },
                        { "is_prime", false },
                        { "as_text", "four" },
                    },
                    transaction: tran);
                Assert.AreEqual(1, insertsCount);
                tran.Commit();
            }
            using(var sw = new MsSqlWorker(ConnectionString))
            {
                Assert.AreEqual(
                    expected: (4, 16L, 2.0, false, "four"),
                    actual: sw.Query("select * from numbers where number = 4", dr => ((int)dr[0], (long)dr[1], (double)dr[2], (bool)dr[3], (string)dr[4])).Single());
            }
        }

        [TestMethod]
        public void CanRollBackTransaction()
        {
            using (var sw = new MsSqlWorker(ConnectionString))
            using (var tran = sw.TransactionBegin())
            {
                var insertsCount = sw.Exec(
                    command: @"insert into numbers values (@number, @square, @sqrt, @is_prime, @as_text)",
                    parameters: new SwParameters
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
            using (var sw = new MsSqlWorker(ConnectionString))
            {
                Assert.AreEqual(
                    expected: 0,
                    actual: sw.Query("select COUNT(*) from numbers where number = 100500", dr => (int)dr[0]).Single());
            }
        }

        [TestMethod]
        public void TransactionRolledBackIfNotCommitted()
        {
            using (var sw = new MsSqlWorker(ConnectionString))
            using (var tran = sw.TransactionBegin())
            {
                var insertsCount = sw.Exec(
                    command: @"insert into numbers values (@number, @square, @sqrt, @is_prime, @as_text)",
                    parameters: new SwParameters
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
            using (var sw = new MsSqlWorker(ConnectionString))
            {
                Assert.AreEqual(
                    expected: 0,
                    actual: sw.Query("select COUNT(*) from numbers where number = 100500", dr => (int)dr[0]).Single());
            }
        }

        [TestMethod]
        public void CanBulkInsert()
        {
            using (var sw = new MsSqlWorker(ConnectionString))
            {
                void bulkInsertAndCheck(int start, int length, int chunkSize)
                {
                    using (var tran = sw.SqlTransactionBegin())
                    {
                        var rangeToInsert = Enumerable
                                .Range(start, length)
                                .Select(i => new { number = i, square = (long)i * i, sqrt = Math.Sqrt(i), is_prime = _primes.Contains(i), as_text = (string)null })
                                .ToArray();

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
                                parameters: new SwParameters { { "min_number", start } })
                                .ToArray());
                    }
                }

                bulkInsertAndCheck(5, 3, 1);
                bulkInsertAndCheck(8, 7, 2);
                bulkInsertAndCheck(15, 10, 3);
                bulkInsertAndCheck(25, 11, 3);
                bulkInsertAndCheck(36, 16, 5);
                bulkInsertAndCheck(52, 18, 5);
                bulkInsertAndCheck(70, 20, 5);
                bulkInsertAndCheck(90, 20, 7);
                bulkInsertAndCheck(110, 0, 11);
                bulkInsertAndCheck(110, 10, 11);
                bulkInsertAndCheck(120, 11, 11);
                bulkInsertAndCheck(131, 20, 13);
            }
        }
    }
}
