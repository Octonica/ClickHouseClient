
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Octonica.ClickHouseClient.Benchmark
{
    [MarkdownExporter, RPlotExporter]
    
    [SimpleJob(RunStrategy.ColdStart, 
        launchCount: 1,
        warmupCount: 2, 
        targetCount: 20,
        id: "InsertAndRead")]
    public class InsertBenchmark: ClickHouseBaseConnection
	{

        private readonly string TableName = "insert_benchmark";
        private ClickHouseConnection connection;

        public InsertBenchmark()
        {
            connection = OpenConnection();
        }

        [Params(100, 10000, 100000)]
        public int valuesCount { get; set; }

        private IEnumerable<int> idEnumerable { get; set; }
        private IEnumerable<string> strEnumerable { get; set; }

        [GlobalSetup]
        public void GlobalSetup()
        {

            GlobalCleanup();
            var cmd = connection.CreateCommand($"CREATE TABLE {TableName}(id Int32, str Nullable(String)) ENGINE=Memory");
            cmd.ExecuteNonQuery();

            idEnumerable = Enumerable.Range(0, valuesCount);
            strEnumerable = idEnumerable.Select( x => x.ToString());
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            var cmd = connection.CreateCommand($"DROP TABLE IF EXISTS {TableName}");
            cmd.ExecuteNonQuery();
        }


        [Benchmark]
        public void InsertValues() {

            using (var writer = connection.CreateColumnWriter($"INSERT INTO {TableName}(id, str) VALUES"))
            {
                var writeObject = new object[] { idEnumerable, strEnumerable };
                writer.WriteTable(writeObject, valuesCount);
                writer.EndWrite();
            }
        }

        [Benchmark]
        public void ReadValues()
        {
            var cmd = connection.CreateCommand($"SELECT id, str FROM {TableName}");
            int count = 0;

            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    var id = reader.GetInt32(0);
                    var str = reader.GetString(1, null);

                    ++count;
                }
            }
        }
    }
}
