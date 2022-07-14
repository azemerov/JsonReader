using System;
using System.Collections.Generic;
using System.Data;
using Oracle.ManagedDataAccess.Client; // to load table data
using Newtonsoft.Json.Linq;
using Vespa.Db;
using Vespa.Data;

namespace JsonTest
{
    class Program
    {

        protected bool printOut = false;
        protected OracleConnection connection = null;
        static JObject conf;
        static bool printMeta = false;

        static void IntrospectJson(JToken obj)
        {
            if (obj.Type == JTokenType.Object)
                foreach(JProperty v in obj)
                    IntrospectJson(v);
            else
            if (obj.Type == JTokenType.Array )
                foreach(JValue val in obj)
                    IntrospectJson(val);
            else
            if (obj.Type == JTokenType.Property)
               if (((JProperty)obj).Value.Type == JTokenType.Array)
               {
                    Console.WriteLine($"{((JProperty)obj).Name}=[");
                    IntrospectJson(((JProperty)obj).Value);
                    Console.WriteLine("]");
               }
                else
                    Console.WriteLine($"{((JProperty)obj).Name}={((JProperty)obj).Value}");
            else // simple types
            {
                Console.WriteLine($"{obj}");
            }

        }
        static void TestJObject()
        {
            var root = JObject.Parse("{'A':'a', 'B':'b', 'C': ['1', '2', '3']}");
            IntrospectJson(root);
        }

        protected int InMemoryTest()
        {
            string metaStr = 
@"{
        'ata_id': {'$collectionid': 'ATA_ID'},
        'chapter_name': 'ATA_DESCRIPTION',
        'ata_flags': {
            'has_oil_svc_yn': {'$bool': 'ATA_FLAG1'},
            'is_oil_svc_yn': {'$bool': 'ATA_FLAG2'}
        },
        'sub_atas': [{
            'id': {'$collectionid': 'SUB_ATA_ID'},
            'sub_chapter_name': 'SUB_DESCRIPTION'
        }],
        'Items': [{
            'id': {'$collectionid': 'COL2_ID'},
            'name': 'COL2_NAME'
        }]
}";
            IDataReader reader;
            JArray array = (JArray) conf["InMemoryData"];
            List<string> dataLines = array.ToObject<List<string>>(); 

            var meta = Meta.MakeMeta(metaStr, "myCollection");
            //var meta = Meta.MakeMeta((JObject)conf["InMemoryMeta"], "myCollection");
            if (printMeta)
                Console.WriteLine(meta.ToString(""));
            reader = new CSVDataReader(new ListReader(dataLines), ',', true);
            object root = meta.ConstructJson(reader, true, false);
            reader.Close();
            reader.Dispose();
            if (printOut)
                Console.WriteLine(root.ToString());
            return meta.RowCount;
        }

        protected int OracleTest(JObject metadata, string sql, bool useAutoColumns)
        {
            var meta = Meta.MakeMeta(metadata);
            if (printMeta)
                Console.WriteLine(meta.ToString(""));
            if (useAutoColumns)
            {
                var s = meta.MakeSelect();
                sql = sql.Replace("{}", s);
            }
            using var cmd = new OracleCommand(sql, connection);
            using IDataReader reader = cmd.ExecuteReader();
            object root = meta.ConstructJson(reader, true, useAutoColumns);
            reader.Close();
            if (printOut)
                Console.WriteLine(root.ToString());
            return meta.RowCount;
        }

        protected int OracleTest(string sql)
        {
            using var cmd = new OracleCommand(sql, connection);
            using IDataReader reader = cmd.ExecuteReader();
            var meta = Meta.MakeMeta(reader, out int minLevel);
            if (printMeta)
                Console.WriteLine(meta.ToString(""));
            JToken root = meta.ConstructJson(reader, true, false);
            reader.Close();
            if (printOut)
                Console.WriteLine(root.ToString());
            return meta.RowCount;
        }

        static void Main(string[] args)
        {
            conf = JObject.Parse(System.IO.File.ReadAllText("appsettings.json"));

            string connectionStr = "";
            if (System.IO.File.Exists(@".JsonTest"))
                foreach(var line in System.IO.File.ReadAllLines(@".JsonTest"))
                    if (line.StartsWith("connection="))
                        connectionStr = line.Substring(11);
            
            Program program = new();

            bool memoryTest = false;
            bool stationTest = false;
            bool departmentTest = false;
            bool ataTest = false;
            bool ataTest2 = false;

            var cmdLineQuery = new List<string>();

            foreach(var a in args)
                if      (a.StartsWith('~')) ;
                else if (a.Equals("-p")) program.printOut = true;
                else if (a.Equals("-t")) Meta.trace = true;
                else if (a.Equals("-m")) printMeta = true;
                else if (a.Equals("-M")) memoryTest = true;
                else if (a.Equals("-A")) ataTest = true;
                else if (a.Equals("-S")) stationTest = true;
                else if (a.Equals("-D")) departmentTest = true;
                else if (a.Equals("-A2")) ataTest2 = true;
                else cmdLineQuery.Add(a);

            //TestJObject();

            if (memoryTest)                    // demonstrates sub-object and two parallel collections, no SQL used
                program.wrap("InMemoryTest", program.InMemoryTest);

            if (stationTest || ataTest || departmentTest || ataTest2 || cmdLineQuery.Count>0)
            {
                if (connectionStr.Equals(""))
                {
                    Console.WriteLine("have no connection string available, use .program file");
                    return;
                }
                program.connection = new OracleConnection(connectionStr);
                program.connection.Open();

                if (stationTest)                // demonstrates sub-object and sub-collection
                    program.wrap("OracleStationTest", program.OracleTest, (JObject)conf["StationMeta"], conf["StationQuery"].ToString(), true);

                if (ataTest)                    // demonstrates two parallel sub-collections
                    program.wrap("OracleATATest", program.OracleTest, (JObject)conf["ATAMeta"], conf["ATAQuery"].ToString(), true);

                if (departmentTest)             // demonstrates two parallel sub-collections
                    program.wrap("DepartmentTest", program.OracleTest, (JObject)conf["DepartmentMeta"], conf["DepartmentQuery"].ToString(), false);

                if (ataTest2)                    // demonstrates two parallel sub-collections
                    program.wrap("OracleATATest", program.OracleTest, conf["ATAQuery2"].ToString());

                if (cmdLineQuery.Count > 0)
                {
                    string sql =  string.Join(' ', cmdLineQuery);
                    sql = sql.Replace('`', '"');
                    Console.WriteLine(sql);
                    program.wrap("CommandLine", program.OracleTest, sql);
                }

                program.connection.Close();
                program.connection.Dispose();
            }
        }

        public void wrap(string name, Func<int> orig)
        {
            System.DateTime started = System.DateTime.Now;
            int cnt = orig();
            Console.WriteLine($"{name} cnt={cnt}, elapsed: {System.DateTime.Now.Subtract(started)}");
        } 
        public void wrap(string name, Func<string, int> orig, string sql)
        {
            System.DateTime started = System.DateTime.Now;
            int cnt = orig(sql);
            Console.WriteLine($"{name} cnt={cnt}, elapsed: {System.DateTime.Now.Subtract(started)}");
        } 
        public void wrap(string name, Func<JObject, string, bool, int> orig, JObject meta, string sql, bool autoColumns)
        {
            System.DateTime started = System.DateTime.Now;
            int cnt = orig(meta, sql, autoColumns);
            Console.WriteLine($"{name} cnt={cnt}, elapsed: {System.DateTime.Now.Subtract(started)}");
        } 
    }

}

