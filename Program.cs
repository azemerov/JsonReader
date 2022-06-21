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

        static int InMemoryTest(bool print)
        {

            string dataStr = 
@"ATA_ID,ATA_DESCRIPTION,ATA_FLAG1,ATA_FLAG2,SUB_ATA_ID,SUB_DESCRIPTION,COL2_ID,COL2_NAME
01,ATA 01,Y,N,001,SUB ATA 0101,1,C11
01,ATA 01,Y,N,002,SUB ATA 0102,2,C12
02,ATA 02,Y,Y,001,SUB ATA 0201,1,C21
02,ATA 02,Y,Y,002,SUB ATA 0202,2,C22";

            string metaStr = 
@"{
    '$collectionid': 'ATA_ID',
    'ata_id': 'ATA_ID',
    'chapter_name': 'ATA_DESCRIPTION',
    'ata_flags': {
        'has_oil_svc_yn': 'ATA_FLAG1',
        'is_oil_svc_yn': 'ATA_FLAG2'
    },
    'sub_atas': [{
        '$collectionid': 'SUB_ATA_ID',
        'id': 'SUB_ATA_ID',
        'sub_chapter_name': 'SUB_DESCRIPTION'
    }],
    'Items': [{
        '$collectionid': 'COL2_ID',
        'id': 'COL2_ID',
        'name': 'COL2_NAME'
    }]
}";

            IDataReader reader;
            List<string> dataLines = new List<string>(dataStr.Replace("\r", "").Split('\n'));
            //reader = new CSVDataReader(new MyListReader(dataLines), ',', true);
            //while (reader.Read())
            //    Console.WriteLine($"{reader.GetValue(0)}, {reader.GetValue(2)}, {reader.GetValue(reader.GetOrdinal("ATA_DESCRIPTION"))}");
            //reader.Close();
            //reader.Dispose();

            var meta = Meta.MakeMeta(metaStr);
            /*IDataReader*/ reader = new CSVDataReader(new MyListReader(dataLines), ',', true);
            object root = meta.ConstructJson(reader);
            reader.Close();
            reader.Dispose();
            if (print)
                Console.WriteLine(root.ToString());
            return meta.RowCount;
        }

        static int OracleTest(OracleConnection connection, string metadataStr, string sql, bool print)
        {
            using var cmd = new OracleCommand(sql, connection);
            using IDataReader reader = cmd.ExecuteReader();
            var meta = Meta.MakeMeta(metadataStr);
            object root = meta.ConstructJson(reader);
            reader.Close();
            if (print)
                Console.WriteLine(root.ToString());
            return meta.RowCount;
        }

        static int OracleATATest(OracleConnection connection, bool print)
        {
            string metaStr = 
@"{
    '$collectionid': 'ATA_ID',
    'ata_id': 'ATA_ID',
    'chapter_name': 'ATA_DESCRIPTION',
    'ata_flags': {
        'has_oil_svc_yn': 'ATA_FLAG1',
        'is_oil_svc_yn': 'ATA_FLAG2'
    },
    'sub_atas': [{
        '$collectionid': 'SUB_ATA_ID',
        'id': 'SUB_ATA_ID',
        'sub_chapter_name': 'SUB_DESCRIPTION'
    }]
}";

            var sql = 
@"select C.ATA_ID, C.DESCRIPTION ATA_DESCRIPTION, CF.has_oil_svc_yn  ata_flag1, CF.is_rii_item_yn ata_flag2, S.sub_ata_id, S.description SUB_DESCRIPTION 
from ATA_CHAPTER C left join ATA_CHAPTER_MTX_FLAG CF on CF.ATA_ID=C.ATA_ID left join ATA_SUB_CHAPTER S on S.ATA_ID=C.ATA_ID
/*where C.ata_id in ('167CCD91-2081-11D4-B30E-0008C7E97D95','167CCD6C-2081-11D4-B30E-0008C7E97D95')
*/order by c.ata_id, CF.id, s.sub_ata_id";

            return OracleTest(connection, metaStr, sql, print);
        }

        static int OracleStationTest(OracleConnection connection, bool print)
        {

            var metaStr = 
@"{
    '$collectionid': 'ID',
    'id': 'ID',
    'airport': 'AIRPORT_IDENTIFIER',
    'stock_points': [{
        '$collectionid': 'STOCK_POINT_ID',
        'id': 'STOCK_POINT_ID',
        'sp': 'STOCK_POINT',
        'company': 'COMPANY_CODE',
        'name': 'STOCK_POINT_DESCRIPTION'
    }],
    'shops': [{
        '$collectionid': 'SHOP_ID',
        'id': 'SHOP_ID',
        'name': 'SHOP_DESCRIPTION'
    }]
}";

            var sql = 
@"select s.id, s.airport_identifier, p.stock_point_id, p.stock_point, p.company_code, p.description stock_point_description, h.shop_id, h.description shop_description from station s
left join stock_point p on p.station_id=s.id
left join SHOP h on h.station_id=s.id
/*where s.id in ('2','22') */
order by s.id, p.stock_point_id, h.shop_id";

            return OracleTest(connection, metaStr, sql, print);
        }

        static bool DO_PRINT = false;
        static void Main(string[] args)
        {
            string connectionStr = "";
            if (System.IO.File.Exists(@".program"))
                foreach(var line in System.IO.File.ReadAllLines(@".program"))
                    if (line.StartsWith("connection="))
                        connectionStr = line.Substring(11);
                
            bool memoryTest = false;
            bool stationTest = false;
            bool ataTest = false;
            foreach(var a in args)
                if      (a.Equals("p")) DO_PRINT = true;
                else if (a.Equals("t")) Meta.trace = true;
                else if (a.Equals("M")) memoryTest = true;
                else if (a.Equals("S")) stationTest = true;
                else if (a.Equals("A")) ataTest = true;

            //TestJObject();

            if (memoryTest)
                // demonstrates sub-object and two parallel collections without Oracle connection
                wrap("InMemoryTest", InMemoryTest);

            if (stationTest || ataTest)
            {
                if (connectionStr.Equals(""))
                {
                    Console.WriteLine("have no connection string available, use .program file");
                    return;
                }
                using var connection = new OracleConnection(connectionStr);
                connection.Open();

                if (stationTest)
                    // demonstrates sub-object and sub-collection
                    wrap(connection, "OracleStationTest", OracleStationTest);

                if (ataTest)
                    // demonstrates two parallel sub-collections
                    wrap(connection, "OracleATATest", OracleATATest);
            }
        }

        public static void wrap(string name, Func<bool, int> orig)
        {
            System.DateTime started = System.DateTime.Now;
            int cnt = orig(DO_PRINT);
            Console.WriteLine($"{name} cnt={cnt}, elapsed: {System.DateTime.Now.Subtract(started)}");
        } 
        public static void wrap(OracleConnection connection, string name, Func<OracleConnection, bool, int> orig)
        {
            System.DateTime started = System.DateTime.Now;
            int cnt = orig(connection, DO_PRINT);
            Console.WriteLine($"{name} cnt={cnt}, elapsed: {System.DateTime.Now.Subtract(started)}");
        } 
    }

}

