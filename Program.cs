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
01,ATA 01,Y,N,001,SUB ATA 0101,2,C12
01,ATA 01,Y,N,002,SUB ATA 0102,1,C11
01,ATA 01,Y,N,002,SUB ATA 0102,2,C12
02,ATA 02,Y,Y,001,SUB ATA 0201,3,C23
02,ATA 02,Y,Y,002,SUB ATA 0202,4,C24";

            string metaStr = 
@"{
    'ata_id': {$collectionid: 'ATA_ID'},
    'chapter_name': 'ATA_DESCRIPTION',
    'ata_flags': {
        'has_oil_svc_yn': {'$bool': 'ATA_FLAG1'},
        'is_oil_svc_yn': {'$bool': 'ATA_FLAG2'}
    },
    'sub_atas': [{
        'id': {$collectionid: 'SUB_ATA_ID'},
        'sub_chapter_name': 'SUB_DESCRIPTION'
    }],
    'Items': [{
        'id': {$collectionid: 'COL2_ID'},
        'name': 'COL2_NAME'
    }]
}";

            IDataReader reader;
            //List<string> dataLines = new List<string>(dataStr.Replace("\r", "").Split('\n'));
            List<string> dataLines = new List<string>(conf["InMemoryData"].ToString().Replace("\r", "").Split('\n'));
            var meta = Meta.MakeMeta(metaStr, "myCollection");
            //var meta = Meta.MakeMeta(conf["InMemoryMeta"], "myCollection");
            reader = new CSVDataReader(new ListReader(dataLines), ',', true);
            object root = meta.ConstructJson(reader, true, false);
            reader.Close();
            reader.Dispose();
            if (print)
                Console.WriteLine(root.ToString());
            return meta.RowCount;
        }

        static int OracleTest(OracleConnection connection, string metadataStr, string sql, bool useAutoColumns, bool print)
        {
            var meta = Meta.MakeMeta(metadataStr);
            if (useAutoColumns)
            {
                var s = meta.MakeSelect();
                sql = sql.Replace("{}", s);
            }
            using var cmd = new OracleCommand(sql, connection);
            using IDataReader reader = cmd.ExecuteReader();
            object root = meta.ConstructJson(reader, true, useAutoColumns);
            reader.Close();
            if (print)
                Console.WriteLine(root.ToString());
            return meta.RowCount;
        }

        static int OracleATATest(OracleConnection connection, bool print)
        {
            string metaStr = 
@"{
    'ata_id': {$collectionid: 'C.ATA_ID'},
    'chapter_name': 'C.DESCRIPTION',
    'ata_flags': {
        'has_oil_svc_yn': {$bool: 'CF.has_oil_svc_yn'},
        'is_oil_svc_yn': {$bool: 'CF.is_rii_item_yn'}
    },
    'sub_atas': [{
        'id': {$collectionid: 's.SUB_ATA_ID'},
        'sub_chapter_name': 'S.DESCRIPTION'
    }]
}";
            var sql=
@"select {} 
from ATA_CHAPTER C left join ATA_CHAPTER_MTX_FLAG CF on CF.ATA_ID=C.ATA_ID left join ATA_SUB_CHAPTER S on S.ATA_ID=C.ATA_ID
/*where C.ata_id in ('167CCD91-2081-11D4-B30E-0008C7E97D95','167CCD6C-2081-11D4-B30E-0008C7E97D95')
*/order by c.ata_id, CF.id, s.sub_ata_id";

            return OracleTest(connection, metaStr, sql, true, print);
        }

        static int OracleStationTest(OracleConnection connection, bool print)
        {

            var metaStr = 
@"{
    'id': {$collectionid: 's.ID'},
    'airport': 's.AIRPORT_IDENTIFIER',
    'bow_status_cfg': {$int: 's.BOW_STATUS_CFG'},
    'stock_points': [{
        'id': {$collectionid: 'p.STOCK_POINT_ID'},
        'sp': 'p.STOCK_POINT',
        'company': 'p.COMPANY_CODE',
        'name': 'p.DESCRIPTION'
    }],
    'shops': [{
        'id': {$collectionid: 'h.SHOP_ID'},
        'name': 'h.DESCRIPTION'
    }]
}";
            var sql = 
@"select {}
from station s
left join stock_point p on p.station_id=s.id
left join SHOP h on h.station_id=s.id
/*where s.id in ('2','22') */
order by s.id, p.stock_point_id, h.shop_id";

            return OracleTest(connection, metaStr, sql, true, print);
        }

        static int DepartmentTest(OracleConnection connection, bool print)
        {
            var metaStr = 
@"
    {
    'department_id': {$collectionid: 'id'},
     'company_code': '=',
     'active_yn': {$bool: '='},
     'description': '=',
     'right_code': 'department_right_code',
     'event_group_code': 'event_group_code'
    }
        ";
        
    var sql = 
@"
select 
d.id,
d.company_code,
d.active_yn ,
d.department_name,
d.DEPARTMENT_RIGHT_CODE,
case 
when eg.code is not null
then '['||eg.code||']'||et.code 
else
null
end event_group_code
from department d
left join event_template et on d.MERCURY_EVENT_CODE=et.ID
left join event_group eg on eg.id=et.GROUP_ID
where company_code='AE'
order by d.DEPARTMENT_NAME
";
            return OracleTest(connection, metaStr, sql, false, print);
        }

        static bool DO_PRINT = false;
        static JObject conf;

        static void Main(string[] args)
        {
            conf = JObject.Parse(System.IO.File.ReadAllText("appsettings.json"));

            string connectionStr = "";
            if (System.IO.File.Exists(@".JsonTest"))
                foreach(var line in System.IO.File.ReadAllLines(@".JsonTest"))
                    if (line.StartsWith("connection="))
                        connectionStr = line.Substring(11);
                
            bool memoryTest = false;
            bool stationTest = false;
            bool departmentTest = false;
            bool ataTest = false;
            foreach(var a in args)
                if      (a.Equals("p")) DO_PRINT = true;
                else if (a.Equals("t")) Meta.trace = true;
                else if (a.Equals("M")) memoryTest = true;
                else if (a.Equals("S")) stationTest = true;
                else if (a.Equals("A")) ataTest = true;
                else if (a.Equals("D")) departmentTest = true;

            //TestJObject();

            if (memoryTest)
                // demonstrates sub-object and two parallel collections without Oracle connection
                wrap("InMemoryTest", InMemoryTest);

            if (stationTest || ataTest || departmentTest)
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
                if (departmentTest)
                    // demonstrates two parallel sub-collections
                    wrap(connection, "YOTest", DepartmentTest);
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

