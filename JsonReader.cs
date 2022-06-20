/*
* Converts data records returned into JSON structured object.
* A metadata object has to be created using MakeMeta() function whcih takes JSON string which describes expected data structure.
* Then call metadata.ConstructJson() which takes IDataReader object.
* Metadata should be described like 
*
*  {
*      '$collectionid': 'ID',
*      'Id':  'ID',
*      'Name': 'ATA_DESCRIPTION',
*      'Address': {
*          'Street': 'ADDRESS_STREET',
*          'City':    'ADDRESS_CITY'
*      },
*      'Previous addresses': [{
*          '$collectionid': 'ADDRESS_ID',
*          'Id':            'ADDRESS_ID',
*          'From':          'ADDRESS_START_DATE',
*          'Till':          'ADDRESS_END_DATE',
*          'Street':        'ADDRESS_STREET',
*          'City':          'ADDRESS_CITY'
*      }],
*      'Jobs': [{
*          '$collectionid': 'JOB_ID',
*          'Id':            'JOB_ID',
*          'From':          'JOB_START_DATE',
*          'Till':          'JOB_END_DATE',
*          'Position':      'JOB_POSITION'
*      }]
*  }
*
*/

//-using System.Text.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using System.Data;

namespace Vespa.Db
{
    public class FieldMap
    {
        public string Name;
        public string Expression;
        public FieldMap(string name, string expression)
        {
            Name = name;
            Expression = expression;
        }
    }
    public class Meta
    {
        public static bool trace = false;
        public string Name;
        public List<FieldMap> Mappings;
        public List<Meta> Subobjects;
        public List<MetaArray> Subcollections;
        public string __collectionid = ""; //used only during MakeMeta as a temporary holder
        int rowCount;
        public int RowCount { get { return rowCount; }}

        public Meta(string name)
        {
            Name = name;
            Mappings = new();
            Subobjects = new();
            Subcollections = new();
        }

        static public Meta MakeMeta(string metaDataStr)
        {
            //-using JsonDocument metaDoc = JsonDocument.Parse(metaDataStr);
            //-JsonElement json = metaDoc.RootElement;
            var metaDoc = JObject.Parse(metaDataStr);
            JToken json = metaDoc.Root;
            Meta meta = null;
            //-if (json.ValueKind==JsonValueKind.Array)
            if (json.Type == JTokenType.Array )
            {
                //-foreach(var subobj in json.EnumerateArray())
                foreach(var subobj in json)
                {
                    //-JsonElement subjson = (JsonElement) subobj;
                    JToken subjson = subobj;
                    meta = Meta.MakeMeta(subjson, "");
                    break; // we expect only one array item in metadata
                }
            }
            else
            {
                meta = Meta.MakeMeta(json, "");
            }
            meta = new MetaArray("roots", meta); // wrap into collection
            return meta;
        }
        //-static protected Meta MakeMeta(System.Text.Json.JsonElement element, string name)
        static protected Meta MakeMeta(JToken element, string name)
        {
            Meta result = new(name);
            // var childs = element.EnumerateObject();  while (childs.MoveNext()) { var child = childs.Current; ... }
            //-foreach (var child in element.EnumerateObject())
            foreach (var child_ in element)
            {
                JProperty child = (JProperty) child_;
                //-if (child.Value.ValueKind == JsonValueKind.String)
                if (child.Value.Type == JTokenType.String )
                {
                    //-if (child.Name.Equals("$collectionid"))
                    if (child.Name.Equals("$collectionid"))
                        //-result.__collectionid = child.Value.ToString();
                        result.__collectionid = (child.Value.ToString());
                    else
                        //-result.Mappings.Add(new  FieldMap(child.Name, child.Value.ToString()));
                        result.Mappings.Add(new  FieldMap(child.Name, child.Value.ToString()));
                }
                else
                //-if (child.Value.ValueKind == JsonValueKind.Array)
                if (child.Value.Type == JTokenType.Array)
                {
                    MetaArray array = new(child.Name);
                    result.Subcollections.Add(array);
                    //-foreach(var subobj in child.Value.EnumerateArray())
                    foreach(var subobj in child.Value)
                    {
                        //-JsonElement subchild = (JsonElement) subobj;
                        JToken subchild = (JToken) subobj;
                        var sub = MakeMeta(subchild, "");
                        array.CollectionKeyID = sub.__collectionid;
                        array.SubMeta = sub;
                        break; // we expect only one array item in metadata
                    }
                }
                else
                //-if (child.Value.ValueKind == JsonValueKind.Object)
                if (child.Value.Type == JTokenType.Object)
                {
                    var sub = MakeMeta(child.Value, child.Name);
                    /*~
                    if (sub.__collectionid.Length > 0)
                    {
                        MetaArray array = new(result, child.Name);
                        result.Subcollections.Add(array);
                        //~sub.Parent = array; // reassing parent to the MetaArray object
                        array.CollectionKeyID = sub.__collectionid;
                        array.SubMeta = sub;
                    }
                    else
                    */
                    result.Subobjects.Add(sub);
                }
            }
            return result;
        }

        protected internal virtual void ResetCollectionIDs()
        {
            foreach(var child in Subobjects)
                child.ResetCollectionIDs();
            foreach(var child in Subcollections)
                child.ResetCollectionIDs();
        }

        public JObject ConstructJson(IDataReader reader)
        {
            JObject root = new JObject();
            rowCount = 0;
            while (reader.Read()) {
                if (trace)
                {
                    var s = "";
                    for (int i=0; i<reader.FieldCount; i++ )
                        if (reader.IsDBNull(i))
                        s += ",";
                        else
                        s += reader.GetString(i)+",";
                    //System.Console.WriteLine($"... record# {rowCount}: {s}");
		    System.Console.WriteLine(s);
                }
                Process(reader, ref root);
                rowCount++;
            }
            return root;
        }

        static protected internal int ColumnIndex(IDataReader rs, string columnName)
        {
            try { return rs.GetOrdinal(columnName); }
            catch {return -1;}
        }

        static protected internal string GetString(IDataReader rs, int columnIndex, string defaultValue)
        {
            if (columnIndex > -1)
            {
                if (rs.IsDBNull(columnIndex))
                    return defaultValue;
                else
                    return rs.GetString(columnIndex);
            }
            else
                return defaultValue;

        }

        protected internal virtual void Process(IDataReader rs, ref JObject result)
        {
            //System.Console.WriteLine($"    Process(object) {this.Name}");

            if (result.Count==0) //optimization
                foreach(var map in Mappings)
                {
                    var idx = ColumnIndex(rs, map.Expression);
                    if (idx > -1)
                    {
                        var val = rs.GetValue(idx);
                        if (val != null)
                        {
                            JToken t;
                            if (!result.TryGetValue(map.Name, out t))
                                result.Add(map.Name, new JValue(val));
                            else
                                ((JValue)t).Value = val;
                        }
                    }
                    else
                        result.Add(map.Name, new JValue((object)null));
                }

            foreach(var sub in Subobjects)
            {
                JToken token;
                JObject subObj;
                if (!result.TryGetValue(sub.Name, out token))
                {
                    subObj = new JObject();
                    result.Add(sub.Name, subObj);
                }
                else
                    subObj = (JObject) token;
                
                sub.Process(rs, ref subObj);
            }

            foreach(var coll in Subcollections)
                coll.Process(rs, ref result);
        }
    }

    public class MetaArray: Meta
    {
        public string CollectionKeyID;    // key column name
        public int CollectionKeyIndex;    // key column index
        public string CollectionKeyValue; // key column value
        public Meta SubMeta;
        JObject currentItem = null;

        public MetaArray(string name, Meta subItem=null) : base(name)
        {
            CollectionKeyIndex = -1;
            CollectionKeyValue = "";
            if (subItem==null)
                CollectionKeyID = "";
            else
                CollectionKeyID = subItem.__collectionid;
            SubMeta = subItem;
        }

        protected internal override void ResetCollectionIDs()
        {
            currentItem = null;
            CollectionKeyValue = "";
            SubMeta.ResetCollectionIDs();
        }

        protected internal override void Process(IDataReader rs, ref JObject result)
        {
            //System.Console.WriteLine($"    Process(array) {this.Name}");

            JToken token;
            JArray array;
            if (!result.TryGetValue(this.Name, out token))
            {
                array = new JArray();
                result.Add(this.Name, array);
            }
            else
                array = (JArray) token;

            if (CollectionKeyIndex == -1)
                CollectionKeyIndex = ColumnIndex(rs, CollectionKeyID);
 
            string keyValue;
            keyValue = GetString(rs, CollectionKeyIndex, "");
            //System.Console.WriteLine($"... KEY {CollectionKeyID} = {keyValue}");
            if (!keyValue.Equals(CollectionKeyValue))
            {
                ResetCollectionIDs();
                CollectionKeyValue = keyValue;
                currentItem = new JObject();
                array.Add(currentItem);
            }
            if (currentItem != null)
                SubMeta.Process(rs, ref currentItem);

        }
    } 
}
