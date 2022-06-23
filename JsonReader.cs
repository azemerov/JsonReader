/*
* Converts data records returned into JSON structured object.
* Distributed under GPLv3 license (see LICENSE file)
* Repository: https://github.com/azemerov/JsonReader
*
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
        protected Dictionary<string,string> columnRefs;
        int rowCount;
        public int RowCount { get { return rowCount; }}

        public Meta(string name)
        {
            Name = name;
            Mappings = new();
            Subobjects = new();
            Subcollections = new();
            columnRefs = null; // will be set only on the topmost Meta object
        }

        static public Meta MakeMeta(string metaDataStr)
        {
            var metaDoc = JObject.Parse(metaDataStr);
            JToken json = metaDoc.Root;
            Meta meta = null;
            if (json.Type == JTokenType.Array )
            {
                foreach(var subobj in json)
                {
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

            int idx = 0;
            meta.columnRefs = new();
            meta.CollectColumns(meta.columnRefs, ref idx);
            
            return meta;
        }

        public string MakeSelect()
        {
            if (columnRefs != null)
            {
                string result = "";
                foreach(var exprCol in columnRefs)
                    result += $" {exprCol.Key} as {exprCol.Value},";
                return result.TrimEnd(',');
            }
            return "";
        }

        static protected Meta MakeMeta(JToken element, string name)
        {
            Meta result = new(name);
            // var childs = element.EnumerateObject();  while (childs.MoveNext()) { var child = childs.Current; ... }
            foreach (var child_ in element)
            {
                JProperty child = (JProperty) child_;
                if (child.Value.Type == JTokenType.String )
                {
                    if (child.Name.Equals("$collectionid"))
                        result.__collectionid = (child.Value.ToString());
                    else
                        result.Mappings.Add(new  FieldMap(child.Name, child.Value.ToString()));
                }
                else
                if (child.Value.Type == JTokenType.Array)
                {
                    MetaArray array = new(child.Name);
                    result.Subcollections.Add(array);
                    foreach(var subobj in child.Value)
                    {
                        JToken subchild = (JToken) subobj;
                        var sub = MakeMeta(subchild, "");
                        array.CollectionKeyID = sub.__collectionid;
                        array.SubMeta = sub;
                        break; // we expect only one array item in metadata
                    }
                }
                else
                if (child.Value.Type == JTokenType.Object)
                {
                    var sub = MakeMeta(child.Value, child.Name);
                    result.Subobjects.Add(sub);
                }
            }
            return result;
        }

        public virtual void CollectColumns(Dictionary<string, string> columnRefs, ref int idx)
        {
            foreach(var map in Mappings)
                if (!columnRefs.ContainsKey(map.Expression))
                {
                    columnRefs[map.Expression] = $"C{idx}";
                    idx ++;
                }
            foreach(var child in Subobjects)
                child.CollectColumns(columnRefs, ref idx);
            foreach(var col in Subcollections)
                col.CollectColumns(columnRefs, ref idx);
        }

        protected internal virtual void ResetCollectionIDs()
        {
            foreach(var child in Subobjects)
                child.ResetCollectionIDs();
            foreach(var child in Subcollections)
                child.ResetCollectionIDs();
        }

        public JObject ConstructJson(IDataReader reader, bool useAutoColumn=false)
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
                    System.Console.WriteLine($"... record# {rowCount}: {s}");
                }
                if (useAutoColumn)
                    Process(reader, ref root, columnRefs);
                else
                    Process(reader, ref root, null);
                rowCount++;
            }
            return root;
        }

        static protected internal int ColumnIndex(IDataReader rs, string columnName, Dictionary<string,string> columnRefs)
        {
            if (columnRefs != null)
                columnName = columnRefs[columnName];

            for (int i=0; i<rs.FieldCount; i++)
                if (rs.GetName(i).Equals(columnName))
                    return i;
            return -1;
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

        protected internal virtual void Process(IDataReader rs, ref JObject result, Dictionary<string,string> columnRefs)
        {
            //System.Console.WriteLine($"    Process(object) {this.Name}");

            if (result.Count==0) // optimization - not yet loaded
                foreach(var map in Mappings)
                {
                    var idx = ColumnIndex(rs, map.Expression, columnRefs);
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
                
                sub.Process(rs, ref subObj, columnRefs);
            }

            foreach(var coll in Subcollections)
                coll.Process(rs, ref result, columnRefs);
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

        public override void CollectColumns(Dictionary<string, string> columnRefs, ref int idx)
        {
            if (!columnRefs.ContainsKey(CollectionKeyID))
            {
                columnRefs[CollectionKeyID] = $"C{idx}";
                idx ++;
            }
            SubMeta.CollectColumns(columnRefs, ref idx);
        }

        protected internal override void Process(IDataReader rs, ref JObject result, Dictionary<string,string> columnRefs)
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
                CollectionKeyIndex = ColumnIndex(rs, CollectionKeyID, columnRefs);
 
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
                SubMeta.Process(rs, ref currentItem, columnRefs);

        }
    } 
}
