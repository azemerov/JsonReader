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
*      'Id':  {$collectionid: 'ID'},
*      'Name': 'ATA_DESCRIPTION',
*      'Address': {
*          'Street': 'ADDRESS_STREET',
*          'City':    'ADDRESS_CITY'
*      },
*      'Previous addresses': [{
*          'Id':            {$collectionid: 'ADDRESS_ID'},
*          'From':          'ADDRESS_START_DATE',
*          'Till':          'ADDRESS_END_DATE',
*          'Street':        'ADDRESS_STREET',
*          'City':          'ADDRESS_CITY'
*      }],
*      'Jobs': [{
*          'Id':            {$collectionid: 'JOB_ID'},
*          'From':          'JOB_START_DATE',
*          'Till':          'JOB_END_DATE',
*          'Position':      'JOB_POSITION',
           'IsCurrent':     {'$bool': 'JOB_IS_CURRENT'}
*      }]
*  }
*
*/

using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;

namespace Vespa.Db
{
    public class FieldMap
    {
        public string Name;
        public string Expression;
        public Func<object, object> Function;
        public FieldMap(string name, string expression, Func<object, object> function)
        {
            Name = name;
            Expression = expression;
            Function = function;
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

        static public Meta MakeMeta(JObject metaDoc, string rootName="roots")
        {
            JToken json = metaDoc;
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

            meta = new MetaArray(rootName, meta); // wrap into collection

            int idx = 0;
            meta.columnRefs = new(StringComparer.OrdinalIgnoreCase);
            meta.CollectColumns(meta.columnRefs, ref idx);
            
            return meta;
        }

        static public Meta MakeMeta(string metaDataStr, string rootName="roots")
        {
            var metaDoc = JObject.Parse(metaDataStr);
            return MakeMeta(metaDoc, rootName);
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
                string childName = child.Name;
                Func<object, object> function = null;
                bool isCollectionId = false;

                if (child.Value.Type == JTokenType.Object)
                    foreach(var subchild_ in child.Value)
                    if (subchild_.Type == JTokenType.Property)
                    {
                        JProperty subchild = (JProperty) subchild_;
                        if (subchild.Name.Equals("$collectionid") )
                        {
                            isCollectionId = true;
                            child = subchild;
                        }
                        else
                        if (subchild.Name.Equals("$bool") )
                        {
                            function = (v =>  (v.Equals("Y") || v.Equals("y") || v.Equals(1)));
                            child = subchild;
                        }
                        if (subchild.Name.Equals("$int") )
                        {
                            function = (v =>  v is DBNull ? 0 : Convert.ToInt32(v));
                            child = subchild;
                        }
                        if (subchild.Name.Equals("$str") )
                        {
                            function = (v =>  v is DBNull ? "" : Convert.ToString(v));
                            child = subchild;
                        }
                        if (subchild.Name.StartsWith("$str:") )
                        {
                            var format = subchild.Name.Substring(5);
                            function = (v =>  v is DBNull ? "" : v is DateTime ? ((DateTime)v).ToString(format) : v.ToString());
                            child = subchild;
                        }
                        break;
                    }

                if (child.Value.Type == JTokenType.String )
                {
                    if (isCollectionId)
                        result.__collectionid = (child.Value.ToString());
                    var childValue = child.Value.ToString();
                    if (childValue.Equals("=")) childValue = childName;
                    result.Mappings.Add(new  FieldMap(childName, childValue, function));
                }
                else
                if (child.Value.Type == JTokenType.Array)
                {
                    MetaArray array = new(childName);
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
                    var sub = MakeMeta(child.Value, childName);
                    result.Subobjects.Add(sub);
                }
            }
            return result;
        }

        public virtual Meta ChildByName(string name)
        {
            foreach (var sub in Subobjects)
                if(sub.Name.Equals(name)) return sub;
            foreach (var sub in Subcollections)
                if(sub.Name.Equals(name)) return sub;
            return null;
        }

        static public Meta MakeMeta(IDataReader reader, out int minLevel)
        {
            Meta result = new("");
            minLevel = 9999;
            int addLevel = 0;
            for (int i=0; i<reader.FieldCount; i++)
            {
                var fullName = reader.GetName(i);
                var cname = fullName;
                string fname = "";
                string caction = "";
                var vals =cname.Split(':');
                cname = vals[0];
                for (int j=1; j<vals.Length; j++)
                    if (string.Compare(vals[j], "id", StringComparison.OrdinalIgnoreCase)==0)
                        caction = vals[j];
                    else
                        fname = vals[j];

                var names = cname.Split('.');
                int level = names.Length;
                var current = result;
                for (int j=0; j<names.Length-1; j++)
                {
                    var n = current.ChildByName(names[j]);
                    if (n==null)
                    {
                        if (caction.Equals("id"))
                        {
                            var sub = new Meta("");
                            sub.__collectionid = fullName;
                            current.Subcollections.Add(new MetaArray(names[j], sub));
                            current = sub;
                            addLevel = 1;
                        }
                        else
                        {
                            n = new Meta(names[j]);
                            current.Subobjects.Add(n);
                            current = n;
                        }
                    }
                    else if (n is MetaArray)
                        current = ((MetaArray)n).SubMeta;
                    else
                        current = n;
                }

                if (level+addLevel < minLevel)
                    minLevel = level+addLevel;

                Func<object,object> function = null;
                if (fname.Equals("bool") )
                    function = (v =>  (v.Equals("Y") || v.Equals("y") || v.Equals(1)));
                if (fname.Equals("int") )
                    function = (v =>  v is DBNull ? 0 : Convert.ToInt32(v));
                if (fname.Equals("str") )
                    function = (v =>  v is DBNull ? "" : Convert.ToString(v));
                if (fname.StartsWith("str-") )
                {
                    var format = fname.Substring(5);
                    function = (v =>  v is DBNull ? "" : v is DateTime ? ((DateTime)v).ToString(format) : v.ToString());
                }

                current.Mappings.Add(new FieldMap(names[names.Length-1], fullName, function));
                
            }

            if (result.Subcollections.Count==0)
            {
                minLevel = 0;
                return result;
            }
            else
                return result.Subcollections[0];
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

        public static string RowString(IDataReader reader)
        {
            var s = "";
            for (int i=0; i<reader.FieldCount; i++ )
                if (reader.IsDBNull(i))
                s += ",";
                else
                s += reader.GetString(i)+",";
            return s;
        }

        public JObject ConstructJson(IDataReader reader, bool sorted, bool useAutoColumn=false)
        {
            JObject root = new JObject();
            rowCount = 0;
            while (reader.Read()) {
                if (trace)
                    System.Console.WriteLine($"... record# {rowCount}: {RowString(reader)}");

                if (useAutoColumn)
                    Process(reader, ref root, sorted, columnRefs);
                else
                    Process(reader, ref root, sorted, null);
                rowCount++;
            }
            return root;
        }

        static protected internal int ColumnIndex(IDataReader rs, string columnName, Dictionary<string,string> columnRefs)
        {
            if (columnRefs != null)
                columnName = columnRefs[columnName];

            for (int i=0; i<rs.FieldCount; i++)
                if (rs.GetName(i).Equals(columnName, StringComparison.OrdinalIgnoreCase))
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

        protected internal virtual void Process(IDataReader rs, ref JObject result, bool sorted, Dictionary<string,string> columnRefs)
        {
            //System.Console.WriteLine($"    Process(object) {this.Name}");

            if (result.Count==0) // optimization - not yet loaded
                foreach(var map in Mappings)
                {
                    var idx = ColumnIndex(rs, map.Expression, columnRefs);
                    if (idx > -1)
                    {
                        var val = rs.GetValue(idx);
                        if (map.Function != null)
                            val = map.Function(val);

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
                
                sub.Process(rs, ref subObj, sorted, columnRefs);
            }

            foreach(var coll in Subcollections)
                coll.Process(rs, ref result, sorted, columnRefs);
        }

        public virtual string ToString(string offset)
        {
            string s = "";
            foreach(var map in Mappings)
                s += $"{offset+"  "}{map.Name}: {map.Expression}\n";
            foreach(var obj in Subobjects)
                s += $"{obj.ToString(offset+"  ")}\n";
            foreach(var col in Subcollections)
                s += $"{col.ToString(offset+"  ")}\n";
            if (Name.Equals(""))
                return $"{offset}{{\n{s}{offset}}}";
            else
                return $"{offset}{Name}: {{\n{s}{offset}}}";
        }
    }

    public class MetaArray: Meta
    {
        public string CollectionKeyID;    // key column name
        public int CollectionKeyIndex;    // key column index
        public SortedSet<string> CollectionKeyValues; // used for non-sorted recordsets
        public string CollectionKeyValue; // key column value, used for sorted recordsets
        public Meta SubMeta;
        JObject currentItem = null;

        public MetaArray(string name, Meta subItem=null) : base(name)
        {
            CollectionKeyIndex = -1;
            CollectionKeyValue = "";
            CollectionKeyValues = new();
            if (subItem==null)
                CollectionKeyID = "";
            else
                CollectionKeyID = subItem.__collectionid;
            SubMeta = subItem;
        }

        public override Meta ChildByName(string name)
        {
            //if (SubMeta.Name.Equals(name))
            return SubMeta.ChildByName(name);
            //return null;
        }

        protected internal override void ResetCollectionIDs()
        {
            currentItem = null;
            CollectionKeyValue = "";
            CollectionKeyValues.Clear();
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

        protected internal override void Process(IDataReader rs, ref JObject result, bool sorted, Dictionary<string,string> columnRefs)
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
            if (keyValue.Equals("")) return;

            if (
                (!sorted && !CollectionKeyValues.Contains(keyValue))
                ||
                (sorted && keyValue.CompareTo(CollectionKeyValue) > 0)
            )
            {
                SubMeta.ResetCollectionIDs();
                if (sorted)
                    CollectionKeyValue = keyValue;
                else
                    CollectionKeyValues.Add(keyValue);
                currentItem = new JObject();
                array.Add(currentItem);
            }
            if (currentItem != null)
                SubMeta.Process(rs, ref currentItem, sorted, columnRefs);
        }

        public override string ToString(string offset)
        {
            return $"{offset}{Name}: [\n{SubMeta.ToString(offset+"  ")}\n{offset}]";
        }
    } 
}
