# JsonReader
C# code to represent DB relation data  in JSON format.

Here is a simple example, wich demostrates a two-level hierachy of objects - list of users where each user has a collection of posts.

For this example we use Oracle ODP, but you may use any provider which implements IDataReader interface.
```
_string sql = @"select U.ID as USER_ID, U.NAME, P.ID as POST_ID, P.POSTED_AT, P.TEXT
from USER U
join POST P on P.USER_ID=U.ID
order by U.ID, P.ID";

string metaStr = @"{
    "id": {"$collectionid": "USER_ID"},
    "name": "NAME",
    "posts": [{
        "id": {"$collectionid": "POST_ID"},
        "timestamp": "POSTED_AT",
        "text": "TEXT"
    }]
}";

// create transformation metadata object, same time we define a name for the tompost Json element
Meta meta = Meta.MakeMeta(metaStr, "My List Of Users");
// prepare a reader object
using connection = new OracleConnection("scott/tiger@testdb");
using var cmd = new OracleCommand(sql, connection);
using IDataReader reader = cmd.ExecuteReader();         // prepare
// pass it to the metadata object and get a Json result
object root = meta.ConstructJson(reader, true, useAutoColumns);
reader.Close();
Console.WriteLine($"processed {meta.RowCount} rows");
Console.WriteLine(root.ToString());_
```

The result will look like 
```
{
    "My List Of Users": [
        {
            "id": 1,
            "name": "Alex",
            "posts": [
                {
                    "id": 1001,
                    "timestamp": "01/12/2022T07:01:30",
                    "text": "My fisrt post"
                },
                {
                    "id": 1003,
                    "timestamp": "01/13/2022T11:00:00",
                    "text": "I've started new project"
                },
                {
                    "id": 1006,
                    "timestamp": "01/14/2022T03:01:30",
                    "text": "I've finished the project"
                },
            ]
        },
        {
            "id": 2,
            "name": "John",
            "posts": [
                {
                    "id": 1002,
                    "timestamp": "01/12/2022T09:31:00",
                    "text": "I like to program"
                },
                {
                    "id": 1004,
                    "timestamp": "01/13/2022T13:00:00",
                    "text": "I like to eat"
                },
                {
                    "id": 1005,
                    "timestamp": "01/13/2022T23:11:35",
                    "text": "I like to sleep"
                },
            ]
        }
    ]
}
```
