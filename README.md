# FastDataReaderMapper
Simple extension for fast mapping objects from IDataReader base source. You can use it for any DataReader, like SQL or Excel.
Usage:
```cs
using FastDataReaderMapper;
using (SqlCommand cmd = connection.CreateCommand("Select * from table"))
{
    foreach(var item in cmd.ExecuteReader().Map<MyObject>())
        ...
}
```
Also it supports IAsyncEnumerable:
```cs
using FastDataReaderMapper;
using (SqlCommand cmd = connection.CreateCommand("Select * from table"))
{
    await foreach(var item in cmd.ExecuteReader().MapAsync<MyObject>())
        ...
}
```
Async approach use tiny bit of memory and allow you use mapped objects on fly without waiting for complete mapping process of query.

Also for this mapper you could overwrite column names if in source they have different names. Simple use *ColumnAttribute* from System.ComponentModel.DataAnnotations.Schema namespace.
Usage:
```cs
class MyCustomObject
{
    public int Id {get;set;}
    
    [Column("Custom_Field_Name_In_Database")]
    public string MyData {get;set;}
}
```