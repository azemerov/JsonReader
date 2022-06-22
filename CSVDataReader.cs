/*
* CSVDataReader - CVS data reader which implements IDataReader interface
* As a parameter it takes IMyLineReader object, can be MyFileReader (read lines from a file) or MyListReader (reads data from a List<string> object)
* Originated from Joseph Shijo's example -
* https://www.codeproject.com/Tips/1029831/Fast-and-Simple-IDataReader-Implementation-to-Read
*/

using System;
using System.Collections.Generic;
using System.IO;
using System.Data;

namespace Vespa.Data
{

    public interface ILineReader
    {
        public bool EndOfStream { get; }
        public string ReadLine();
        public void Reset();
        public void Close();
        public void Dispose();
    }

    public class FileReader: ILineReader
    {
        private string filePath;
        private StreamReader file;

        public FileReader(string filePath)
        {
            this.filePath = filePath;
            file = File.OpenText(filePath);
        }

        public bool EndOfStream { get {return file.EndOfStream;} }

        public string ReadLine()
        {
            if (file.EndOfStream)
                return "";
            else
                return file.ReadLine();
        }

        public void Reset()
        {
            file.Close();
            file = File.OpenText(filePath);
        }

        public void Close() { file.Close(); }

        public void Dispose() { file.Dispose(); }
    }

    public class ListReader: ILineReader
    {
        private List<string> lines;
        private int index;
        
        public ListReader(List<string> lines)
        {
            this.lines = lines;
            index = 0;
        }

        public bool EndOfStream { get {return index >= lines.Count;} }
        
        public string ReadLine() {
            if (EndOfStream)
                return "";
            else
                return lines[index++];
        }
        
        public void Reset()  { index = 0; }

        public void Close() {  }

        public void Dispose() {  }
    }

    public class CSVDataReader : IDataReader, IDisposable
    {
        private ILineReader reader;
        private char delimiter;

        // Returns an array of header names as string in the order of columns 
        // from left to right of csv file. If CSV file doesn't have header then a dummy header 
        // with 'COL_' + 'column position' will be returned.
        public string[] Header;

        // Returns an array of strings from the current line of csv file. 
        // Is filled by Read() method
        private string[] line;

        private int recordsAffected;

        // Creates an instance of CSV reader
        // <param name="reader">IMyLineReader object</param>
        // <param name="delimiter">delimiter character used in csv file.</param>
        // <param name="firstRowHeader">specify the csv got a header in first row or not. 
        // Default is true and if argument is false then auto header 'ROW_xx will be used as per 
        // the order of columns.</param>
        public CSVDataReader(ILineReader reader, char delimiter = ',', bool firstRowHeader = true)
        {
            this.reader = reader;
            this.delimiter = delimiter;
            if (firstRowHeader == true)
            {
                if (Read())
                    Header = line;
            }
            else
            {
                // read the first line to determine number of columns
                Read();
                Header = line;
                int i = 0;
                var s = "";
                foreach (var item in Header)//read each column and create a dummy header.
                {
                    s = s + "COL_" + i.ToString() + this.delimiter;
                    i++;
                }
                s = s.TrimEnd(this.delimiter);
                Header = ParseRow(s);
                reader.Reset();
            }
            line = null;
            recordsAffected = 0;
        }

        public bool Read()  //: IDataReader
        {
            var result = !reader.EndOfStream;
            if (result == true)
            {
                var s = reader.ReadLine();
                line = ParseRow(s);
                recordsAffected++;
            }
            return result;
        }

        // Parse a row of data from a CSV file
        private string[] ParseRow(string line)
        {
            line = line.Replace($"\\{delimiter}", "\x0001");
            var result = line.Split(delimiter);
            for (int i=0; i<result.Length; i++)
                result[i] = result[i].Replace("\x0001", $"{delimiter}");
            return result;
        }

        public void Close()
        {
            reader.Close();
            reader.Dispose();
            reader = null;
        }

        public int Depth //: IDataReader
        {
            get { return 1; }
        }

        public DataTable GetSchemaTable() //: IDataReader
        {
            DataTable t = new DataTable();
            t.Rows.Add(Header);
            return t;
        }

        public bool IsClosed //: IDataReader
        {
            get { return reader == null; }
        }

        public bool NextResult() //: IDataReader
        {
            return Read();
        }

        // Returns how many records are read so far
        public int RecordsAffected  //: IDataReader
        {
            get { return recordsAffected; }
        }

        public void Dispose() //: IDisposable
        {
            if (reader != null)
            {
                reader.Dispose();
                reader = null;
            }
        }

        public int FieldCount //: IDataReader
        {
            get { return Header.Length; }
        }

        public bool GetBoolean(int i)  //: IDataRecord
        {
            return Boolean.Parse(line[i]);
        }

        public byte GetByte(int i)  //: IDataRecord
        {
            return Byte.Parse(line[i]);
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)  //: IDataRecord
        {
            throw new NotImplementedException();
        }

        public char GetChar(int i)  //: IDataRecord
        {
            return Char.Parse(line[i]);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)  //: IDataRecord
        {
            throw new NotImplementedException();
        }

        public IDataReader GetData(int i)  //: IDataRecord
        {
            return (IDataReader)this;
        }

        public string GetDataTypeName(int i)  //: IDataRecord
        {
            throw new NotImplementedException();
        }

        public DateTime GetDateTime(int i)  //: IDataRecord
        {
            return DateTime.Parse(line[i]);
        }

        public decimal GetDecimal(int i)  //: IDataRecord
        {
            return Decimal.Parse(line[i]);
        }

        public double GetDouble(int i)  //: IDataRecord
        {
            return Double.Parse(line[i]);
        }

        public Type GetFieldType(int i)  //: IDataRecord
        {
            return typeof(String);
        }

        public float GetFloat(int i)  //: IDataRecord
        {
            return float.Parse(line[i]);
        }

        public Guid GetGuid(int i)  //: IDataRecord
        {
            return Guid.Parse(line[i]);
        }

        public short GetInt16(int i)  //: IDataRecord
        {
            return Int16.Parse(line[i]);
        }

        public int GetInt32(int i)  //: IDataRecord
        {
            return Int32.Parse(line[i]);
        }

        public long GetInt64(int i)  //: IDataRecord
        {
            return Int64.Parse(line[i]);
        }

        public string GetName(int i)  //: IDataRecord
        {
            return Header[i];
        }

        public int GetOrdinal(string name)  //: IDataRecord
        {
            int result = -1;
            for (int i = 0; i < Header.Length; i++)
                if (Header[i] == name)
                {
                    result = i;
                    break;
                }
            return result;
        }

        public string GetString(int i)  //: IDataRecord
        {
            return line[i];
        }

        public object GetValue(int i)  //: IDataRecord
        {
            return line[i];
        }

        public int GetValues(object[] values)  //: IDataRecord
        {
            values = line;
            return 1;
        }

        public bool IsDBNull(int i)  //: IDataRecord
        {
            return string.IsNullOrWhiteSpace(line[i]);
        }

        public object this[string name]  //: IDataRecord
        {
            get { return line[GetOrdinal(name)]; }
        }

        public object this[int i]  //: IDataRecord
        {
            get { return GetValue(i); }
        }

    }
}
