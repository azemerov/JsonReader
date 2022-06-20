/*
* CSVDataReader - CVS data reader which implements IDataReader interface
* As a parameter it takes IMyLineReader object, can be MyFileReader (read lines from a file) or MyListReader (reads data from a List<string> object)
* Cloned from Joseph Shijo's example -
* https://www.codeproject.com/Tips/1029831/Fast-and-Simple-IDataReader-Implementation-to-Read
*/

using System;
using System.Collections.Generic;
using System.IO;
using System.Data;

namespace Vespa.Data
{

    public interface IMyLineReader
    {
        public bool EndOfStream { get; }
        public string ReadLine();
        public void Close();
        public void Dispose();
    }

    public class MyFileReader: IMyLineReader
    {
        private StreamReader _file;
        public MyFileReader(string filePath)
        {
            _file = File.OpenText(filePath);
        }
        public bool EndOfStream { get {return _file.EndOfStream;} }
        public string ReadLine() {
            if (_file.EndOfStream)
                return "";
            else
                return _file.ReadLine();
        }
        public void Close() { _file.Close(); }
        public void Dispose() { _file.Dispose(); }
    }

    public class MyListReader: IMyLineReader
    {
        private List<string> _lines;
        private int _index;
        public MyListReader(List<string> lines)
        {
            _lines = lines;
            for (int i=0; i<_lines.Count; i++)
                _lines[i] = _lines[i];
            _index = 0;
        }
        public bool EndOfStream { get {return _index >= _lines.Count;} }
        public string ReadLine() {
            if (EndOfStream)
                return "";
            else
                return _lines[_index++];
        }
        public void Close() {  }
        public void Dispose() {  }
    }

    public class CSVDataReader : IDataReader, IDisposable
    {
        private IMyLineReader _reader;
        private char _delimiter;
        /* stores the header and values of csv and also virtual*/
        private string _virtualHeaderString = "", _csvHeaderstring = "", _csvlinestring = "", _virtuallineString = "";
        private bool _firstRowHeader = true;

        private string[] _header;
        
        /// <summary>
        /// Returns an array of header names as string in the order of columns 
        /// from left to right of csv file. If CSV file doesn't have header then a dummy header 
        /// with 'COL_' + 'column position' will be returned. This can be manually renamed calling 
        /// 'RenameCSVHeader'
        /// </summary>
        public string[] Header
        {
            get { return _header; }
        }

        /*
         * The values of header and values must be in same order. So using this collection.
         * This collection stores header key as header name and its related value as value. 
         * When the value of a specific 
         * header is updated the specific key value will be updated. 
         * For Original Header values from the csv file the values will be null. 
         * This is used as a check and identify this is a csv value or not.
         */
        private System.Collections.Specialized.OrderedDictionary headercollection = 
					new System.Collections.Specialized.OrderedDictionary();

        private string[] _line;

        /// <summary>
        /// Returns an array of strings from the current line of csv file. 
        /// Call Read() method to read the next line/record of csv file. 
        /// </summary>
        public string[] Line
        {
            get
            {
                return _line;
            }
        }

        private int recordsaffected;
        private bool _iscolumnlocked = false;

        /// <summary>
        /// Creates an instance of CSV reader
        /// </summary>
        /// <param name="filePath">Path to the csv file.</param>
        /// <param name="delimiter">delimiter character used in csv file.</param>
        /// <param name="firstRowHeader">specify the csv got a header in first row or not. 
        /// Default is true and if argument is false then auto header 'ROW_xx will be used as per 
        /// the order of columns.</param>
        public CSVDataReader(IMyLineReader reader /*string filePath*/, char delimiter = ',', bool firstRowHeader = true)
        {
            //_file = File.OpenText(filePath);
            _reader = reader;
            _delimiter = delimiter;
            _firstRowHeader = firstRowHeader;
            if (_firstRowHeader == true)
            {
                Read();
                _csvHeaderstring = _csvlinestring;
                _header = ReadRow(_csvHeaderstring);
                foreach (var item in _header) //check for duplicate headers and create a header record.
                {
                    if (headercollection.Contains(item) == true)
                        throw new Exception("Duplicate found in CSV header. Cannot create a CSV reader instance with duplicate header");
                    headercollection.Add(item, null);
                }
            }
            else
            {
                //just open and close the file with read of first line to determine how many 
        		//rows are there and then add default rows as  row1,row2 etc to collection.
                Read();
                _csvHeaderstring = _csvlinestring;
                _header = ReadRow(_csvHeaderstring);
                int i = 0;
                _csvHeaderstring = "";
                foreach (var item in _header)//read each column and create a dummy header.
                {
                    headercollection.Add("COL_" + i.ToString(), null);
                    _csvHeaderstring = _csvHeaderstring + "COL_" + i.ToString() + _delimiter;
                    i++;
                }
                _csvHeaderstring.TrimEnd(_delimiter);
                _header = ReadRow(_csvHeaderstring);
                Close(); //close and repoen to get the record position to beginning.
                //_file = File.OpenText(filePath);
                _reader = reader;
            }
            _iscolumnlocked = false; //setting this to false since above read is called 
			//internally during constructor and actual user read() didnot start.
            _csvlinestring = "";
            _line = null;
            recordsaffected = 0;

        }
        public bool Read()
        {
            var result = !_reader.EndOfStream;
            if (result == true)
            {
                _csvlinestring = _reader.ReadLine();
                if (_virtuallineString == "")
                    _line = ReadRow(_csvlinestring);
                else
                    _line = ReadRow(_virtuallineString + _delimiter + _csvlinestring);
                recordsaffected++;
            }
            if (_iscolumnlocked == false)
                _iscolumnlocked = true;
            return result;
        }

        /// <summary>
        /// Adds a new virtual column at the beginning of each row. 
	/// If a virtual column exists then the new one is placed left of the first one. 
	/// Adding virtual column is possible only before read is made.
        /// </summary>
        /// <param name="columnName">Name of the header of column</param>
        /// <param name="value">Value for this column. This will be returned for every row 
	/// for this column until the value for this column is changed through method 
	/// 'UpdateVirtualcolumnValues'</param>
        /// <returns>Success status</returns>
        public bool AddVirtualColumn(string columnName, string value)
        {
            if (value == null)
                return false;
            if (_iscolumnlocked == true)
                throw new Exception("Cannot add new records after Read() is called.");
            if (headercollection.Contains(columnName) == true)
                throw new Exception("Duplicate found in CSV header.	Cannot create a CSV readerinstance with duplicate header");
            headercollection.Add(columnName, value); //add this to main collection so that 
					//we can check for duplicates next time col is added.

            if (_virtualHeaderString == "")
                _virtualHeaderString = columnName;
            else
                _virtualHeaderString = columnName + _delimiter + _virtualHeaderString;
            _header = ReadRow(_virtualHeaderString + _delimiter + _csvHeaderstring);

            if (_virtuallineString == "")
                _virtuallineString = value;
            else
                _virtuallineString = value + _delimiter + _virtuallineString;
            _line = ReadRow(_virtuallineString + _delimiter + _csvlinestring);
            return true;
        }

        /// <summary>
        /// Update the column header. This method must be called before Read() method is called. 
	/// Otherwise it will throw an exception.
        /// </summary>
        /// <param name="columnName">Name of the header of column</param>
        /// <param name="value">Value for this column. This will be returned for every row 
	/// for this column until the value for this column is changed through method 
	/// 'UpdateVirtualcolumnValues'</param>
        /// <returns>Success status</returns>
        public bool RenameCSVHeader(string oldColumnName, string newColumnName)
        {
            if (_iscolumnlocked == true)
                throw new Exception("Cannot update header after Read() is called.");
            if (headercollection.Contains(oldColumnName) == false)
                throw new Exception("CSV header not found. Cannot update.");
            string value = headercollection[oldColumnName] == null ? 
			null : headercollection[oldColumnName].ToString();
            int i = 0;
            foreach (var item in headercollection.Keys) //this collection does no have a position 
			//location property so using this way assuming the key is ordered
            {
                if (item.ToString() == oldColumnName)
                    break;
                i++;
            }
            headercollection.RemoveAt(i);
            headercollection.Insert(i, newColumnName, value);
            if (value == null) //csv header update.
            {
                _csvHeaderstring = _csvHeaderstring.Replace(oldColumnName, newColumnName);
                _header = ReadRow(_virtualHeaderString + _delimiter + _csvHeaderstring);
            }
            else //virtual header update
            {
                _virtualHeaderString = _virtualHeaderString.Replace(oldColumnName, newColumnName);
                _header = ReadRow(_virtualHeaderString + _delimiter + _csvHeaderstring);
            }
            return true;
        }

        /// <summary>
        /// Updates the value of the virtual column if it exists. Else throws exception.
        /// </summary>
        /// <param name="columnName">Name of the header of column</param>
        /// <param name="value">Value for this column. 
        /// This new value will be returned for every row for this column until 
        /// the value for this column is changed again</param>
        /// <returns>Success status</returns>
        public bool UpdateVirtualColumnValue(string columnName, string value)
        {
            if (value == null)
                return false;
            if (headercollection.Contains(columnName) == false)
                throw new Exception("Unable to find the csv header. Cannot update value.");
            if (headercollection.Contains(columnName) == true && headercollection[columnName] == null)
                throw new Exception("Cannot update values for default csv based columns.");
            headercollection[columnName] = value; //add this to main collection so that 
				//we can check for duplicates next time col is added.
            _virtuallineString = "";
            foreach (var item in headercollection.Values) //cannot use string.replace since 
		//values may be duplicated and can update wrong column. So rebuilding the string.
            {
                if (item != null)
                {
                    _virtuallineString = (string)item + _delimiter + _virtuallineString;
                }
            }
            _virtuallineString = _virtuallineString.TrimEnd(',');
            _line = ReadRow(_virtuallineString + _delimiter + _csvlinestring);
            return true;
        }

        /// <summary>
        /// Reads a row of data from a CSV file
        /// </summary>
        /// <returns>array of strings from csv line</returns>
        private string[] ReadRow(string line)
        {
            List<string> lines = new List<string>();
            if (String.IsNullOrEmpty(line) == true)
                return null;

            int pos = 0;
            int rows = 0;
            while (pos < line.Length)
            {
                string value;

                // Special handling for quoted field
                if (line[pos] == '"')
                {
                    // Skip initial quote
                    pos++;

                    // Parse quoted value
                    int start = pos;
                    while (pos < line.Length)
                    {
                        // Test for quote character
                        if (line[pos] == '"')
                        {
                            // Found one
                            pos++;

                            // If two quotes together, keep one
                            // Otherwise, indicates end of value
                            if (pos >= line.Length || line[pos] != '"')
                            {
                                pos--;
                                break;
                            }
                        }
                        pos++;
                    }
                    value = line.Substring(start, pos - start);
                    value = value.Replace("\"\"", "\"");
                }
                else
                {
                    // Parse unquoted value
                    int start = pos;
                    while (pos < line.Length && line[pos] != _delimiter)
                        pos++;
                    value = line.Substring(start, pos - start);
                }
                // Add field to list
                if (rows < lines.Count)
                    lines[rows] = value;
                else
                    lines.Add(value);
                rows++;

                // Eat up to and including next comma
                while (pos < line.Length && line[pos] != _delimiter)
                    pos++;
                if (pos < line.Length)
                    pos++;
            }
            return lines.ToArray();
        }

        public void Close()
        {
            _reader.Close();
            _reader.Dispose();
            _reader = null;
        }

        /// <summary>
        /// Gets a value that indicates the depth of nesting for the current row.
        /// </summary>
        public int Depth
        {
            get { return 1; }
        }

        public DataTable GetSchemaTable()
        {
            DataTable t = new DataTable();
            t.Rows.Add(Header);
            return t;
        }

        public bool IsClosed
        {
            get { return _reader == null; }
        }

        public bool NextResult()
        {
            return Read();
        }

        /// <summary>
        /// Returns how many records read so far.
        /// </summary>
        public int RecordsAffected
        {
            get { return recordsaffected; }
        }

        public void Dispose()
        {
            if (_reader != null)
            {
                _reader.Dispose();
                _reader = null;
            }
        }

        /// <summary>
        /// Gets the number of columns in the current row.
        /// </summary>
        public int FieldCount
        {
            get { return Header.Length; }
        }

        public bool GetBoolean(int i)
        {
            return Boolean.Parse(Line[i]);
        }

        public byte GetByte(int i)
        {
            return Byte.Parse(Line[i]);
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public char GetChar(int i)
        {
            return Char.Parse(Line[i]);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public IDataReader GetData(int i)
        {
            return (IDataReader)this;
        }

        public string GetDataTypeName(int i)
        {
            throw new NotImplementedException();
        }

        public DateTime GetDateTime(int i)
        {
            return DateTime.Parse(Line[i]);
        }

        public decimal GetDecimal(int i)
        {
            return Decimal.Parse(Line[i]);
        }

        public double GetDouble(int i)
        {
            return Double.Parse(Line[i]);
        }

        public Type GetFieldType(int i)
        {
            return typeof(String);
        }

        public float GetFloat(int i)
        {
            return float.Parse(Line[i]);
        }

        public Guid GetGuid(int i)
        {
            return Guid.Parse(Line[i]);
        }

        public short GetInt16(int i)
        {
            return Int16.Parse(Line[i]);
        }

        public int GetInt32(int i)
        {
            return Int32.Parse(Line[i]);
        }

        public long GetInt64(int i)
        {
            return Int64.Parse(Line[i]);
        }

        public string GetName(int i)
        {
            return Header[i];
        }

        public int GetOrdinal(string name)
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

        public string GetString(int i)
        {
            return Line[i];
        }

        public object GetValue(int i)
        {
            return Line[i];
        }

        public int GetValues(object[] values)
        {
            values = Line;
            return 1;
        }

        public bool IsDBNull(int i)
        {
            return string.IsNullOrWhiteSpace(Line[i]);
        }

        public object this[string name]
        {
            get { return Line[GetOrdinal(name)]; }
        }

        public object this[int i]
        {
            get { return GetValue(i); }
        }

    }
}
