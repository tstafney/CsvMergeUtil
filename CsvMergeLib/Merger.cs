using CsvHelper;
using CsvHelper.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace CsvMergeLib
{
    public sealed class Merger : IDisposable
    {
        public CsvReader Master { get; private set; }

        public CsvReader Slave { get; private set; }

        public CsvWriter Output { get; private set; }

        public Dictionary<string, int> MasterHeaders { get; private set; }

        public HashSet<string> MergeHeaders { get; private set; }

        public Dictionary<string, int> SlaveHeaders { get; private set; }

        public HashSet<string> KeyColumns { get; private set; }

        public StringComparer StringComparer { get; private set; }

        public Merger(TextReader master, TextReader slave, TextWriter output, IEnumerable<string> keyColumns)
        {
            if (!keyColumns.Any()) throw new InvalidOperationException("KeyColumns required");

            var csvConfiguration = new CsvConfiguration();
            StringComparer = csvConfiguration.IsHeaderCaseSensitive ? StringComparer.InvariantCulture : StringComparer.InvariantCultureIgnoreCase;

            KeyColumns = new HashSet<string>(keyColumns, StringComparer);
            if (KeyColumns.Count != keyColumns.Count()) throw new InvalidOperationException("KeyColumns count mismatch (case sensitivity?)");

            Master = new CsvReader(master, csvConfiguration);
            Master.Read();
            Slave = new CsvReader(slave, csvConfiguration);
            Slave.Read();
            Output = new CsvWriter(output, csvConfiguration);

            var h = Master.FieldHeaders;
            MasterHeaders = new Dictionary<string, int>(StringComparer);
            for (var ii = 0; ii < h.Length; ii++) MasterHeaders[h[ii]] = ii;

            SlaveHeaders = new Dictionary<string, int>(StringComparer);
            h = Slave.FieldHeaders;
            for (var ii = 0; ii < h.Length; ii++) SlaveHeaders[h[ii]] = ii; 

            if (MasterHeaders.Count != Master.FieldHeaders.Length) throw new InvalidOperationException("Master header count mismatch (case sensitivity?)");
            if (SlaveHeaders.Count != Slave.FieldHeaders.Length) throw new InvalidOperationException("Slave header count mismatch (case sensitivity?)");
            if (!KeyColumns.IsSubsetOf(MasterHeaders.Keys)) throw new InvalidOperationException("KeyColumns not found in Master");
            if (!KeyColumns.IsSubsetOf(SlaveHeaders.Keys)) throw new InvalidOperationException("KeyColumns not found in Slave");

            // now pare down our SlaveHeaders to only those in MasterHeaders and NOT key columns
            var toRemove = SlaveHeaders.Keys.Except(MasterHeaders.Keys, StringComparer).Concat(KeyColumns).ToList();
            MergeHeaders = new HashSet<string>(SlaveHeaders.Keys.Except(toRemove, StringComparer));
        }

        public string CreateKey(string[] record)
        {
            var sb = new StringBuilder();
            foreach(var columnName in KeyColumns)
            {
                sb.Append(record[SlaveHeaders[columnName]]).Append("|");
            }
            return sb.ToString();
        }

        public void Merge()
        {
            var slaveRecords = new Dictionary<string, string[]>(StringComparer);
            do
            {
                var record = Slave.CurrentRecord;
                var key = CreateKey(record);
                slaveRecords.Add(key, record);
            } while (Slave.Read());

            foreach (var item in MasterHeaders) Output.WriteField(item.Key);
            Output.NextRecord();
            do
            {
                var master = Master.CurrentRecord;
                var key = CreateKey(master);
                string[] slave;
                if (slaveRecords.TryGetValue(key, out slave))
                {
                    foreach (var slaveCol in MergeHeaders)
                    {
                        master[MasterHeaders[slaveCol]] = slave[SlaveHeaders[slaveCol]];
                    }
                }
                foreach (var item in master) Output.WriteField(item);
                Output.NextRecord();
            } while (Master.Read());
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        /* protected virtual */ // if not sealed!
        void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                    Master.Dispose();
                    Slave.Dispose();
                    Output.Dispose();
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~Merger() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}
