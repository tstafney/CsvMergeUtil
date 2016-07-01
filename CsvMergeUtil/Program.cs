using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace CsvMergeUtil
{
    class Program
    {
        static void Main(string[] args)
        {
            var master = new FileInfo(args[0]);
            var slave = new FileInfo(args[1]);
            var output = new FileInfo(args[2]);
            var keys = new List<string>(args.Skip(3));

            if (!master.Exists) throw new FileNotFoundException(master.FullName, master.FullName);
            if (!slave.Exists) throw new FileNotFoundException(slave.FullName, master.FullName);
            if (!output.Directory.Exists) throw new DirectoryNotFoundException(output.Directory.FullName);

            using (var merger = new Merger(
                master.OpenText(),
                slave.OpenText(),
                new StreamWriter(output.Open(FileMode.Create, FileAccess.Write, FileShare.Read)),
                keys))
            {
                merger.Merge();
            }
        }
    }
}
