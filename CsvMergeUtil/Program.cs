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
            if (!master.Exists) throw new FileNotFoundException("Master", master.FullName);
            var slave = new FileInfo(args[1]);
            if (!slave.Exists) throw new FileNotFoundException("Slave", slave.FullName);
            var output = new FileInfo(args[2]);
            if (!output.Directory.Exists) throw new DirectoryNotFoundException(output.Directory.FullName);

            var keys = new List<string>(args.Skip(3));

            using (var merger = new CsvMergeLib.Merger(
                master.OpenText(),
                slave.OpenText(),
                new StreamWriter(output.OpenWrite()),
                keys))
            {
                merger.Merge();
            }


        }
    }
}
