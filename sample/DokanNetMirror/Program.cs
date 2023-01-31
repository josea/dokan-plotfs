using System;
using System.Linq;
using DokanNet;
using DokanNet.Logging;

namespace DokanNetMirror
{
    internal class Program
    {
        private const string MirrorKey = "-what";
        private const string MountKey = "-where";
        private const string UseUnsafeKey = "-unsafe";

        private static void Main(string[] args)
        {
            try
            {
                var arguments = args
                   .Select(x => x.Split(new char[] { '=' }, 2, StringSplitOptions.RemoveEmptyEntries))
                   .ToDictionary(x => x[0], x => x.Length > 1 ? x[1] as object : true, StringComparer.OrdinalIgnoreCase);

                //var mirrorPath = arguments.ContainsKey(MirrorKey)
                //? arguments[MirrorKey] as string
                //: @"C:\scratch\";
                
                string[] mirrorPaths = { @"c:\scratch\dokan\dir1\", @"c:\scratch\dokan\dir2\" 
                , @"c:\scratch\dokan\d\scratch\dokan\", @"e:\scratch\dokan\"};


                var mountPath = arguments.ContainsKey(MountKey)
                   ? arguments[MountKey] as string
                   : @"N:\";

                var unsafeReadWrite = arguments.ContainsKey(UseUnsafeKey);

                using (var mre = new System.Threading.ManualResetEvent(false))
                using (var mirrorLogger = new ConsoleLogger("[Mirror] "))
                using (var dokanLogger = new ConsoleLogger("[Dokan] "))
                using (var dokan = new Dokan(dokanLogger))
                {
                    Console.CancelKeyPress += (object sender, ConsoleCancelEventArgs e) =>
                    {
                        e.Cancel = true;
                        mre.Set();
                    };

                    Console.WriteLine($"Using unsafe methods: {unsafeReadWrite}");
                    var mirror = //unsafeReadWrite
                        //? new UnsafeMirror(mirrorLogger, mirrorPath)
                        //: 
                        new Mirror(mirrorLogger, mirrorPaths , @"c:\scratch\dokan\");

                    var dokanBuilder = new DokanInstanceBuilder(dokan)
                        .ConfigureLogger(() => dokanLogger)
                        .ConfigureOptions(options =>
                        {
                            options.Options = DokanOptions.DebugMode | DokanOptions.EnableNotificationAPI;
                            options.MountPoint = mountPath;
                        });
                    using (var dokanInstance = dokanBuilder.Build(mirror))
                    using (var notify = new Notify(mirrorPaths, mountPath, dokanInstance))
                    {
                        mre.WaitOne();
                    }
                }

                Console.WriteLine(@"Success");
            }
            catch (DokanException ex)
            {
                Console.WriteLine(@"Error: " + ex.Message);
            }
        }
    }
}