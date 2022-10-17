using System.Collections.Generic;
using System.Diagnostics;
using System.Text.RegularExpressions;
using HDF5.NET;
using HDF5CSharp;
using Spectre.Console;

namespace HDF5.ConsoleTest
{
    internal class Program
    {
        static void Main(string[] args)
        {
            int max = 100;
            AnsiConsole.Write(new FigletText("HDF5 Lib Test").Centered().Color(Color.Red));
            var file = Path.Combine(AppContext.BaseDirectory, "testFile.H5");

            AnsiConsole.Write(new Rule($"[White] {DateTime.Now} Create Data[/]").Centered());
            var gid = Guid.NewGuid().ToString();
            var data = new List<string[]>();
             
            for (int i = 0; i < max; i++)
            {
                var testClass = new string[1000000];
                for (int j = 0; j < testClass.Length; j++)
                {
                    testClass[j] = $"{gid}---{1.1 * i + 2.2 * j + -3.1 * j - 4.2 * i}";
                }
                 
                data.Add(testClass);
            }

            AnsiConsole.Write(new Rule().Centered());
            AnsiConsole.Write(new Rule($"[White] {DateTime.Now} Write Data[/]").Centered());
            var st = Stopwatch.StartNew();
            var fileId = Hdf5.CreateFile(file);

            for (int i = 0; i < max; i++)
            {
                var test = data[i];
                Hdf5.WriteObject(fileId, test, $"{i}");
            }

            st.Stop();

            Hdf5.CloseFile(fileId);

            AnsiConsole.Write(new Rule($"[White] times {st.Elapsed.TotalSeconds} s[/]").Centered()); 
        }
    }

    internal class TestClassWithArray
    {
        public double[] TestDoubles { get; set; }
        public string[] TestStrings { get; set; }
        public int TestInteger { get; set; }
        public double TestDouble { get; set; }
        public bool TestBoolean { get; set; }
        public string TestString { get; set; }
    }
}