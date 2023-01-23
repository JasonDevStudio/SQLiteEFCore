using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using HDF.PInvoke;
using HDF5.NET;
using HDF5CSharp;
using HDF5CSharp.DataTypes;
using Spectre.Console;
using static HDF.PInvoke.H5F;

namespace HDF5.ConsoleTest
{
    internal class Program
    {
        public static int RowCount = 50000000;
        public static int ParaCount { get; set; } = 200;

        static void Main(string[] args)
        {
            WriteLongTable();
        }

        static void WriteLongTable1()
        {
            AnsiConsole.Write(new FigletText("HDF5 Lib Test").Centered().Color(Color.Red));
            var file = Path.Combine(@"C:\Users\jiede\Documents\HDF5", $"{DateTime.Now:yyyyMMddHHmmss}.H5");
            var fileId = Hdf5.CreateFile(file);
            var groupId = Hdf5.CreateOrOpenGroup(fileId, "PARA_OBJECT");
            AnsiConsole.Write(new Rule($"[White] {DateTime.Now} Start Processing Data[/]").Centered());

            var st = Stopwatch.StartNew();
            for (int i = 0; i < ParaCount; i++)
            {
                var values = new double[RowCount];
                for (int j = 0; j < RowCount; j++)
                {
                    values[j] = 314.915289854656478636 / (j + 1) / 1000000;
                }

                Hdf5.WriteDataset(groupId, $"PARA_{i}_OBJECT", values);
            }
             
            st.Stop();
            Hdf5.CloseFile(fileId);
            AnsiConsole.Write(new Rule($"[White] times {st.Elapsed.TotalSeconds} s[/]").Centered());
            AnsiConsole.Ask<string>("Input any key exit;");
        }

        static void WriteLongTable()
        {
            AnsiConsole.Write(new FigletText("HDF5 Lib Test").Centered().Color(Color.Red));
            var file = Path.Combine(@"C:\Users\jiede\Documents\HDF5", $"{DateTime.Now:yyyyMMddHHmmss}.H5"); 
            var data = new List<double[]>(ParaCount);
            AnsiConsole.Write(new Rule($"[White] {DateTime.Now} Start Processing Data[/]").Centered());

            var st = Stopwatch.StartNew();
            for (int i = 0; i < ParaCount; i++)
            {
                var values = new double[RowCount];
                for (int j = 0; j < RowCount; j++)
                {
                    values[j] = 314.915289854656478636 / (j + 1) / 1000000;
                }

                data.Add(values); 
            }

            st.Stop();
            AnsiConsole.Write(new Rule($"[White] {DateTime.Now} End Processing Data，times {st.Elapsed.TotalSeconds} s...[/]").Centered());
            AnsiConsole.Write(new Rule($"[White] {DateTime.Now} Write Data[/]").Centered());
            st.Restart();
            var fileId = Hdf5.CreateFile(file);
            var groupId = Hdf5.CreateOrOpenGroup(fileId, "PARA_OBJECT");
            for (int i = 0; i < data.Count; i++)
            {
                Hdf5.WriteDataset(groupId, $"PARA_{i}_OBJECT", data[i]); 
            }
             
            st.Stop();
            Hdf5.CloseGroup(groupId); 
            Hdf5.CloseFile(fileId);
            AnsiConsole.Write(new Rule($"[White] times {st.Elapsed.TotalSeconds} s[/]").Centered());
            AnsiConsole.Ask<string>("Input any key exit;");
        }

        static void WriteDataset()
        {
            AnsiConsole.Write(new FigletText("HDF5 Lib Test").Centered().Color(Color.Red));
            var file = Path.Combine(@"C:\Users\jiede\Documents\HDF5", $"{DateTime.Now:yyyyMMddHHmmss}.H5");

            AnsiConsole.Write(new Rule($"[White] {DateTime.Now} Write Data[/]").Centered());
            var st = Stopwatch.StartNew();
            var fileId = Hdf5.CreateFile(file);
            var groupId = Hdf5.CreateOrOpenGroup(fileId, $"PARA_DETAIL");
            var dataset = new double[RowCount, ParaCount];
            for (int i = 0; i < RowCount; i++)
            {
                for (int j = 0; j < ParaCount; j++)
                    dataset[i, j] = 314.91526 / (j + 3) * (i + 5);
            }

            Hdf5.WriteDataset(groupId, "PARA_DETAIL_TEST", dataset);

            var dset = Hdf5.ReadDataset<double>(groupId, "PARA_DETAIL_TEST", 0, 1);

            st.Stop();

            Hdf5.CloseFile(fileId);

            AnsiConsole.Write(new Rule($"[White] times {st.Elapsed.TotalSeconds} s[/]").Centered());
        }

        static void WriteLargeCompounds()
        {
            AnsiConsole.Write(new FigletText("HDF5 Lib Test").Centered().Color(Color.Red));
            var file = Path.Combine(@"C:\Users\jiede\Documents\HDF5", $"{DateTime.Now:yyyyMMddHHmmss}.H5");

            AnsiConsole.Write(new Rule($"[White] {DateTime.Now} Write Data[/]").Centered());
            var st = Stopwatch.StartNew();
            var fileId = Hdf5.CreateFile(file);
            var groupId = Hdf5.CreateOrOpenGroup(fileId, $"WAFER_DETAIL");
            var attributes = new Dictionary<string, List<string>>();
            var data = new DataValue[RowCount];
            for (int i = 0; i < ParaCount; i++)
            {
                for (int j = 0; j < RowCount; j++)
                {
                    var value = new DataValue(j, $"{314.915289854656478636 / (j + 1) / 1000000}");
                    data[j] = value;
                }

                //Hdf5.WriteDataset(fileId, $"PARA_{i}_OBJECT", values);
                Hdf5.WriteCompounds(groupId, $"PARA_{i}_OBJECT", data, attributes);
            }

            st.Stop();
            Hdf5.CloseFile(fileId);
            AnsiConsole.Write(new Rule($"[White] times {st.Elapsed.TotalSeconds} s[/]").Centered());
            AnsiConsole.Ask<string>("Input any key exit;");
        }

        static void Main666(string[] args)
        {
            int max = 100;
            AnsiConsole.Write(new FigletText("HDF5 Lib Test").Centered().Color(Color.Red));
            var file = Path.Combine(@"C:\Users\jiede\Documents\HDF5", $"{DateTime.Now:yyyyMMddHHmmss}.H5");

            AnsiConsole.Write(new Rule($"[White] {DateTime.Now} Write Data[/]").Centered());
            var st = Stopwatch.StartNew();
            var fileId = Hdf5.CreateFile(file);
            var groupId = Hdf5.CreateOrOpenGroup(fileId, $"PARA_DETAIL");
            var dataset = new double[RowCount, ParaCount];
            for (int i = 0; i < RowCount; i++)
            {
                for (int j = 0; j < ParaCount; j++)
                    dataset[i, j] = 314.91526 / (j + 3) * (i + 5);
            }

            Hdf5.WriteDataset(groupId, "PARA_DETAIL_TEST", dataset);

            st.Stop();

            Hdf5.CloseFile(fileId);

            AnsiConsole.Write(new Rule($"[White] times {st.Elapsed.TotalSeconds} s[/]").Centered());
        }

        static void MainObject(string[] args)
        {
            int max = 500000;
            AnsiConsole.Write(new FigletText("HDF5 Lib Test").Centered().Color(Color.Red));
            var file = Path.Combine(@"C:\Users\jiede\Documents\HDF5", $"{DateTime.Now:yyyyMMddHHmmss}.H5");

            AnsiConsole.Write(new Rule($"[White] {DateTime.Now} Create Data[/]").Centered());
            var gid = Guid.NewGuid().ToString();

            AnsiConsole.Write(new Rule().Centered());
            AnsiConsole.Write(new Rule($"[White] {DateTime.Now} Write Data[/]").Centered());
            var st = Stopwatch.StartNew();
            var fileId = Hdf5.CreateFile(file);
            var groupId = Hdf5.CreateOrOpenGroup(fileId, $"WAFER");
            var tests = new List<TestEntity>();

            for (int i = 0; i < max; i++)
            {
                var enty = new TestEntity
                {
                    WaferId = $"WAFER_{i}",
                    Product = "JI3680",
                    RowIndex = i,
                    RowKey = Guid.NewGuid().ToString(),
                };

                enty.Parater_1 = 3.149256 * (i + 5) / (i + 3);
                enty.Parater_2 = 3.149256 * (i + 5) / (i + 4);
                enty.Parater_3 = 3.149256 * (i + 5) / (i + 5);
                enty.Parater_4 = 3.149256 * (i + 5) / (i + 6);
                enty.Parater_5 = 3.149256 * (i + 5) / (i + 7);
                enty.Parater_6 = 3.149256 * (i + 5) / (i + 8);
                enty.Parater_7 = 3.149256 * (i + 5) / (i + 9);
                enty.Parater_8 = 3.149256 * (i + 5) / (i + 10);
                enty.Parater_9 = 3.149256 * (i + 5) / (i + 11);
                enty.Parater_10 = 3.149256 * (i + 5) / (i + 12);
                enty.Parater_11 = 3.149256 * (i + 5) / (i + 13);
                enty.Parater_12 = 3.149256 * (i + 5) / (i + 14);
                enty.Parater_13 = 3.149256 * (i + 5) / (i + 15);
                enty.Parater_14 = 3.149256 * (i + 5) / (i + 16);
                enty.Parater_15 = 3.149256 * (i + 5) / (i + 17);
                enty.Parater_16 = 3.149256 * (i + 5) / (i + 18);
                enty.Parater_17 = 3.149256 * (i + 5) / (i + 19);
                enty.Parater_18 = 3.149256 * (i + 5) / (i + 20);
                enty.Parater_19 = 3.149256 * (i + 5) / (i + 21);
                enty.Parater_20 = 3.149256 * (i + 5) / (i + 22);
                enty.Parater_21 = 3.149256 * (i + 5) / (i + 23);
                enty.Parater_22 = 3.149256 * (i + 5) / (i + 24);
                enty.Parater_23 = 3.149256 * (i + 5) / (i + 25);
                enty.Parater_24 = 3.149256 * (i + 5) / (i + 26);
                enty.Parater_25 = 3.149256 * (i + 5) / (i + 27);
                enty.Parater_26 = 3.149256 * (i + 5) / (i + 28);
                enty.Parater_27 = 3.149256 * (i + 5) / (i + 29);
                enty.Parater_28 = 3.149256 * (i + 5) / (i + 30);
                enty.Parater_29 = 3.149256 * (i + 5) / (i + 31);
                enty.Parater_30 = 3.149256 * (i + 5) / (i + 32);
                enty.Parater_31 = 3.149256 * (i + 5) / (i + 33);
                enty.Parater_32 = 3.149256 * (i + 5) / (i + 34);
                enty.Parater_33 = 3.149256 * (i + 5) / (i + 35);
                enty.Parater_34 = 3.149256 * (i + 5) / (i + 36);
                enty.Parater_35 = 3.149256 * (i + 5) / (i + 37);
                enty.Parater_36 = 3.149256 * (i + 5) / (i + 38);
                enty.Parater_37 = 3.149256 * (i + 5) / (i + 39);
                enty.Parater_38 = 3.149256 * (i + 5) / (i + 40);
                enty.Parater_39 = 3.149256 * (i + 5) / (i + 41);
                enty.Parater_40 = 3.149256 * (i + 5) / (i + 42);
                enty.Parater_41 = 3.149256 * (i + 5) / (i + 43);
                enty.Parater_42 = 3.149256 * (i + 5) / (i + 44);
                enty.Parater_43 = 3.149256 * (i + 5) / (i + 45);
                enty.Parater_44 = 3.149256 * (i + 5) / (i + 46);
                enty.Parater_45 = 3.149256 * (i + 5) / (i + 47);
                enty.Parater_46 = 3.149256 * (i + 5) / (i + 48);
                enty.Parater_47 = 3.149256 * (i + 5) / (i + 49);
                enty.Parater_48 = 3.149256 * (i + 5) / (i + 50);
                enty.Parater_49 = 3.149256 * (i + 5) / (i + 51);
                enty.Parater_50 = 3.149256 * (i + 5) / (i + 52);
                enty.Parater_51 = 3.149256 * (i + 5) / (i + 53);
                enty.Parater_52 = 3.149256 * (i + 5) / (i + 54);
                enty.Parater_53 = 3.149256 * (i + 5) / (i + 55);
                enty.Parater_54 = 3.149256 * (i + 5) / (i + 56);
                enty.Parater_55 = 3.149256 * (i + 5) / (i + 57);
                enty.Parater_56 = 3.149256 * (i + 5) / (i + 58);
                enty.Parater_57 = 3.149256 * (i + 5) / (i + 59);
                enty.Parater_58 = 3.149256 * (i + 5) / (i + 60);
                enty.Parater_59 = 3.149256 * (i + 5) / (i + 61);
                enty.Parater_60 = 3.149256 * (i + 5) / (i + 62);
                enty.Parater_61 = 3.149256 * (i + 5) / (i + 63);
                enty.Parater_62 = 3.149256 * (i + 5) / (i + 64);
                enty.Parater_63 = 3.149256 * (i + 5) / (i + 65);
                enty.Parater_64 = 3.149256 * (i + 5) / (i + 66);
                enty.Parater_65 = 3.149256 * (i + 5) / (i + 67);
                enty.Parater_66 = 3.149256 * (i + 5) / (i + 68);
                enty.Parater_67 = 3.149256 * (i + 5) / (i + 69);
                enty.Parater_68 = 3.149256 * (i + 5) / (i + 70);
                enty.Parater_69 = 3.149256 * (i + 5) / (i + 71);
                enty.Parater_70 = 3.149256 * (i + 5) / (i + 72);
                enty.Parater_71 = 3.149256 * (i + 5) / (i + 73);
                enty.Parater_72 = 3.149256 * (i + 5) / (i + 74);
                enty.Parater_73 = 3.149256 * (i + 5) / (i + 75);
                enty.Parater_74 = 3.149256 * (i + 5) / (i + 76);
                enty.Parater_75 = 3.149256 * (i + 5) / (i + 77);
                enty.Parater_76 = 3.149256 * (i + 5) / (i + 78);
                enty.Parater_77 = 3.149256 * (i + 5) / (i + 79);
                enty.Parater_78 = 3.149256 * (i + 5) / (i + 80);
                enty.Parater_79 = 3.149256 * (i + 5) / (i + 81);
                enty.Parater_80 = 3.149256 * (i + 5) / (i + 82);
                enty.Parater_81 = 3.149256 * (i + 5) / (i + 83);
                enty.Parater_82 = 3.149256 * (i + 5) / (i + 84);
                enty.Parater_83 = 3.149256 * (i + 5) / (i + 85);
                enty.Parater_84 = 3.149256 * (i + 5) / (i + 86);
                enty.Parater_85 = 3.149256 * (i + 5) / (i + 87);
                enty.Parater_86 = 3.149256 * (i + 5) / (i + 88);
                enty.Parater_87 = 3.149256 * (i + 5) / (i + 89);
                enty.Parater_88 = 3.149256 * (i + 5) / (i + 90);
                enty.Parater_89 = 3.149256 * (i + 5) / (i + 91);
                enty.Parater_90 = 3.149256 * (i + 5) / (i + 92);
                enty.Parater_91 = 3.149256 * (i + 5) / (i + 93);
                enty.Parater_92 = 3.149256 * (i + 5) / (i + 94);
                enty.Parater_93 = 3.149256 * (i + 5) / (i + 95);
                enty.Parater_94 = 3.149256 * (i + 5) / (i + 96);
                enty.Parater_95 = 3.149256 * (i + 5) / (i + 97);
                enty.Parater_96 = 3.149256 * (i + 5) / (i + 98);
                enty.Parater_97 = 3.149256 * (i + 5) / (i + 99);
                enty.Parater_98 = 3.149256 * (i + 5) / (i + 100);
                enty.Parater_99 = 3.149256 * (i + 5) / (i + 101);
                enty.Parater_100 = 3.149256 * (i + 5) / (i + 102);

                Hdf5.WriteObject(groupId, enty, $"{i}");
            }

            st.Stop();

            Hdf5.CloseFile(fileId);

            AnsiConsole.Write(new Rule($"[White] times {st.Elapsed.TotalSeconds} s[/]").Centered());
        }

        static void TestEmptyMemory()
        {
            AnsiConsole.Write(new FigletText("Empty Data Lib Test").Centered().Color(Color.Red));

            AnsiConsole.Write(new Rule($"[White] {DateTime.Now} Create Data[/]").Centered());
            var st = Stopwatch.StartNew();
            var rows = new List<double[]>();
            for (int i = 0; i < RowCount; i++)
            {
                rows.Add(new double[ParaCount]);
            }

            st.Stop();

            AnsiConsole.Write(new Rule($"[White] times {st.Elapsed.TotalSeconds} s[/]").Centered());
            AnsiConsole.Ask<string>("Input any key exit;");
        }
    }

    [Serializable]
    [StructLayout(LayoutKind.Sequential)]
    public struct DataValue
    {
        public DataValue(int index, string value)
        {
            this.RowIndex = index;
            this.Value = value;
        }

        public int RowIndex;

        [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 20)]
        public string Value;
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

    internal class TestEntity
    {
        public int RowIndex { get; set; }
        public string RowKey { get; set; }
        public string Product { get; set; }
        public string WaferId { get; set; }
        public double Parater_1 { get; set; }
        public double Parater_2 { get; set; }
        public double Parater_3 { get; set; }
        public double Parater_4 { get; set; }
        public double Parater_5 { get; set; }
        public double Parater_6 { get; set; }
        public double Parater_7 { get; set; }
        public double Parater_8 { get; set; }
        public double Parater_9 { get; set; }
        public double Parater_10 { get; set; }
        public double Parater_11 { get; set; }
        public double Parater_12 { get; set; }
        public double Parater_13 { get; set; }
        public double Parater_14 { get; set; }
        public double Parater_15 { get; set; }
        public double Parater_16 { get; set; }
        public double Parater_17 { get; set; }
        public double Parater_18 { get; set; }
        public double Parater_19 { get; set; }
        public double Parater_20 { get; set; }
        public double Parater_21 { get; set; }
        public double Parater_22 { get; set; }
        public double Parater_23 { get; set; }
        public double Parater_24 { get; set; }
        public double Parater_25 { get; set; }
        public double Parater_26 { get; set; }
        public double Parater_27 { get; set; }
        public double Parater_28 { get; set; }
        public double Parater_29 { get; set; }
        public double Parater_30 { get; set; }
        public double Parater_31 { get; set; }
        public double Parater_32 { get; set; }
        public double Parater_33 { get; set; }
        public double Parater_34 { get; set; }
        public double Parater_35 { get; set; }
        public double Parater_36 { get; set; }
        public double Parater_37 { get; set; }
        public double Parater_38 { get; set; }
        public double Parater_39 { get; set; }
        public double Parater_40 { get; set; }
        public double Parater_41 { get; set; }
        public double Parater_42 { get; set; }
        public double Parater_43 { get; set; }
        public double Parater_44 { get; set; }
        public double Parater_45 { get; set; }
        public double Parater_46 { get; set; }
        public double Parater_47 { get; set; }
        public double Parater_48 { get; set; }
        public double Parater_49 { get; set; }
        public double Parater_50 { get; set; }
        public double Parater_51 { get; set; }
        public double Parater_52 { get; set; }
        public double Parater_53 { get; set; }
        public double Parater_54 { get; set; }
        public double Parater_55 { get; set; }
        public double Parater_56 { get; set; }
        public double Parater_57 { get; set; }
        public double Parater_58 { get; set; }
        public double Parater_59 { get; set; }
        public double Parater_60 { get; set; }
        public double Parater_61 { get; set; }
        public double Parater_62 { get; set; }
        public double Parater_63 { get; set; }
        public double Parater_64 { get; set; }
        public double Parater_65 { get; set; }
        public double Parater_66 { get; set; }
        public double Parater_67 { get; set; }
        public double Parater_68 { get; set; }
        public double Parater_69 { get; set; }
        public double Parater_70 { get; set; }
        public double Parater_71 { get; set; }
        public double Parater_72 { get; set; }
        public double Parater_73 { get; set; }
        public double Parater_74 { get; set; }
        public double Parater_75 { get; set; }
        public double Parater_76 { get; set; }
        public double Parater_77 { get; set; }
        public double Parater_78 { get; set; }
        public double Parater_79 { get; set; }
        public double Parater_80 { get; set; }
        public double Parater_81 { get; set; }
        public double Parater_82 { get; set; }
        public double Parater_83 { get; set; }
        public double Parater_84 { get; set; }
        public double Parater_85 { get; set; }
        public double Parater_86 { get; set; }
        public double Parater_87 { get; set; }
        public double Parater_88 { get; set; }
        public double Parater_89 { get; set; }
        public double Parater_90 { get; set; }
        public double Parater_91 { get; set; }
        public double Parater_92 { get; set; }
        public double Parater_93 { get; set; }
        public double Parater_94 { get; set; }
        public double Parater_95 { get; set; }
        public double Parater_96 { get; set; }
        public double Parater_97 { get; set; }
        public double Parater_98 { get; set; }
        public double Parater_99 { get; set; }
        public double Parater_100 { get; set; }

    }
}