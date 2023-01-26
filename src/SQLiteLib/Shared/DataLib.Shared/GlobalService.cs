using System.Runtime.CompilerServices;
using Autofac;
using DataLib.Table;
using DataLib.Table.Impl;
using DataLib.Table.Interfaces;
using Microsoft.Win32;

namespace DataLib;

public static class GlobalService
{
    public static Action<ContainerBuilder> Register { get; set; }

    private static ContainerBuilder Builder = new ContainerBuilder();

    private static IContainer Container;

    public static void Registers()
    {
        Builder.RegisterType<DataTable>().As<IDataTable>().InstancePerDependency();
        Builder.RegisterType<DataRowCollection>().As<IDataRowCollection>();
        Builder.RegisterType<DataRow>().As<IDataRow>().InstancePerDependency();
        Builder.RegisterType<DataColumnCollection>().As<IDataColumnCollection>();
        Builder.RegisterType<DataColumn>().As<IDataColumn>().InstancePerDependency();
        Register?.Invoke(Builder);
        Container = Builder.Build();
    }

    public static T GetService<T>(object key = null) => key == null ? Container.Resolve<T>() : Container.ResolveKeyed<T>(key);
}
