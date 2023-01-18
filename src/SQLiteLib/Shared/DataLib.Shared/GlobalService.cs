using Autofac;
using DataLib.Table;
using DataLib.Table.Impl;
using DataLib.Table.Interfaces;
using SQLiteLib;

namespace DataLib;

public static class GlobalService
{ 
    private static ContainerBuilder Builder = new ContainerBuilder();

    private static IContainer Container;

    public static void Registers()
    {
        Builder.RegisterType<DataTable>().As<IDataTable>().InstancePerDependency();
        Builder.RegisterType<DataRowCollection>().As<IDataRowCollection>();
        Builder.RegisterType<DataRow>().As<IDataRow>().InstancePerDependency();
        Builder.RegisterType<DataColumnCollection>().As<IDataColumnCollection>();
        Builder.RegisterType<DataColumn>().As<IDataColumn>().InstancePerDependency();
        Builder.RegisterType<DBContext>().As<IDBContext>().SingleInstance();
        Container = Builder.Build();
    }

    public static T GetService<T>() => Container.Resolve<T>();
}
