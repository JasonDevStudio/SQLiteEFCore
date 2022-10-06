using System;
using System.Collections.Generic;
using System.Text;
using Autofac;
using Autofac.Core;
using SQLiteLib.Table.Impl;
using SQLiteLib.Table.Interfaces;

namespace SQLiteLib
{
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
            Container = Builder.Build();
        }

        public static T GetService<T>() => Container.Resolve<T>();
    }
}
