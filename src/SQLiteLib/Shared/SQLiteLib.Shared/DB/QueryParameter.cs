using SQLiteLib.Table.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace SQLiteEFCore.Shared.DB
{
    public class QueryParameter
    {
        public IDataColumn DataColumn { get; set; }

        public string Logic { get; set; }
    }
}
