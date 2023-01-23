using System.Collections;
using System.Linq;
using DataLib;
using DataLib.Table;
using HDF5CSharp;

namespace Hdf5Lib.Table.Impl;

/// <summary>
/// Hdf5QueryFilter
/// </summary>
public class Hdf5QueryFilter : QueryFilter
{
    public override List<int> Filter(Array source)
    {
        switch (this.Logic)
        {
            case LogicMode.Between: return this.Between(source);
            case LogicMode.NotBetween: return this.NotBetween(source);
            case LogicMode.IN: return this.In(source);
            case LogicMode.Equal: return this.Equal(source);
            case LogicMode.NotEqual: return this.NotEqual(source);
            case LogicMode.GreaterThan: return this.GreaterThan(source);
            case LogicMode.GreaterThanOrEqual: return this.GreaterThanOrEqual(source);
            case LogicMode.LessThan: return this.LessThan(source);
            case LogicMode.LessThanOrEqual: return this.LessThanOrEqual(source);
            case LogicMode.IsNotNull: return this.IsNotNull(source);
            case LogicMode.IsNull: return this.IsNull(source);
            default: return new List<int>();
        }
    }

    public List<int> In(Array source)
    {
        var rowIndexs = new List<int>();
        if (this.Value is IList list)
        {
            var type = this.DataColumn.TypeCode.GetTypeEx();

            for (int i = 0; i < source.Length; i++)
            {
                source.GetValue(i).TryChangeType(type, out var val);

                if (list.Contains(val))
                    rowIndexs.Add(i);
            }
        }
        else
        {
            rowIndexs = this.Equal(source);
        }

        return rowIndexs;
    }

    public List<int> Equal(Array source)
    {
        var rowIndexs = new List<int>();
        var type = this.DataColumn.TypeCode.GetTypeEx();

        for (int i = 0; i < source.Length; i++)
        {
            source.GetValue(i).TryChangeType(type, out var val);

            if (this.Value.Equals(val))
                rowIndexs.Add(i);
        }

        return rowIndexs;
    }

    public List<int> NotEqual(Array source)
    {
        var rowIndexs = new List<int>();
        var type = this.DataColumn.TypeCode.GetTypeEx();

        for (int i = 0; i < source.Length; i++)
        {
            source.GetValue(i).TryChangeType(type, out var val);

            if (!this.Value.Equals(val))
                rowIndexs.Add(i);
        }

        return rowIndexs;
    }

    public List<int> GreaterThan(Array source)
    {
        var rowIndexs = new List<int>();

        Action doubleFilter = () =>
        {
            try
            {
                this.Value.TryParseDouble(out var fval);
                for (int i = 0; i < source.Length; i++)
                {
                    source.GetValue(i).TryParseDouble(out var val);

                    if (fval > val)
                        rowIndexs.Add(i);
                }
            }
            catch (Exception)
            {
            }
        };

        Action timeFilter = () =>
        {
            try
            {
                this.Value.TryParseTime(out var fval);
                for (int i = 0; i < source.Length; i++)
                {
                    source.GetValue(i).TryParseTime(out var val);

                    if (fval > val)
                        rowIndexs.Add(i);
                }
            }
            catch (Exception)
            {
            }
        };

        switch (this.DataColumn.TypeCode)
        {
            case TypeCode.Int16:
            case TypeCode.UInt16:
            case TypeCode.Int32:
            case TypeCode.UInt32:
            case TypeCode.Int64:
            case TypeCode.UInt64:
            case TypeCode.Single:
            case TypeCode.Double:
            case TypeCode.Decimal:
                doubleFilter();
                break;
            case TypeCode.DateTime:
                timeFilter();
                break;
            default:
                break;
        }

        return rowIndexs;
    }

    public List<int> GreaterThanOrEqual(Array source)
    {
        var rowIndexs = new List<int>();

        Action doubleFilter = () =>
        {
            try
            {
                this.Value.TryParseDouble(out var fval);
                for (int i = 0; i < source.Length; i++)
                {
                    source.GetValue(i).TryParseDouble(out var val);

                    if (fval >= val)
                        rowIndexs.Add(i);
                }
            }
            catch (Exception)
            {
            }
        };

        Action timeFilter = () =>
        {
            try
            {
                this.Value.TryParseTime(out var fval);
                for (int i = 0; i < source.Length; i++)
                {
                    source.GetValue(i).TryParseTime(out var val);

                    if (fval >= val)
                        rowIndexs.Add(i);
                }
            }
            catch (Exception)
            {
            }
        };

        switch (this.DataColumn.TypeCode)
        {
            case TypeCode.Int16:
            case TypeCode.UInt16:
            case TypeCode.Int32:
            case TypeCode.UInt32:
            case TypeCode.Int64:
            case TypeCode.UInt64:
            case TypeCode.Single:
            case TypeCode.Double:
            case TypeCode.Decimal:
                doubleFilter();
                break;
            case TypeCode.DateTime:
                timeFilter();
                break;
            default:
                break;
        }

        return rowIndexs;
    }

    public List<int> LessThan(Array source)
    {
        var rowIndexs = new List<int>();

        Action doubleFilter = () =>
        {
            try
            {
                this.Value.TryParseDouble(out var fval);
                for (int i = 0; i < source.Length; i++)
                {
                    source.GetValue(i).TryParseDouble(out var val);

                    if (fval < val)
                        rowIndexs.Add(i);
                }
            }
            catch (Exception)
            {
            }
        };

        Action timeFilter = () =>
        {
            try
            {
                this.Value.TryParseTime(out var fval);
                for (int i = 0; i < source.Length; i++)
                {
                    source.GetValue(i).TryParseTime(out var val);

                    if (fval < val)
                        rowIndexs.Add(i);
                }
            }
            catch (Exception)
            {
            }
        };

        switch (this.DataColumn.TypeCode)
        {
            case TypeCode.Int16:
            case TypeCode.UInt16:
            case TypeCode.Int32:
            case TypeCode.UInt32:
            case TypeCode.Int64:
            case TypeCode.UInt64:
            case TypeCode.Single:
            case TypeCode.Double:
            case TypeCode.Decimal:
                doubleFilter();
                break;
            case TypeCode.DateTime:
                timeFilter();
                break;
            default:
                break;
        }

        return rowIndexs;
    }

    public List<int> LessThanOrEqual(Array source)
    {
        var rowIndexs = new List<int>();

        Action doubleFilter = () =>
        {
            try
            {
                this.Value.TryParseDouble(out var fval);
                for (int i = 0; i < source.Length; i++)
                {
                    source.GetValue(i).TryParseDouble(out var val);

                    if (fval <= val)
                        rowIndexs.Add(i);
                }
            }
            catch (Exception)
            {
            }
        };

        Action timeFilter = () =>
        {
            try
            {
                this.Value.TryParseTime(out var fval);
                for (int i = 0; i < source.Length; i++)
                {
                    source.GetValue(i).TryParseTime(out var val);

                    if (fval <= val)
                        rowIndexs.Add(i);
                }
            }
            catch (Exception)
            {
            }
        };

        switch (this.DataColumn.TypeCode)
        {
            case TypeCode.Int16:
            case TypeCode.UInt16:
            case TypeCode.Int32:
            case TypeCode.UInt32:
            case TypeCode.Int64:
            case TypeCode.UInt64:
            case TypeCode.Single:
            case TypeCode.Double:
            case TypeCode.Decimal:
                doubleFilter();
                break;
            case TypeCode.DateTime:
                timeFilter();
                break;
            default:
                break;
        }

        return rowIndexs;
    }

    public List<int> Between(Array source)
    {
        var rowIndexs = new List<int>();

        Action doubleFilter = () =>
        {
            try
            {
                if (this.Value is IList vals && vals.Count == 2)
                {
                    vals[0].TryParseDouble(out var start);
                    vals[1].TryParseDouble(out var end);
                    for (int i = 0; i < source.Length; i++)
                    {
                        source.GetValue(i).TryParseDouble(out var val);

                        if (start <= val && end >= val)
                            rowIndexs.Add(i);
                    }
                }
            }
            catch (Exception)
            {
            }
        };

        Action timeFilter = () =>
        {
            try
            {
                if (this.Value is IList vals && vals.Count == 2)
                {
                    vals[0].TryParseTime(out var start);
                    vals[1].TryParseTime(out var end);
                    for (int i = 0; i < source.Length; i++)
                    {
                        source.GetValue(i).TryParseTime(out var val);

                        if (start <= val && end >= val)
                            rowIndexs.Add(i);
                    }
                }
            }
            catch (Exception)
            {
            }
        };

        switch (this.DataColumn.TypeCode)
        {
            case TypeCode.Int16:
            case TypeCode.UInt16:
            case TypeCode.Int32:
            case TypeCode.UInt32:
            case TypeCode.Int64:
            case TypeCode.UInt64:
            case TypeCode.Single:
            case TypeCode.Double:
            case TypeCode.Decimal:
                doubleFilter();
                break;
            case TypeCode.DateTime:
                timeFilter();
                break;
            default:
                break;
        }

        return rowIndexs;
    }

    public List<int> NotBetween(Array source)
    {
        var rowIndexs = new List<int>();

        Action doubleFilter = () =>
        {
            try
            {
                if (this.Value is IList vals && vals.Count == 2)
                {
                    vals[0].TryParseDouble(out var start);
                    vals[1].TryParseDouble(out var end);
                    for (int i = 0; i < source.Length; i++)
                    {
                        source.GetValue(i).TryParseDouble(out var val);

                        if (start >= val || end <= val)
                            rowIndexs.Add(i);
                    }
                }
            }
            catch (Exception)
            {
            }
        };

        Action timeFilter = () =>
        {
            try
            {
                if (this.Value is IList vals && vals.Count == 2)
                {
                    vals[0].TryParseTime(out var start);
                    vals[1].TryParseTime(out var end);
                    for (int i = 0; i < source.Length; i++)
                    {
                        source.GetValue(i).TryParseTime(out var val);

                        if (start <= val && end >= val)
                            rowIndexs.Add(i);
                    }
                }
            }
            catch (Exception)
            {
            }
        };

        switch (this.DataColumn.TypeCode)
        {
            case TypeCode.Int16:
            case TypeCode.UInt16:
            case TypeCode.Int32:
            case TypeCode.UInt32:
            case TypeCode.Int64:
            case TypeCode.UInt64:
            case TypeCode.Single:
            case TypeCode.Double:
            case TypeCode.Decimal:
                doubleFilter();
                break;
            case TypeCode.DateTime:
                timeFilter();
                break;
            default:
                break;
        }

        return rowIndexs;
    }

    public List<int> IsNull(Array source)
    {
        var rowIndexs = new List<int>();

        for (int i = 0; i < source.Length; i++)
        {
            if (source.GetValue(i) == null)
                rowIndexs.Add(i);
        }

        return rowIndexs;
    }

    public List<int> IsNotNull(Array source)
    {
        var rowIndexs = new List<int>();

        for (int i = 0; i < source.Length; i++)
        {
            if (source.GetValue(i) != null)
                rowIndexs.Add(i);
        }

        return rowIndexs;
    }

    public override string ToString()
    {
        throw new NotImplementedException();
    }
}
