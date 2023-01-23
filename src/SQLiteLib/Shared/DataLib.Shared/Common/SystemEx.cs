using System;
using System.Collections.Generic;
using System.Text;

namespace DataLib;

public static class SystemEx
{
    /// <summary>
    /// Gets the type.
    /// </summary>
    /// <param name="code">The code.</param>
    /// <returns></returns>
    public static Type GetTypeEx(this TypeCode code)
    {
        switch (code)
        {
            case TypeCode.Object:
                return typeof(object);

            case TypeCode.Boolean:
            case TypeCode.SByte:
            case TypeCode.Byte:
            case TypeCode.Int16:
            case TypeCode.UInt16:
            case TypeCode.Int32:
            case TypeCode.UInt32:
            case TypeCode.Int64:
            case TypeCode.UInt64:
                return typeof(int);

            case TypeCode.Single:
            case TypeCode.Double:
            case TypeCode.Decimal:
                return typeof(double);

            case TypeCode.DateTime:
                return typeof(DateTime);
            case TypeCode.String:
            case TypeCode.Char:
            case TypeCode.DBNull:
            case TypeCode.Empty:
            default:
                return typeof(string);
        }
    }

    public static object TryChangeType(this object value, Type type, out object? result)
    {
        result = default;
        if (value == null)
            return false;

        try
        {
            result = Convert.ChangeType(value, type);
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    public static bool TryParseDouble(this object value, out double? result)
    {
        result = default;
        if (value == null)
            return false;

        try
        {
            result = Convert.ToDouble(value);
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    public static bool TryParseInt(this object value, out int? result)
    {
        result = default;
        if (value == null)
            return false;

        try
        {
            result = Convert.ToInt32(value);
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    public static bool TryParseTime(this object value, out DateTime? result)
    {
        result = default;
        if (value == null)
            return false;

        try
        {
            result = Convert.ToDateTime(value);
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    public static bool TryParseBool(this object value, out bool? result)
    {
        result = default;
        if (value == null)
            return false;

        try
        {
            result = Convert.ToBoolean(value);
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }
}
