//******************************************************************************************************
//  Schema.cs - Gbtc
//
//  Copyright © 2012, Grid Protection Alliance.  All Rights Reserved.
//
//  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements. See
//  the NOTICE file distributed with this work for additional information regarding copyright ownership.
//  The GPA licenses this file to you under the MIT License (MIT), the "License"; you may
//  not use this file except in compliance with the License. You may obtain a copy of the License at:
//
//      http://www.opensource.org/licenses/MIT
//
//  Unless agreed to in writing, the subject software distributed under the License is distributed on an
//  "AS-IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. Refer to the
//  License for the specific language governing permissions and limitations.
//
//  Code Modification History:
//  ----------------------------------------------------------------------------------------------------
//  06/28/2010 - J. Ritchie Carroll
//       Generated original version of source code.
//  08/15/2008 - Mihir Brahmbhatt
//       Converted to C# extensions.
//  09/27/2010 - Mihir Brahmbhatt
//       Edited code comments.
//  12/07/2010 - Mihir Brahmbhatt
//       Changed SqlEncoded method to check proper numeric conversion value
//  12/20/2012 - Starlynn Danyelle Gilliam
//       Modified Header.
//  12/13/2019 - J. Ritchie Carroll
//      Migrated to Gemstone libraries.
//
//******************************************************************************************************

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;
using Gemstone.Data.DataExtensions;

#pragma warning disable CS8603
#pragma warning disable CS8604
#pragma warning disable CS8618
#pragma warning disable CS8625

// ReSharper disable InconsistentNaming
namespace Gemstone.Data;
// James Ritchie Carroll - 2003

#region [ Enumerations ]

/// <summary>Specifies the data type of a field, a property.</summary>
/// <remarks>
/// Copied from <c>System.Data.OleDb.OleDbParameter</c> in the .NET Framework.
/// </remarks>
public enum OleDbType
{
    /// <summary>No value (DBTYPE_EMPTY).</summary>
    Empty = 0,

    /// <summary>A 16-bit signed integer (DBTYPE_I2). This maps to <see cref="T:System.Int16" />.</summary>
    SmallInt = 2,

    /// <summary>A 32-bit signed integer (DBTYPE_I4). This maps to <see cref="T:System.Int32" />.</summary>
    Integer = 3,

    /// <summary>A floating-point number within the range of -3.40E +38 through 3.40E +38 (DBTYPE_R4). This maps to <see cref="T:System.Single" />.</summary>
    Single = 4,

    /// <summary>A floating-point number within the range of -1.79E +308 through 1.79E +308 (DBTYPE_R8). This maps to <see cref="T:System.Double" />.</summary>
    Double = 5,

    /// <summary>A currency value ranging from -2 63 (or -922,337,203,685,477.5808) to 2 63 -1 (or +922,337,203,685,477.5807) with an accuracy to a ten-thousandth of a currency unit (DBTYPE_CY). This maps to <see cref="T:System.Decimal" />.</summary>
    Currency = 6,

    /// <summary>Date data, stored as a double (DBTYPE_DATE). The whole portion is the number of days since December 30, 1899, and the fractional portion is a fraction of a day. This maps to <see cref="T:System.DateTime" />.</summary>
    Date = 7,

    /// <summary>A null-terminated character string of Unicode characters (DBTYPE_BSTR). This maps to <see cref="T:System.String" />.</summary>
    BSTR = 8,

    /// <summary>A pointer to an <see langword="IDispatch" /> interface (DBTYPE_IDISPATCH). This maps to <see cref="T:System.Object" />.</summary>
    IDispatch = 9,

    /// <summary>A 32-bit error code (DBTYPE_ERROR). This maps to <see cref="T:System.Exception" />.</summary>
    Error = 10, // 0x0000000A

    /// <summary>A Boolean value (DBTYPE_BOOL). This maps to <see cref="T:System.Boolean" />.</summary>
    Boolean = 11, // 0x0000000B

    /// <summary>A special data type that can contain numeric, string, binary, or date data, and also the special values Empty and Null (DBTYPE_VARIANT). This type is assumed if no other is specified. This maps to <see cref="T:System.Object" />.</summary>
    Variant = 12, // 0x0000000C

    /// <summary>A pointer to an <see langword="IUnknown" /> interface (DBTYPE_UNKNOWN). This maps to <see cref="T:System.Object" />.</summary>
    IUnknown = 13, // 0x0000000D

    /// <summary>A fixed precision and scale numeric value between -10 38 -1 and 10 38 -1 (DBTYPE_DECIMAL). This maps to <see cref="T:System.Decimal" />.</summary>
    Decimal = 14, // 0x0000000E

    /// <summary>A 8-bit signed integer (DBTYPE_I1). This maps to <see cref="T:System.SByte" />.</summary>
    TinyInt = 16, // 0x00000010

    /// <summary>A 8-bit unsigned integer (DBTYPE_UI1). This maps to <see cref="T:System.Byte" />.</summary>
    UnsignedTinyInt = 17, // 0x00000011

    /// <summary>A 16-bit unsigned integer (DBTYPE_UI2). This maps to <see cref="T:System.UInt16" />.</summary>
    UnsignedSmallInt = 18, // 0x00000012

    /// <summary>A 32-bit unsigned integer (DBTYPE_UI4). This maps to <see cref="T:System.UInt32" />.</summary>
    UnsignedInt = 19, // 0x00000013

    /// <summary>A 64-bit signed integer (DBTYPE_I8). This maps to <see cref="T:System.Int64" />.</summary>
    BigInt = 20, // 0x00000014

    /// <summary>A 64-bit unsigned integer (DBTYPE_UI8). This maps to <see cref="T:System.UInt64" />.</summary>
    UnsignedBigInt = 21, // 0x00000015

    /// <summary>A 64-bit unsigned integer representing the number of 100-nanosecond intervals since January 1, 1601 (DBTYPE_FILETIME). This maps to <see cref="T:System.DateTime" />.</summary>
    Filetime = 64, // 0x00000040

    /// <summary>A globally unique identifier (or GUID) (DBTYPE_GUID). This maps to <see cref="T:System.Guid" />.</summary>
    Guid = 72, // 0x00000048

    /// <summary>A stream of binary data (DBTYPE_BYTES). This maps to an <see cref="T:System.Array" /> of type <see cref="T:System.Byte" />.</summary>
    Binary = 128, // 0x00000080

    /// <summary>A character string (DBTYPE_STR). This maps to <see cref="T:System.String" />.</summary>
    Char = 129, // 0x00000081

    /// <summary>A null-terminated stream of Unicode characters (DBTYPE_WSTR). This maps to <see cref="T:System.String" />.</summary>
    WChar = 130, // 0x00000082

    /// <summary>An exact numeric value with a fixed precision and scale (DBTYPE_NUMERIC). This maps to <see cref="T:System.Decimal" />.</summary>
    Numeric = 131, // 0x00000083

    /// <summary>Date data in the format yyyymmdd (DBTYPE_DBDATE). This maps to <see cref="T:System.DateTime" />.</summary>
    DBDate = 133, // 0x00000085

    /// <summary>Time data in the format hhmmss (DBTYPE_DBTIME). This maps to <see cref="T:System.TimeSpan" />.</summary>
    DBTime = 134, // 0x00000086

    /// <summary>Data and time data in the format yyyymmddhhmmss (DBTYPE_DBTIMESTAMP). This maps to <see cref="T:System.DateTime" />.</summary>
    DBTimeStamp = 135, // 0x00000087

    /// <summary>An automation PROPVARIANT (DBTYPE_PROP_VARIANT). This maps to <see cref="T:System.Object" />.</summary>
    PropVariant = 138, // 0x0000008A

    /// <summary>A variable-length numeric value (<see cref="T:System.Data.OleDb.OleDbParameter" /> only). This maps to <see cref="T:System.Decimal" />.</summary>
    VarNumeric = 139, // 0x0000008B

    /// <summary>A variable-length stream of non-Unicode characters (<see cref="T:System.Data.OleDb.OleDbParameter" /> only). This maps to <see cref="T:System.String" />.</summary>
    VarChar = 200, // 0x000000C8

    /// <summary>A long string value (<see cref="T:System.Data.OleDb.OleDbParameter" /> only). This maps to <see cref="T:System.String" />.</summary>
    LongVarChar = 201, // 0x000000C9

    /// <summary>A variable-length, null-terminated stream of Unicode characters (<see cref="T:System.Data.OleDb.OleDbParameter" /> only). This maps to <see cref="T:System.String" />.</summary>
    VarWChar = 202, // 0x000000CA

    /// <summary>A long null-terminated Unicode string value (<see cref="T:System.Data.OleDb.OleDbParameter" /> only). This maps to <see cref="T:System.String" />.</summary>
    LongVarWChar = 203, // 0x000000CB

    /// <summary>A variable-length stream of binary data (<see cref="T:System.Data.OleDb.OleDbParameter" /> only). This maps to an <see cref="T:System.Array" /> of type <see cref="T:System.Byte" />.</summary>
    VarBinary = 204, // 0x000000CC

    /// <summary>A long binary value (<see cref="T:System.Data.OleDb.OleDbParameter" /> only). This maps to an <see cref="T:System.Array" /> of type <see cref="T:System.Byte" />.</summary>
    LongVarBinary = 205, // 0x000000CD
}

/// <summary>
/// Specifies the type of object in database
/// </summary>
[Flags]
[Serializable]
public enum TableType
{
    /// <summary>
    /// Database object is DataTable
    /// </summary>
    Table = 1,

    /// <summary>
    /// Database object is View
    /// </summary>
    View = 2,

    /// <summary>
    /// Database object is System Defined Table
    /// </summary>
    SystemTable = 4,

    /// <summary>
    /// Database object is System Defined View
    /// </summary>
    SystemView = 8,

    /// <summary>
    /// Database object is Alias
    /// </summary>
    Alias = 16,

    /// <summary>
    /// Database object is Synonym
    /// </summary>
    Synonym = 32,

    /// <summary>
    /// Database object is Global Temp 
    /// </summary>
    GlobalTemp = 64,

    /// <summary>
    /// Database object is local Temp
    /// </summary>
    LocalTemp = 128,

    /// <summary>
    /// Database object is Link
    /// </summary>
    Link = 256,

    /// <summary>
    /// Database object is not defined
    /// </summary>
    Undetermined = 512
}

/// <summary>
/// Specified the type of referential action on database object/Tables
/// </summary>
[Serializable]
public enum ReferentialAction
{
    /// <summary>
    /// Action Type is cascade
    /// </summary>
    Cascade,

    /// <summary>
    /// Action Type is to set null
    /// </summary>
    SetNull,

    /// <summary>
    /// Action Type is to set default
    /// </summary>
    SetDefault,

    /// <summary>
    /// No Action
    /// </summary>
    NoAction
}

#endregion

/// <summary>
/// Represents a database field.
/// </summary>
[Serializable]
public class Field : IComparable
{
    #region [ Members ]

    //Fields
    private string m_name;

    #endregion

    #region [ Constructors ]

    /// <summary>
    /// Creates a new <see cref="Field"/>.
    /// </summary>
    /// <param name="name">Field name.</param>
    /// <param name="type">OLEDB data type for field.</param>
    public Field(string name, OleDbType type)
    {
        // We only allow internal creation of this object
        m_name = name;
        Type = type;
        ForeignKeys = new ForeignKeyFields(this);
    }

    #endregion

    #region [ Properties ]

    /// <summary>
    /// Get Field Name 
    /// </summary>
    public string Name
    {
        get => m_name;
        set
        {
            Parent?.FieldDictionary.Remove(m_name);
            Parent?.FieldDictionary.Add(value, this);
            m_name = value;
        }
    }

    /// <summary>
    /// Get SQL escaped name of <see cref="Table"/>
    /// </summary>
    public string SQLEscapedName => Parent.Parent.Parent.Parent.SQLEscapeName(m_name);

    /// <summary>
    /// Get <see cref="OleDbType"/> Type
    /// </summary>
    public OleDbType Type { get; set; }

    /// <summary>
    /// Get or set file ordinal
    /// </summary>
    public int Ordinal { get; set; }

    /// <summary>
    /// Get or set Allow Null flag
    /// </summary>
    public bool AllowsNulls { get; internal set; }

    /// <summary>
    /// Get or set Auto increment flag
    /// </summary>
    public bool AutoIncrement { get; internal set; }

    /// <summary>
    /// Get or set Auto increment seed
    /// </summary>
    public int AutoIncrementSeed { get; internal set; }

    /// <summary>
    /// Get or set Auto increment step
    /// </summary>
    public int AutoIncrementStep { get; internal set; }

    /// <summary>
    /// Get or set has default value flag
    /// </summary>
    public bool HasDefault { get; internal set; }

    /// <summary>
    /// Get or set default value
    /// </summary>
    public object DefaultValue { get; internal set; }

    /// <summary>
    /// Get or set maximum length of field
    /// </summary>
    public int MaxLength { get; internal set; }

    /// <summary>
    /// Get or set numeric precision
    /// </summary>
    public int NumericPrecision { get; internal set; }

    /// <summary>
    /// Get or set Numeric scale
    /// </summary>
    public int NumericScale { get; internal set; }

    /// <summary>
    /// Get or set date time precision
    /// </summary>
    public int DateTimePrecision { get; internal set; }

    /// <summary>
    /// Get or set read-only flag
    /// </summary>
    public bool ReadOnly { get; internal set; }

    /// <summary>
    /// Get or set for unique
    /// </summary>
    public bool Unique { get; internal set; }

    /// <summary>
    /// Get or set description
    /// </summary>
    public string Description { get; internal set; }

    /// <summary>
    /// Get or set auto increment translation
    /// </summary>
    internal Hashtable AutoIncrementTranslations { get; set; }

    /// <summary>
    /// Get or set value of <see cref="Field"/>
    /// </summary>
    public object Value { get; set; }

    /// <summary>
    /// Get or set flag to check <see cref="Field"/> is primary key or not
    /// </summary>
    public bool IsPrimaryKey { get; set; }

    /// <summary>
    /// Get or set ordinal for Primary key field
    /// </summary>
    public int PrimaryKeyOrdinal { get; set; }

    /// <summary>
    /// Get or set primary key name
    /// </summary>
    public string PrimaryKeyName { get; set; }

    /// <summary>
    /// Get or set list of <see cref="ForeignKeyFields"/>
    /// </summary>
    public ForeignKeyFields ForeignKeys { get; set; }

    /// <summary>
    /// Get or set - check <see cref="Field"/> is reference by
    /// </summary>
    public Field ReferencedBy { get; internal set; }

    /// <summary>
    /// Get or set foreign key flag. if <see cref="Field"/> is <see cref="ReferencedBy"/> then true else false
    /// </summary>
    public bool IsForeignKey => ReferencedBy is not null;

    /// <summary>
    /// Get or set <see cref="Fields"/> parent
    /// </summary>
    public Fields Parent { get; internal set; }

    /// <summary>
    /// Get or set <see cref="Field"/>'s parent <see cref="Table"/>
    /// </summary>
    public Table Table => Parent?.Parent;

    #endregion

    #region [ Methods ]

    /// <summary>
    /// Compare <paramref name="obj"/> ordinal to current field <see cref="Ordinal"/>
    /// </summary>
    /// <param name="obj">Check <paramref name="obj"/> type <see cref="object"/>, if it is type of <see cref="Field"/> then compare to <see cref="Ordinal"/> of <paramref name="obj"/> else throw <see cref="ArgumentException"/></param>
    /// <returns></returns>
    public int CompareTo(object? obj)
    {
        // Fields are sorted in ordinal position order
        if (obj is Field field)
            return Ordinal.CompareTo(field.Ordinal);

        throw new ArgumentException("Field can only be compared to other Fields");
    }

    /// <summary>
    /// Change <see cref="Field"/> value to encoded string. It will check <see cref="Type"/>  and <see cref="Parent"/> value before convert to <see cref="OleDbType"/> compatible value
    /// </summary>
    public string SQLEncodedValue
    {
        get
        {
            string encodedValue = "";

            if (!Convert.IsDBNull(Value))
            {
                try
                {
                    // Attempt to get string based source field value
                    encodedValue = Value.ToString()!.Trim();

                    // Format field value based on field's data type
                    switch (Type)
                    {
                        case OleDbType.BigInt:
                        case OleDbType.Integer:
                        case OleDbType.SmallInt:
                        case OleDbType.TinyInt:
                        case OleDbType.UnsignedBigInt:
                        case OleDbType.UnsignedInt:
                        case OleDbType.UnsignedSmallInt:
                        case OleDbType.UnsignedTinyInt:
                        case OleDbType.Error:
                            if (encodedValue.Length == 0)
                            {
                                encodedValue = Parent.Parent.Parent.Parent.AllowNumericNulls ? "NULL" : "0";
                            }
                            else
                            {
                                if (long.TryParse(Value.ToString(), out _)) //(Information.IsNumeric(Value))
                                    encodedValue = Convert.ToInt64(Value).ToString().Trim();
                                else
                                    encodedValue = Parent.Parent.Parent.Parent.AllowNumericNulls ? "NULL" : "0";
                            }

                            break;
                        case OleDbType.Single:
                            if (encodedValue.Length == 0)
                            {
                                encodedValue = Parent.Parent.Parent.Parent.AllowNumericNulls ? "NULL" : "0.0";
                            }
                            else
                            {
                                if (float.TryParse(Value.ToString(), out _)) //if (Information.IsNumeric(Value))
                                    encodedValue = Convert.ToSingle(Value).ToString(CultureInfo.InvariantCulture).Trim();
                                else
                                    encodedValue = Parent.Parent.Parent.Parent.AllowNumericNulls ? "NULL" : "0.0";
                            }

                            break;
                        case OleDbType.Double:
                            if (encodedValue.Length == 0)
                            {
                                encodedValue = Parent.Parent.Parent.Parent.AllowNumericNulls ? "NULL" : "0.0";
                            }
                            else
                            {
                                if (double.TryParse(Value.ToString(), out _)) //if (Information.IsNumeric(Value))
                                    encodedValue = Convert.ToDouble(Value).ToString(CultureInfo.InvariantCulture).Trim();
                                else
                                    encodedValue = Parent.Parent.Parent.Parent.AllowNumericNulls ? "NULL" : "0.0";
                            }

                            break;
                        case OleDbType.Currency:
                        case OleDbType.Decimal:
                        case OleDbType.Numeric:
                        case OleDbType.VarNumeric:
                            if (encodedValue.Length == 0)
                            {
                                encodedValue = Parent.Parent.Parent.Parent.AllowNumericNulls ? "NULL" : "0.00";
                            }
                            else
                            {
                                if (decimal.TryParse(Value.ToString(), out _)) //if (Information.IsNumeric(Value))
                                    encodedValue = Convert.ToDecimal(Value).ToString(CultureInfo.InvariantCulture).Trim();
                                else
                                    encodedValue = Parent.Parent.Parent.Parent.AllowNumericNulls ? "NULL" : "0.00";
                            }

                            break;
                        case OleDbType.Boolean:
                            if (encodedValue.Length == 0)
                            {
                                encodedValue = Parent.Parent.Parent.Parent.AllowNumericNulls ? "NULL" : "0";
                            }
                            else
                            {
                                if (long.TryParse(Value.ToString(), out long tempValue)) //if (Information.IsNumeric(strValue))
                                {
                                    encodedValue = tempValue == 0 ? "0" : "1";
                                }
                                else
                                {
                                    switch (char.ToUpper(encodedValue.Trim()[0]))
                                    {
                                        case 'Y':
                                        case 'T':
                                            encodedValue = "1";

                                            break;
                                        case 'N':
                                        case 'F':
                                            encodedValue = "0";

                                            break;
                                        default:
                                            encodedValue = Parent.Parent.Parent.Parent.AllowNumericNulls ? "NULL" : "0";

                                            break;
                                    }
                                }
                            }

                            break;
                        case OleDbType.Char:
                        case OleDbType.WChar:
                        case OleDbType.VarChar:
                        case OleDbType.VarWChar:
                        case OleDbType.LongVarChar:
                        case OleDbType.LongVarWChar:
                        case OleDbType.BSTR:
                            if (encodedValue.Length == 0)
                                encodedValue = Parent.Parent.Parent.Parent.AllowTextNulls ? "NULL" : "''";
                            else
                                encodedValue = $"'{encodedValue.SQLEncode(Parent.Parent.Parent.Parent.DataSourceType)}'";

                            break;
                        case OleDbType.DBTimeStamp:
                        case OleDbType.DBDate:
                        case OleDbType.Date:
                            if (encodedValue.Length > 0)
                            {
                                if (DateTime.TryParse(Value.ToString(), out DateTime tempDateTimeValue)) //if (Information.IsDate(strValue))
                                {
                                    switch (Parent.Parent.Parent.Parent.DataSourceType)
                                    {
                                        case DatabaseType.Access:
                                            encodedValue = $"#{tempDateTimeValue.ToString("MM/dd/yyyy HH:mm:ss", CultureInfo.InvariantCulture)}#";

                                            break;
                                        case DatabaseType.Oracle:
                                            encodedValue = $"to_date('{tempDateTimeValue:dd-MMM-yyyy HH:mm:ss}', 'DD-MON-YYYY HH24:MI:SS')";

                                            break;
                                        default:
                                            encodedValue = $"'{tempDateTimeValue:yyyy-MM-dd HH:mm:ss}'";

                                            break;
                                    }
                                }
                                else
                                {
                                    encodedValue = "NULL";
                                }
                            }
                            else
                            {
                                encodedValue = "NULL";
                            }

                            break;
                        case OleDbType.DBTime:
                            if (encodedValue.Length > 0)
                                encodedValue = DateTime.TryParse(Value.ToString(), out DateTime tempDateTimeValue) ? $"'{tempDateTimeValue:HH:mm:ss}'" : "NULL";
                            else
                                encodedValue = "NULL";

                            break;
                        case OleDbType.Filetime:
                            encodedValue = encodedValue.Length > 0 ? $"'{encodedValue}'" : "NULL";

                            break;
                        case OleDbType.Guid:
                            if (encodedValue.Length == 0)
                                encodedValue = new Guid().ToString().ToLower();

                            encodedValue = Parent.Parent.Parent.Parent.DataSourceType == DatabaseType.Access ? $"{{{encodedValue.ToLower()}}}" : $"'{encodedValue.ToLower()}'";

                            break;
                    }
                }
                catch //(Exception ex)
                {
                    // We'll default to NULL if we failed to evaluate field data
                    encodedValue = "NULL";
                }
            }

            if (encodedValue.Length == 0)
                encodedValue = "NULL";

            return encodedValue;
        }
    }

    /// <summary>
    /// Gets the native value for the field (SQL Encoded).
    /// </summary>
    public string NonNullNativeValue
    {
        get
        {
            string encodedValue = "";

            // Format field value based on field's data type
            switch (Type)
            {
                case OleDbType.BigInt:
                case OleDbType.Integer:
                case OleDbType.SmallInt:
                case OleDbType.TinyInt:
                case OleDbType.UnsignedBigInt:
                case OleDbType.UnsignedInt:
                case OleDbType.UnsignedSmallInt:
                case OleDbType.UnsignedTinyInt:
                case OleDbType.Error:
                case OleDbType.Boolean:
                    encodedValue = "0";

                    break;
                case OleDbType.Single:
                case OleDbType.Double:
                case OleDbType.Currency:
                case OleDbType.Decimal:
                case OleDbType.Numeric:
                case OleDbType.VarNumeric:
                    encodedValue = "0.00";

                    break;
                case OleDbType.Char:
                case OleDbType.WChar:
                case OleDbType.VarChar:
                case OleDbType.VarWChar:
                case OleDbType.LongVarChar:
                case OleDbType.LongVarWChar:
                case OleDbType.BSTR:
                case OleDbType.Filetime:
                    encodedValue = "''";

                    break;
                case OleDbType.DBTimeStamp:
                case OleDbType.DBDate:
                case OleDbType.Date:
                    switch (Parent.Parent.Parent.Parent.DataSourceType)
                    {
                        case DatabaseType.Access:
                            encodedValue = $"#{DateTime.UtcNow.ToString("MM/dd/yyyy HH:mm:ss", CultureInfo.InvariantCulture)}#";

                            break;
                        case DatabaseType.Oracle:
                            encodedValue = $"to_date('{DateTime.UtcNow:dd-MMM-yyyy HH:mm:ss}', 'DD-MON-YYYY HH24:MI:SS')";

                            break;
                        default:
                            encodedValue = $"'{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}'";

                            break;
                    }

                    break;
                case OleDbType.DBTime:
                    encodedValue = $"'{DateTime.UtcNow:HH:mm:ss}'";

                    break;
                case OleDbType.Guid:
                    encodedValue = new Guid().ToString().ToLower();
                    if (Parent.Parent.Parent.Parent.DataSourceType == DatabaseType.Access)
                        encodedValue = $"{{{encodedValue}}}";
                    else
                        encodedValue = $"'{encodedValue}'";

                    break;
            }

            return encodedValue;
        }
    }

    /// <summary>
    /// Get information about referential action
    /// </summary>
    /// <param name="action">check <paramref name="action"/> and return to appropriate <see cref="ReferentialAction"/>.</param>
    /// <returns></returns>
    internal static ReferentialAction GetReferentialAction(string action)
    {
        switch (action.Trim().ToUpper())
        {
            case "CASCADE":
                return ReferentialAction.Cascade;
            case "SET NULL":
                return ReferentialAction.SetNull;
            case "SET DEFAULT":
                return ReferentialAction.SetDefault;
            case "NO ACTION":
                return ReferentialAction.NoAction;
            default:
                return ReferentialAction.NoAction;
        }
    }

    #endregion
}

/// <summary>
/// Represents a database foreign key field.
/// </summary>
[Serializable]
public class ForeignKeyField
{
    #region [ Constructors ]

    // We only allow internal creation of this object
    internal ForeignKeyField(ForeignKeyFields parent) => Parent = parent;

    #endregion

    #region [ Properties ]

    /// <summary>
    /// Get foreign key parent information
    /// </summary>
    public ForeignKeyFields Parent { get; private set; }

    /// <summary>
    /// Get or set Primary key field
    /// </summary>
    public Field PrimaryKey { get; set; }

    /// <summary>
    /// Get or set Foreign key field
    /// </summary>
    public Field ForeignKey { get; set; }

    /// <summary>
    /// Get or set ordinal of <see cref="Field"/>
    /// </summary>
    public int Ordinal { get; set; }

    /// <summary>
    /// Get or set name of key
    /// </summary>
    public string KeyName { get; set; }

    /// <summary>
    /// Get or set update rule for <see cref="ReferentialAction"/> for <see cref="Field"/>
    /// </summary>
    public ReferentialAction UpdateRule { get; set; } = ReferentialAction.NoAction;

    /// <summary>
    /// Get or set delete rule for <see cref="ReferentialAction"/> for <see cref="Field"/>
    /// </summary>
    public ReferentialAction DeleteRule { get; set; } = ReferentialAction.NoAction;

    #endregion
}

/// <summary>
/// Represents a collection of <see cref="ForeignKeyField"/> values.
/// </summary>
[Serializable]
public class ForeignKeyFields : IEnumerable
{
    #region [ Constructors ]

    // We only allow internal creation of this object
    internal ForeignKeyFields(Field parent)
    {
        Parent = parent;
        FieldDictionary = new Dictionary<string, ForeignKeyField>(StringComparer.OrdinalIgnoreCase);
        FieldsList = new List<ForeignKeyField>();
    }

    #endregion

    #region [ Properties ]

    /// <summary>
    /// Get or set field parent information
    /// </summary>
    public Field Parent { get; private set; }

    /// <summary>
    /// Get of Set Fields names to lookups
    /// </summary>
    internal Dictionary<string, ForeignKeyField> FieldDictionary { get; private set; }

    /// <summary>
    /// Get or set field indexes to lookups
    /// </summary>
    internal List<ForeignKeyField> FieldsList { get; private set; }

    /// <summary>
    /// Get the current index of foreign key field information
    /// </summary>
    /// <param name="index"></param>
    /// <returns></returns>
    public ForeignKeyField this[int index] => index < 0 || index >= FieldsList.Count ? null : FieldsList[index];

    /// <summary>
    /// Get the current <see cref="ForeignKeyField"/> information by name
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    public ForeignKeyField this[string name]
    {
        get
        {
            FieldDictionary.TryGetValue(name, out ForeignKeyField? lookup);
            return lookup;
        }
    }

    /// <summary>
    /// Get count of <see cref="ForeignKeyFields"/> list
    /// </summary>
    public int Count => FieldsList.Count;

    #endregion

    #region [ Methods ]

    /// <summary>
    /// Get <see cref="IEnumerator"/> of field lists
    /// </summary>
    /// <returns></returns>
    public IEnumerator GetEnumerator() => FieldsList.GetEnumerator();

    /// <summary>
    /// Add a <see cref="ForeignKeyField"/> to list object
    /// </summary>
    /// <param name="newField"><paramref name="newField"/> is type of <see cref="ForeignKeyField"/></param>
    internal void Add(ForeignKeyField newField)
    {
        FieldsList.Add(newField);
        FieldDictionary.Add(newField.KeyName.Length > 0 ? newField.KeyName : $"FK{FieldsList.Count}", newField);
    }

    /// <summary>
    /// Get comma separated <see cref="string"/> of <see cref="ForeignKeyField"/>
    /// </summary>
    /// <returns></returns>
    public string GetList()
    {
        StringBuilder fieldList = new();

        foreach (ForeignKeyField field in FieldsList)
        {
            if (fieldList.Length > 0)
                fieldList.Append(',');

            fieldList.Append('[');
            fieldList.Append(field.ForeignKey.Name);
            fieldList.Append(']');
        }

        return fieldList.ToString();
    }

    #endregion
}

/// <summary>
/// Represents a collection of <see cref="Field"/> values.
/// </summary>
[Serializable]
public class Fields : IEnumerable<Field>
{
    #region [ Constructors ]

    // We only allow internal creation of this object
    internal Fields(Table parent)
    {
        Parent = parent;
        FieldDictionary = new Dictionary<string, Field>(StringComparer.OrdinalIgnoreCase);
        FieldList = new List<Field>();
    }

    #endregion

    #region [ Properties ]

    /// <summary>
    /// Get <see cref="Field"/>'s parent <see cref="Table"/>.
    /// </summary>
    public Table Parent { get; private set; }

    /// <summary>
    /// Get or set to fields lookup 
    /// </summary>
    internal Dictionary<string, Field> FieldDictionary { get; private set; }

    /// <summary>
    /// Get or set Fields index lookup
    /// </summary>
    internal List<Field> FieldList { get; private set; }

    /// <summary>
    /// Indexer property of Field
    /// </summary>
    /// <param name="index"></param>
    /// <returns></returns>
    public Field? this[int index] => index < 0 || index >= FieldList.Count ? null : FieldList[index];

    /// <summary>
    /// Indexer property of Field by Name
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    public Field? this[string name]
    {
        get
        {
            FieldDictionary.TryGetValue(name, out Field? lookup);
            return lookup;
        }
    }

    /// <summary>
    /// Get count of collection of <see cref="Field"/>
    /// </summary>
    public int Count => FieldList.Count;

    #endregion

    #region [ Methods ]

    /// <summary>
    /// Add new <see cref="Field"/> to this collection.
    /// </summary>
    /// <param name="newField">Field to add.</param>
    public void Add(Field newField)
    {
        newField.Parent ??= this;

        FieldDictionary.Add(newField.Name, newField);
        FieldList.Add(newField);
    }

    /// <summary>
    /// Removes <see cref="Field"/> from the collection.
    /// </summary>
    /// <param name="field">Field to remove.</param>
    public void Remove(Field field)
    {
        if (field.Parent == this)
            field.Parent = null;

        FieldDictionary.Remove(field.Name);
        FieldList.Remove(field);
    }

    /// <summary>
    /// Clears the field list.
    /// </summary>
    public void Clear()
    {
        foreach (Field field in FieldList)
        {
            if (field.Parent == this)
                field.Parent = null;
        }

        FieldDictionary.Clear();
        FieldList.Clear();
    }

    /// <summary>
    /// Get <see cref="IEnumerator{Field}"/> type of <see cref="Field"/> list.
    /// </summary>
    /// <returns></returns>
    public IEnumerator<Field> GetEnumerator() => FieldList.GetEnumerator();

    /// <summary>
    /// Get comma separated list of <see cref="Field"/>
    /// </summary>
    /// <param name="returnAutoInc"></param>
    /// <param name="sqlEscapeFunction"></param>
    /// <returns></returns>
    public string GetList(bool returnAutoInc = true, Func<string, string>? sqlEscapeFunction = null)
    {
        sqlEscapeFunction ??= Parent.Parent.Parent.SQLEscapeName;

        StringBuilder fieldList = new();

        foreach (Field field in FieldList.Where(field => !field.AutoIncrement || returnAutoInc))
        {
            if (fieldList.Length > 0)
                fieldList.Append(", ");

            fieldList.Append(sqlEscapeFunction(field.Name));
        }

        return fieldList.ToString();
    }

    /// <summary>
    /// Get <see cref="IEnumerator"/> type of <see cref="Field"/> list.
    /// </summary>
    /// <returns></returns>
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    #endregion
}

/// <summary>
/// Get data table information for data processing
/// </summary>
[Serializable]
public class Table : IComparable, IComparable<Table>
{
    #region [ Members ]

    private Tables m_parent;
    private string m_name;

    #endregion

    #region [ Constructors ]

    /// <summary>
    /// Creates a new <see cref="Table"/>.
    /// </summary>
    public Table() : this(null)
    {
    }

    /// <summary>
    /// Creates a new <see cref="Table"/>.
    /// </summary>
    public Table(string name) : this(null, null, name, TableType.Table.ToString(), null, 0)
    {
    }

    /// <summary>
    /// Creates a new <see cref="Table"/>.
    /// </summary>
    public Table(string catalog, string schema, string name, string type, string description, int rows)
    {
        // We only allow internal creation of this object
        Fields = new Fields(this);

        Catalog = catalog;
        Schema = schema;
        m_name = name;
        MapName = name;
        Description = description;

        switch (type.Trim().ToUpper())
        {
            case "TABLE":
                Type = TableType.Table;

                break;
            case "VIEW":
                Type = TableType.View;

                break;
            case "SYSTEM TABLE":
                Type = TableType.SystemTable;

                break;
            case "SYSTEM VIEW":
                Type = TableType.SystemView;

                break;
            case "ALIAS":
                Type = TableType.Alias;

                break;
            case "SYNONYM":
                Type = TableType.Synonym;

                break;
            case "GLOBAL TEMPORARY":
                Type = TableType.GlobalTemp;

                break;
            case "LOCAL TEMPORARY":
                Type = TableType.LocalTemp;

                break;
            case "LINK":
                Type = TableType.Link;

                break;
            default:
                Type = TableType.Undetermined;

                break;
        }

        RowCount = rows;
        ReevalulateIdentitySQL();
    }

    #endregion

    #region [ Properties ]

    /// <summary>
    /// Get or set list of <see cref="Fields"/> for <see cref="Table"/>
    /// </summary>
    public Fields Fields { get; set; }

    /// <summary>
    /// Get or set name of <see cref="Table"/>
    /// </summary>
    public string MapName { get; set; }

    /// <summary>
    /// Get or set process flag
    /// </summary>
    public bool Process { get; set; }

    /// <summary>
    /// Get or set priority 
    /// </summary>
    public int Priority { get; set; }

    /// <summary>
    /// Get or set identity SQL for <see cref="Table"/>
    /// </summary>
    public string IdentitySQL { get; set; } = "SELECT @@IDENTITY";

    /// <summary>
    /// Get name of <see cref="Table"/>
    /// </summary>
    public string Name
    {
        get => m_name;
        set
        {
            m_parent?.TableDictionary.Remove(m_name);
            m_parent?.TableDictionary.Add(value, this);
            m_name = value;
        }
    }

    /// <summary>
    /// Get SQL escaped name of <see cref="Table"/>
    /// </summary>
    public string SQLEscapedName => m_parent.Parent.SQLEscapeName(m_name);

    /// <summary>
    /// Get or set full name of <see cref="Table"/>
    /// </summary>
    public string FullName
    {
        get
        {
            Schema schema = m_parent.Parent;

            string strFullName = "";

            if (!string.IsNullOrWhiteSpace(Catalog))
                strFullName += $"{schema.SQLEscapeName(Catalog)}.";

            if (!string.IsNullOrWhiteSpace(Schema))
                strFullName += $"{schema.SQLEscapeName(Schema)}.";

            strFullName += schema.SQLEscapeName(m_name);

            return strFullName;
        }
    }

    /// <summary>
    /// Get or set catalog information for <see cref="Table"/>
    /// </summary>
    public string Catalog { get; set; }

    /// <summary>
    /// Get or set schema name
    /// </summary>
    public string Schema { get; set; }

    /// <summary>
    /// Get <see cref="TableType"/>
    /// </summary>
    public TableType Type { get; set; }

    /// <summary>
    /// Get or set description
    /// </summary>
    public string Description { get; set; }

    /// <summary>
    /// Get row count in <see cref="Table"/>
    /// </summary>
    public int RowCount { get; private set; }

    /// <summary>
    /// Get parent <see cref="Table"/> information
    /// </summary>
    public Tables Parent
    {
        get => m_parent;
        internal set
        {
            m_parent = value;
            ReevalulateIdentitySQL();

            if (RowCount == 0)
                CalculateRowCount();
        }
    }

    /// <summary>
    /// Get <see cref="IDbConnection"/> of object
    /// </summary>
    public DbConnection Connection => m_parent.Parent.Connection;

    /// <summary>
    /// Check for object is view
    /// </summary>
    public bool IsView => Type is TableType.View or TableType.SystemView;

    /// <summary>
    /// Check for system tables and system views
    /// </summary>
    public bool IsSystem => Type is TableType.SystemTable or TableType.SystemView;

    /// <summary>
    /// Get flag for <see cref="TableType"/>  for temp
    /// </summary>
    public bool IsTemporary => Type is TableType.GlobalTemp or TableType.LocalTemp;

    /// <summary>
    /// Get flag for <see cref="TableType"/> alias or link
    /// </summary>
    public bool IsLinked => Type is TableType.Alias or TableType.Link;

    /// <summary>
    /// Get count for primary key <see cref="Field"/>
    /// </summary>
    public int PrimaryKeyFieldCount => Fields.Count(field => field.IsPrimaryKey);

    /// <summary>
    /// Get flag that determines if the table has any foreign keys.
    /// </summary>
    public bool ReferencedByForeignKeys => Fields.Any(field => field is { IsPrimaryKey: true, ForeignKeys.Count: > 0 });

    /// <summary>
    /// Get flag of any foreign key <see cref="Field"/>
    /// </summary>
    public bool IsForeignKeyTable => Fields.Any(field => field.IsForeignKey);

    /// <summary>
    /// Gets flag that determines if the <see cref="Table"/> has an auto-increment field.
    /// </summary>
    public bool HasAutoIncField => AutoIncField is not null;

    /// <summary>
    /// Gets auto-increment field for the <see cref="Table"/>, if any.
    /// </summary>
    public Field AutoIncField => Fields.FirstOrDefault(field => field.AutoIncrement);

    #endregion

    #region [ Methods ]

    /// <summary>
    /// Get schema information flag based on <see cref="DatabaseType"/>
    /// </summary>
    /// <returns></returns>
    public bool UsesDefaultSchema()
    {
        if (Parent.Parent.DataSourceType == DatabaseType.SQLServer)
            return string.Compare(Schema, "dbo", StringComparison.OrdinalIgnoreCase) == 0;

        return Schema.Length == 0;
    }

    /// <summary>
    /// Gets display name for table.
    /// </summary>
    public override string ToString() => MapName;

    /// <summary>
    /// Compare <see cref="object"/> type of <paramref name="obj"/> with <see cref="Table"/> object <see cref="Priority"/>
    /// </summary>
    /// <param name="obj"></param>
    /// <returns></returns>
    public int CompareTo(object? obj)
    {
        // Tables are sorted in priority order
        if (obj is Table table)
            return CompareTo(table);

        throw new ArgumentException("Table can only be compared to other Tables");
    }

    /// <summary>
    /// Compare Table with other <see cref="Table"/> object <see cref="Priority"/>
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public int CompareTo(Table? other) => Priority.CompareTo(other?.Priority);

    /// <summary>
    /// Check for reference flag, whether table has reference in another table
    /// </summary>
    /// <param name="otherTable"></param>
    /// <param name="tableStack"></param>
    /// <returns></returns>
    internal bool IsReferencedBy(Table otherTable, List<Table> tableStack)
    {
        Table table;

        tableStack ??= new List<Table>();

        tableStack.Add(this);

        foreach (Field field in Fields)
        {
            foreach (ForeignKeyField foreignKey in field.ForeignKeys)
            {
                // We don't want to circle back on ourselves
                table = foreignKey.ForeignKey.Table;
                bool tableIsInStack = tableStack.Exists(tbl => string.Compare(tbl.Name, table.Name, StringComparison.OrdinalIgnoreCase) == 0);

                if (tableIsInStack)
                {
                    if (string.Compare(table.Name, otherTable.Name, StringComparison.OrdinalIgnoreCase) == 0)
                        return true;
                }
                else
                {
                    if (table.IsReferencedBy(otherTable, tableStack))
                        return true;

                    if (string.Compare(table.Name, otherTable.Name, StringComparison.OrdinalIgnoreCase) == 0)
                        return true;
                }
            }
        }

        return false;
    }

    /// <summary>
    /// Check for direct table reference by <paramref name="otherTable"/>.
    /// </summary>
    /// <param name="otherTable">Table to check for relation.</param>
    /// <returns><c>true</c> if directly referenced; otherwise, <c>false</c>.</returns>
    public bool IsReferencedBy(Table otherTable)
    {
        return IsReferencedBy(otherTable, null);
    }

    /// <summary>
    /// Checks for indirect table reference through <paramref name="otherTable"/>.
    /// </summary>
    /// <param name="otherTable">Table to check for relation.</param>
    /// <returns><c>true</c> if indirectly referenced; otherwise, <c>false</c>.</returns>
    public bool IsReferencedVia(Table otherTable)
    {
        Table table;

        foreach (Field field in otherTable.Fields)
        {
            foreach (ForeignKeyField foreignKey in field.ForeignKeys)
            {
                table = foreignKey.ForeignKey.Table;

                // Not a direct relation, but children are related
                if (string.Compare(m_name, table.Name, StringComparison.OrdinalIgnoreCase) != 0 && IsReferencedBy(table))
                    return true;
            }
        }

        return false;
    }

    /// <summary>
    /// check for primary key field in <see cref="Table"/>
    /// </summary>
    /// <param name="fieldName"></param>
    /// <param name="primaryKeyOrdinal"></param>
    /// <param name="primaryKeyName"></param>
    /// <returns></returns>
    public bool DefinePrimaryKey(string fieldName, int primaryKeyOrdinal = -1, string primaryKeyName = "")
    {
        Field? lookupField = Fields[fieldName];

        if (lookupField is not null)
        {
            lookupField.IsPrimaryKey = true;
            lookupField.PrimaryKeyOrdinal = primaryKeyOrdinal == -1 ? PrimaryKeyFieldCount + 1 : primaryKeyOrdinal;
            lookupField.PrimaryKeyName = primaryKeyName;

            return true;
        }

        return false;
    }

    /// <summary>
    /// Check for <see cref="ForeignKeyField"/>
    /// </summary>
    /// <param name="primaryKeyFieldName"></param>
    /// <param name="foreignKeyTableName"></param>
    /// <param name="foreignKeyFieldName"></param>
    /// <param name="foreignKeyOrdinal"></param>
    /// <param name="foreignKeyName"></param>
    /// <param name="foreignKeyUpdateRule"></param>
    /// <param name="foreignKeyDeleteRule"></param>
    /// <returns></returns>
    public bool DefineForeignKey(string primaryKeyFieldName, string foreignKeyTableName, string foreignKeyFieldName, int foreignKeyOrdinal = -1, string foreignKeyName = "", ReferentialAction foreignKeyUpdateRule = ReferentialAction.NoAction, ReferentialAction foreignKeyDeleteRule = ReferentialAction.NoAction)
    {
        Field? localPrimaryKeyField = Fields[primaryKeyFieldName];

        if (localPrimaryKeyField is null)
            return false;

        Table? remoteForeignKeyTable = m_parent[foreignKeyTableName];
        Field? remoteForeignKeyField = remoteForeignKeyTable?.Fields[foreignKeyFieldName];

        if (remoteForeignKeyField == null)
            return false;
                
        ForeignKeyField localForeignKeyField = new(localPrimaryKeyField.ForeignKeys);

        localForeignKeyField.PrimaryKey = localPrimaryKeyField;
        localForeignKeyField.ForeignKey = remoteForeignKeyField;
        localForeignKeyField.ForeignKey.ReferencedBy = localForeignKeyField.PrimaryKey;
        localForeignKeyField.Ordinal = foreignKeyOrdinal == -1 ? localPrimaryKeyField.ForeignKeys.Count + 1 : foreignKeyOrdinal;
        localForeignKeyField.KeyName = foreignKeyName;
        localForeignKeyField.UpdateRule = foreignKeyUpdateRule;
        localForeignKeyField.DeleteRule = foreignKeyDeleteRule;

        localPrimaryKeyField.ForeignKeys.Add(localForeignKeyField);

        return true;
    }

    /// <summary>
    /// Re-evaluates identity SQL for database type.
    /// </summary>
    public void ReevalulateIdentitySQL()
    {
        switch (m_parent?.Parent.DataSourceType)
        {
            case DatabaseType.SQLServer:
                IdentitySQL = $"SELECT IDENT_CURRENT('{Name}')";

                break;

            case DatabaseType.Oracle:
                IdentitySQL = $"SELECT SEQ_{Name}.CURRVAL from dual";

                break;

            case DatabaseType.SQLite:
                IdentitySQL = "SELECT last_insert_rowid()";

                break;

            case DatabaseType.PostgreSQL:
                if (AutoIncField is not null)
                    IdentitySQL = $"SELECT currval(pg_get_serial_sequence('{Name.ToLower()}', '{AutoIncField.Name.ToLower()}'))";
                else
                    IdentitySQL = "SELECT lastval()";

                break;

            default:
                IdentitySQL = "SELECT @@IDENTITY";

                break;
        }
    }

    /// <summary>
    /// Calculates row count.
    /// </summary>
    public void CalculateRowCount()
    {
        if (Type == TableType.Table)
        {
            try
            {
                IDbCommand command = m_parent.Parent.Connection.CreateCommand();

                command.CommandText = $"SELECT COUNT(*) FROM {SQLEscapedName}";
                command.CommandType = CommandType.Text;

                RowCount = Convert.ToInt32(command.ExecuteScalar());
            }
            catch
            {
                RowCount = 0;
            }
        }
    }

    #endregion
}

/// <summary>
/// List of <see cref="Table"/> collection
/// </summary>
[Serializable]
public class Tables : IEnumerable<Table>
{
    #region [ Memebers ]

    //Fields
    private Schema m_parent;

    #endregion

    #region [ Constructor ]

    internal Tables(Schema parent)
    {
        // We only allow internal creation of this object
        m_parent = parent;
        TableDictionary = new Dictionary<string, Table>(StringComparer.OrdinalIgnoreCase);
        TableList = new List<Table>();
    }

    #endregion

    #region [ Properties ]

    internal Dictionary<string, Table> TableDictionary { get; private set; }

    internal List<Table> TableList { get; private set; }

    /// <summary>
    /// Gets table count.
    /// </summary>
    public int Count => TableList.Count;

    /// <summary>
    /// Gets or sets parent <see cref="Schema"/>.
    /// </summary>
    public Schema Parent
    {
        get => m_parent;
        internal set
        {
            m_parent = value;

            foreach (Table table in TableList)
            {
                table.ReevalulateIdentitySQL();
            }
        }
    }

    #endregion

    #region [ Methods ]

    /// <summary>
    /// Adds new table.
    /// </summary>
    /// <param name="table">Table to add.</param>
    public void Add(Table table)
    {
        table.Parent ??= this;

        TableDictionary.Add(table.Name, table);
        TableList.Add(table);
    }

    /// <summary>
    /// Removes table.
    /// </summary>
    /// <param name="table">Table to remove.</param>
    public void Remove(Table table)
    {
        if (table.Parent == this)
            table.Parent = null;

        TableDictionary.Remove(table.Name);
        TableList.Remove(table);
    }

    /// <summary>
    /// Clears all tables.
    /// </summary>
    public void Clear()
    {
        foreach (Table table in TableList)
        {
            if (table.Parent == this)
                table.Parent = null;
        }

        TableDictionary.Clear();
        TableList.Clear();
    }

    /// <summary>
    /// Gets table at index.
    /// </summary>
    /// <param name="index">Index of table</param>
    /// <returns>Table at index.</returns>
    public Table? this[int index]
    {
        get
        {
            if (index < 0 || index >= TableList.Count)
                return null;

            return TableList[index];
        }
    }

    /// <summary>
    /// Gets table by name.
    /// </summary>
    /// <param name="name">Table name.</param>
    /// <returns>Table with specified name.</returns>
    public Table? this[string name]
    {
        get
        {
            TableDictionary.TryGetValue(name, out Table? lookup);
            return lookup;
        }
    }

    /// <summary>
    /// Finds table by mapped named.
    /// </summary>
    /// <param name="mapName">Mapped table name.</param>
    /// <returns>Table with mapped name.</returns>
    public Table? FindByMapName(string mapName)
    {
        foreach (Table table in TableList)
        {
            if (string.Compare(table.MapName, mapName, StringComparison.OrdinalIgnoreCase) == 0)
                return table;
        }

        return null;
    }

    /// <summary>
    /// Gets table enumerator.
    /// </summary>
    /// <returns></returns>
    public IEnumerator<Table> GetEnumerator() => TableList.GetEnumerator();

    /// <summary>
    /// Gets table field list.
    /// </summary>
    /// <returns>Comma separated field list.</returns>
    public string GetList()
    {
        StringBuilder fieldList = new();

        foreach (Table tbl in TableList)
        {
            if (fieldList.Length > 0)
                fieldList.Append(',');

            fieldList.Append(tbl.SQLEscapedName);
        }

        return fieldList.ToString();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    #endregion

    #region [ Inner Class ]

    /// <summary>
    /// Check for referential order of <see cref="Table"/>
    /// </summary>
    public class ReferentialOrderComparer : IComparer<Table>
    {
        #region  [ Properties ]

        /// <summary>
        /// Default property of object
        /// </summary>
        public static readonly ReferentialOrderComparer Default = new();

        #endregion

        #region [ Methods ]

        /// <summary>
        /// Allows tables to be sorted in proper referential integrity process object
        /// </summary>
        /// <param name="table1">First table to compare.</param>
        /// <param name="table2">Second table to compare.</param>
        /// <returns></returns>
        public int Compare(Table? table1, Table? table2)
        {
            // This function allows tables to be sorted in proper referential integrity process order
            int result = 0;

            if (table1 == table2)
                return 0;

            if (table1?.IsReferencedBy(table2) ?? false) // || table1.IsReferencedVia(table2))
                result = -1;
            else if (table2?.IsReferencedBy(table1) ?? false) // || table2.IsReferencedVia(table1))
                result = 1;

            // Sort by existence of foreign key fields, if defined
            if (result == 0)
                result = ForeignKeyCompare(table1, table2);

            return result;
        }

        /// <summary>
        /// Compare foreign key comparison of tables
        /// </summary>
        /// <param name="table1"></param>
        /// <param name="table2"></param>
        /// <returns></returns>
        private int ForeignKeyCompare(Table table1, Table table2)
        {
            if (table1.IsForeignKeyTable && table2.IsForeignKeyTable)
                return 0; // Both tables have foreign key fields, consider them equal

            if (!table1.IsForeignKeyTable && !table2.IsForeignKeyTable)
                return 0; // Neither table has foreign key fields, consider them equal

            if (table1.IsForeignKeyTable)
                return 1; // Table1 has foreign key fields and Table2 does not, sort it below

            return -1; // Table2 has foreign key fields and Table1 does not, sort it below
        }

        ///// <summary>
        ///// We compare based on the existence of AutoInc fields as a secondary compare in case user
        ///// has no defined relational integrity - lastly we just sort by table name
        ///// </summary>
        ///// <param name="tbl1"></param>
        ///// <param name="tbl2"></param>
        ///// <returns></returns>
        //private int AutoIncCompare(Table tbl1, Table tbl2)
        //{
        //    return (tbl1.HasAutoIncField == tbl2.HasAutoIncField ? 0 : (tbl1.HasAutoIncField ? -1 : 1));
        //}

        #endregion
    }

    #endregion
}

/// <summary>
/// Get information about database schema
/// </summary>
[Serializable]
public class Schema
{
    #region [ Members ]

    // Fields

    /// <summary>
    /// Defines a table filter that specifies no restrictions.
    /// </summary>
    public const TableType NoRestriction = TableType.Table | TableType.View | TableType.SystemTable | TableType.SystemView | TableType.Alias | TableType.Synonym | TableType.GlobalTemp | TableType.LocalTemp | TableType.Link | TableType.Undetermined;

    [NonSerialized]
    private DbConnection m_schemaConnection;

    [NonSerialized]
    private string m_connectionString;

    #endregion

    #region [ Constructors ]

    /// <summary>
    /// Creates a new <see cref="Schema"/>.
    /// </summary>
    public Schema()
    {
        m_connectionString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=C:\\SourceDB.mdb";
        DataSourceType = DatabaseType.Other;
        TableTypeRestriction = NoRestriction;
        ImmediateClose = true;
        AllowTextNulls = false;
        AllowNumericNulls = false;
        Tables = new Tables(this);
    }

    /// <summary>
    /// Creates a new <see cref="Schema"/>.
    /// </summary>
    public Schema(string connectionString, TableType tableTypeRestriction = NoRestriction, bool immediateClose = true, bool analyzeNow = true)
    {
        m_connectionString = connectionString;
        TableTypeRestriction = tableTypeRestriction;
        ImmediateClose = immediateClose;
        AllowTextNulls = false;
        AllowNumericNulls = false;

        if (analyzeNow)
            Analyze();
        else
            Tables = new Tables(this);
    }

    #endregion

    #region [ Properties ]

    /// <summary>
    /// Get or set - information to process <see cref="Tables"/>
    /// </summary>
    public Tables Tables { get; set; }

    /// <summary>
    /// OLEDB connection string to data source to analyze.
    /// </summary>
    public string ConnectionString
    {
        get => m_connectionString;
        set => m_connectionString = value;
    }

    /// <summary>
    /// Set this value to restrict the types of tables returned in your schema.  Table types can be OR'd together to create this table type restriction.
    /// </summary>
    public TableType TableTypeRestriction { get; set; }

    /// <summary>
    /// Set this value to False to keep the schema connection used during analysis open after analysis is complete.
    /// </summary>
    public bool ImmediateClose { get; set; }

    /// <summary>
    /// Type of database specified in connect string.
    /// </summary>
    public DatabaseType DataSourceType { get; set; }

    /// <summary>
    /// Set this value to False to convert all Null values encountered in character fields to empty strings.
    /// </summary>
    public bool AllowTextNulls { get; set; }

    /// <summary>
    /// Set this value to False to convert all Null values encountered in numeric fields to zeros.
    /// </summary>
    public bool AllowNumericNulls { get; set; }

    /// <summary>
    /// <see cref="IDbConnection"/> to open a database connection
    /// </summary>
    public DbConnection Connection
    {
        get => m_schemaConnection;
        set => m_schemaConnection = value;
    }

    #endregion

    #region [ Methods ]

    /// <summary>
    /// Escapes a field or table name.
    /// </summary>
    /// <param name="name">Name to escape.</param>
    /// <returns>Escaped <paramref name="name"/>.</returns>
    public string SQLEscapeName(string name)
    {
        switch (DataSourceType) //-V3002
        {
            case DatabaseType.Access:
            case DatabaseType.SQLServer:
                return $"[{name}]";
            case DatabaseType.Oracle:
                return $"\"{name.ToUpper()}\"";
            case DatabaseType.PostgreSQL:
                return $"\"{name.ToLower()}\"";
        }

        return $"\"{name}\"";
    }

    /// <summary>
    /// Analyze data schema for processing data
    /// </summary>
    public void Analyze()
    {
        m_schemaConnection = OpenConnection(m_connectionString, out DatabaseType databaseType, out Schema deserializedSchema, out bool isAdoConnection);

        if (isAdoConnection)
        {
            if (deserializedSchema is null)
                throw new InvalidOperationException("Cannot use an ADO style connection with out a serialized schema.\r\nValidate that the \"serializedSchema\" connection string parameter exists and refers to an existing file.");

            // Reference table collection from deserialized collection
            Tables = deserializedSchema.Tables;

            // Update database type and force re-evaluation of SQL identity statements
            DataSourceType = databaseType;
            Tables.Parent = this;

            // Set normal ANSI SQL quotes mode for MySQL
            if (databaseType == DatabaseType.MySQL)
                m_schemaConnection.ExecuteNonQuery("SET sql_mode='ANSI_QUOTES'");

            // Validate table / field existence against open connection as defined in serialized schema
            List<Table> tablesToRemove = new();

            foreach (Table table in Tables)
            {
                try
                {
                    // Make sure table exists
                    m_schemaConnection.ExecuteScalar($"SELECT COUNT(*) FROM {table.SQLEscapedName}");

                    List<Field> fieldsToRemove = new();
                    string testFieldSQL;

                    try
                    {
                        // If table has an auto-inc field, this will typically be indexed and will allow for a faster field check than a count
                        if (table.HasAutoIncField)
                            testFieldSQL = $"SELECT {{0}} FROM {table.SQLEscapedName} WHERE {table.AutoIncField.SQLEscapedName} < 0";
                        else
                            testFieldSQL = $"SELECT COUNT({{0}}) FROM {table.SQLEscapedName}";
                    }
                    catch
                    {
                        testFieldSQL = $"SELECT COUNT({{0}}) FROM {table.SQLEscapedName}";
                    }

                    foreach (Field field in table.Fields)
                    {
                        try
                        {
                            // Make sure field exists
                            m_schemaConnection.ExecuteScalar(string.Format(testFieldSQL, field.SQLEscapedName));
                        }
                        catch
                        {
                            fieldsToRemove.Add(field);
                        }
                    }

                    foreach (Field field in fieldsToRemove)
                    {
                        table.Fields.Remove(field);
                    }
                }
                catch
                {
                    tablesToRemove.Add(table);
                }
            }

            foreach (Table table in tablesToRemove)
            {
                Tables.Remove(table);
            }

            // Check to see if user requested to keep connection open, this is just for convience...
            if (ImmediateClose)
                Close();
        }

        // If connection is OleDB, attempt to directly infer schema
        //AnalyzeOleDbSchema();
    }

    //[SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
    //[SuppressMessage("Microsoft.Maintainability", "CA1506:AvoidExcessiveClassCoupling")]
    //private void AnalyzeOleDbSchema()
    //{
    //    DataRow row;
    //    Table table;
    //    Field field;
    //    int x;
    //    int y;

    //    // See http://technet.microsoft.com/en-us/library/ms131488.aspx for detailed OLEDB schema row set information
    //    Tables = new Tables(this);

    //    OleDbConnection oledbSchemaConnection = m_schemaConnection as OleDbConnection;

    //    if (oledbSchemaConnection is null)
    //        throw new NullReferenceException("Current connection is an ADO style connection, OLE DB connection expected");

    //    // Attach to schema connection state change event
    //    oledbSchemaConnection.StateChange += SchemaConnection_StateChange;

    //    // Load all tables and views into the schema
    //    DataTable schemaTable = oledbSchemaConnection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables_Info, null);

    //    if (schemaTable is null)
    //        throw new NullReferenceException("Failed to retrieve OLE DB schema table for OleDbSchemaGuid.Tables_Info");

    //    for (x = 0; x < schemaTable.Rows.Count; x++)
    //    {
    //        row = schemaTable.Rows[x];

    //        table = new Table(Common.ToNonNullString(row["TABLE_CATALOG"]),
    //            Common.ToNonNullString(row["TABLE_SCHEMA"]),
    //            row["TABLE_NAME"].ToString(),
    //            row["TABLE_TYPE"].ToString(),
    //            Common.ToNonNullString(row["DESCRIPTION"], ""), 0);

    //        table.Parent = Tables;

    //        if ((table.Type & TableTypeRestriction) == TableTypeRestriction)
    //        {
    //            // Both the data adapter and the OleDB schema row sets provide column properties
    //            // that the other doesn't - so we use both to get a very complete schema                        
    //            DataSet data = new DataSet();
    //            OleDbDataAdapter adapter = new OleDbDataAdapter();

    //            if (table.Name.IndexOf(' ') == -1 && table.UsesDefaultSchema())
    //            {
    //                try
    //                {
    //                    // For standard table names we can use direct table commands for speed
    //                    adapter.SelectCommand = new OleDbCommand(table.Name, oledbSchemaConnection);
    //                    adapter.SelectCommand.CommandType = CommandType.TableDirect;
    //                    adapter.FillSchema(data, SchemaType.Mapped);
    //                }
    //                catch
    //                {
    //                    // We'll fall back on the standard method (maybe provider doesn't support TableDirect)
    //                    adapter.SelectCommand = new OleDbCommand($"SELECT TOP 1 * FROM {table.FullName}", oledbSchemaConnection);
    //                    adapter.FillSchema(data, SchemaType.Mapped);
    //                }
    //            }
    //            else
    //            {
    //                // For schema based databases and non-standard table names we must use a regular select command
    //                adapter.SelectCommand = new OleDbCommand($"SELECT TOP 1 * FROM {table.FullName}", oledbSchemaConnection);
    //                adapter.FillSchema(data, SchemaType.Mapped);
    //            }

    //            // Load all column data into the schema
    //            DataTable currentTable = oledbSchemaConnection.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, new object[] { null, null, table.Name });

    //            if (currentTable is not null)
    //            {
    //                for (y = 0; y < currentTable.Rows.Count; y++)
    //                {
    //                    row = currentTable.Rows[y];

    //                    // New field encountered, create new field
    //                    field = new Field(row["COLUMN_NAME"].ToString(), (OleDbType)row["DATA_TYPE"]);
    //                    field.HasDefault = Convert.ToBoolean(Common.NotNull(row["COLUMN_HASDEFAULT"], false));
    //                    field.NumericPrecision = Convert.ToInt32(Common.NotNull(row["NUMERIC_PRECISION"], false));
    //                    field.NumericScale = Convert.ToInt32(Common.NotNull(row["NUMERIC_SCALE"], false));
    //                    field.DateTimePrecision = Convert.ToInt32(Common.NotNull(row["DATETIME_PRECISION"], false));
    //                    field.Description = Common.ToNonNullString(row["DESCRIPTION"], "");

    //                    // We also use as many properties as we can from data adapter schema
    //                    field.Ordinal = data.Tables[0].Columns[field.Name].Ordinal;

    //                    field.AllowsNulls = data.Tables[0].Columns[field.Name].AllowDBNull;
    //                    field.DefaultValue = data.Tables[0].Columns[field.Name].DefaultValue;
    //                    field.MaxLength = data.Tables[0].Columns[field.Name].MaxLength;
    //                    field.AutoIncrement = data.Tables[0].Columns[field.Name].AutoIncrement;
    //                    field.AutoIncrementSeed = Convert.ToInt32(data.Tables[0].Columns[field.Name].AutoIncrementSeed);
    //                    field.AutoIncrementStep = Convert.ToInt32(data.Tables[0].Columns[field.Name].AutoIncrementStep);
    //                    field.ReadOnly = data.Tables[0].Columns[field.Name].ReadOnly;
    //                    field.Unique = data.Tables[0].Columns[field.Name].Unique;

    //                    // Add field to table's field collection
    //                    table.Fields.Add(field);
    //                }
    //            }

    //            // Sort all loaded fields in ordinal order
    //            table.Fields.FieldList.Sort();

    //            // Define primary keys
    //            try
    //            {
    //                DataTable primaryKeyTable = oledbSchemaConnection.GetOleDbSchemaTable(OleDbSchemaGuid.Primary_Keys, new object[] { null, null, table.Name });

    //                if (primaryKeyTable is not null)
    //                {
    //                    for (y = 0; y <= primaryKeyTable.Rows.Count - 1; y++)
    //                    {
    //                        row = primaryKeyTable.Rows[y];
    //                        table.DefinePrimaryKey(row["COLUMN_NAME"].ToString(), Convert.ToInt32(Common.NotNull(row["ORDINAL"], -1)), Common.ToNonNullString(row["PK_NAME"], ""));
    //                    }
    //                }
    //            }
    //            catch
    //            {
    //                // It's possible that the data source doesn't provide a primary keys row set
    //            }

    //            // Add table to schema's table collection
    //            Tables.Add(table);
    //        }
    //    }

    //    // Define foreign keys (must be done after all tables are defined so relations can be properly established)
    //    for (int i = 0; i < Tables.Count; i++)
    //    {
    //        table = Tables[i];

    //        try
    //        {
    //            DataTable foreignKeyTable = oledbSchemaConnection.GetOleDbSchemaTable(OleDbSchemaGuid.Foreign_Keys, new object[] { null, null, table.Name });

    //            if (foreignKeyTable is not null)
    //            {
    //                for (x = 0; x <= foreignKeyTable.Rows.Count - 1; x++)
    //                {
    //                    row = foreignKeyTable.Rows[x];
    //                    table.DefineForeignKey(row["PK_COLUMN_NAME"].ToString(),
    //                        row["FK_TABLE_NAME"].ToString(), row["FK_COLUMN_NAME"].ToString(),
    //                        Convert.ToInt32(Common.NotNull(row["ORDINAL"], -1)), Common.ToNonNullString(row["FK_NAME"], ""),
    //                        Field.GetReferentialAction(Common.ToNonNullString(row["UPDATE_RULE"], "")),
    //                        Field.GetReferentialAction(Common.ToNonNullString(row["DELETE_RULE"], "")));
    //                }
    //            }
    //        }
    //        catch
    //        {
    //            // It's possible that the data source doesn't provide a foreign keys row set
    //        }
    //    }

    //    // Using a simple (i.e., stable) sorting algorithm here since not all relationships will
    //    // be considered mathematically congruent and the fast .NET sort algorithm depends on
    //    // comparisons based on perfect equality (i.e., if A > B and B > C then A > C - this may
    //    // not be true in terms of referential integrity)
    //    List<Table> sortedList = new List<Table>(Tables.TableList);
    //    Table temp;

    //    for (x = 0; x < sortedList.Count; x++)
    //    {
    //        for (y = 0; y < sortedList.Count; y++)
    //        {
    //            if (x != y && Tables.ReferentialOrderComparer.Default.Compare(sortedList[x], sortedList[y]) < 0)
    //            {
    //                temp = sortedList[y];
    //                sortedList[y] = sortedList[x];
    //                sortedList[x] = temp;
    //            }
    //        }
    //    }

    //    // Set initial I/O processing priorities for tables based on this order.  Processing tables
    //    // based on the "Priority" field allows user to have final say in processing order
    //    for (x = 0; x < sortedList.Count; x++)
    //    {
    //        Tables.TableList.Find(tbl => string.Compare(tbl.Name, sortedList[x].Name, StringComparison.OrdinalIgnoreCase) == 0).Priority = x;
    //    }

    //    // Detach from schema connection state change event
    //    oledbSchemaConnection.StateChange -= SchemaConnection_StateChange;

    //    // Check to see if user requested to keep connection open, this is just for convience...
    //    if (ImmediateClose)
    //        Close();
    //}

    ///// <summary>
    ///// <see cref="IDbConnection"/> state change event will fire if it unexpectedly close connection while processing.
    ///// </summary>
    //private void SchemaConnection_StateChange(object _, StateChangeEventArgs e)
    //{
    //    // The connection may have been closed prematurely so we reopen it.
    //    if (m_schemaConnection.State == ConnectionState.Closed)
    //        m_schemaConnection.Open();
    //}

    /// <summary>
    /// Close <see cref="IDbConnection"/> 
    /// </summary>
    public void Close()
    {
        if (m_schemaConnection is not null)
        {
            try
            {
                m_schemaConnection.Close();
            }
            catch
            {
                // Keep on going here...
            }
        }

        m_schemaConnection = null;
    }

    #endregion

    #region [ Static ]

    // Static Methods

    /// <summary>
    /// Opens an ADO connection.
    /// </summary>
    /// <param name="connectionString">ADO connection string.</param>
    /// <returns>Opened connection.</returns>
    public static IDbConnection OpenConnection(string connectionString)
    {
        return OpenConnection(connectionString, out DatabaseType _, out Schema _, out bool _);
    }

    /// <summary>
    /// Opens an ADO connection.
    /// </summary>
    /// <param name="connectionString">ADO connection string.</param>
    /// <param name="databaseType">Database type.</param>
    /// <param name="deserializedSchema">The deserialized schema.</param>
    /// <param name="isAdoConnection">Flag that determines if connection is ADO.</param>
    /// <returns>Opened connection.</returns>
    public static DbConnection OpenConnection(string connectionString, out DatabaseType databaseType, out Schema deserializedSchema, out bool isAdoConnection)
    {
        //    Dictionary<string, string> settings = connectionString.ParseKeyValuePairs();

        deserializedSchema = null;
        databaseType = DatabaseType.Other;
        isAdoConnection = false;

        //    if (settings.ContainsKey("DataProviderString"))
        //    {
        //        // Assuming ADO connection
        //        string dataProviderString = settings["DataProviderString"];

        //        settings.Remove("DataProviderString");

        //        if (settings.ContainsKey("serializedSchema"))
        //        {
        //            string serializedSchemaFileName = FilePath.GetAbsolutePath(settings["serializedSchema"]);

        //            if (File.Exists(serializedSchemaFileName))
        //            {
        //                using (FileStream stream = new FileStream(serializedSchemaFileName, FileMode.Open, FileAccess.Read, FileShare.Read))
        //                {
        //                    deserializedSchema = Serialization.Deserialize<Schema>(stream, SerializationFormat.Binary);
        //                }
        //            }

        //            settings.Remove("serializedSchema");
        //        }

        //        // Create updated connection string with removed settings
        //        connectionString = settings.JoinKeyValuePairs();

        //        AdoDataConnection database = new AdoDataConnection(connectionString, dataProviderString);

        //        databaseType = database.DatabaseType;
        //        isAdoConnection = true;

        //        return database.Connection;
        //    }

        //    // Assuming OLEDB connection
        //    OleDbConnection connection = new OleDbConnection(connectionString);
        //    connection.Open();

        //    return connection;
        return null;
    }

    #endregion
}
