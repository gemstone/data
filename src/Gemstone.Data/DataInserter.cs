﻿//******************************************************************************************************
//  DataInserter.cs - Gbtc
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
//       Generated original version of source code from code written in 2003.
//  08/21/2008 - Mihir Brahmbhatt
//       Converted to C# extensions.
//  09/27/2010 - Mihir Brahmbhatt
//       Edited code comments.
//  10/12/2010 - Mihir Brahmbhatt
//       Updated preserve value functionality for auto-inc fields
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
using System.IO;
using System.Text;
using Gemstone.Data.DataExtensions;
using Gemstone.EventHandlerExtensions;
using Gemstone.IO;
using Gemstone.Reflection;

#pragma warning disable CS8600
#pragma warning disable CS8601
#pragma warning disable CS8603
#pragma warning disable CS8604
#pragma warning disable CS8618

// ReSharper disable InconsistentNaming
namespace Gemstone.Data;
// Note: if you have triggers that insert records into other tables automatically that have defined records to
// be inserted, this class will check for this occurrence and do SQL updates instead of SQL inserts.  However,
// just like in the DataUpdater class, you must define the primary key fields in the database or through code
// to be used for updates for each table in the table collection in order for the updates to occur, hence any
// tables which have no key fields defined yet appear in the table collection will not be updated...

/// <summary>
/// This class defines a common set of functionality that any data operation implementation can use 
/// </summary>
public class DataInserter : BulkDataOperationBase
{
    #region [ Members ]

    //Fields

    /// <summary>
    /// Table cleared event.
    /// </summary>
    public event EventHandler<EventArgs> TableCleared;

    /// <summary>
    /// Bulk-insert executing event.
    /// </summary>
    public event EventHandler<EventArgs> BulkInsertExecuting;

    /// <summary>
    /// Bulk-insert completed event.
    /// </summary>
    public event EventHandler<EventArgs> BulkInsertCompleted;

    /// <summary>
    /// Bulk-insert exception event.
    /// </summary>
    public event EventHandler<EventArgs> BulkInsertException;

    /// <summary>
    /// Disposed event.
    /// </summary>
    public event EventHandler Disposed;

    #endregion

    #region [ Constructors ]

    /// <summary>
    /// Creates a new <see cref="DataInserter"/>.
    /// </summary>
    public DataInserter()
    {
    }

    /// <summary>
    /// Creates a new <see cref="DataInserter"/>.
    /// </summary>
    public DataInserter(string fromConnectString, string toConnectString)
        : base(fromConnectString, toConnectString)
    {
    }

    /// <summary>
    /// Creates a new <see cref="DataInserter"/>.
    /// </summary>
    public DataInserter(Schema fromSchema, Schema toSchema)
        : base(fromSchema, toSchema)
    {
    }

    #endregion

    #region [ Properties ]

    /// <summary>
    /// Get or set to attempt use of a BULK INSERT on a destination SQL Server connection
    /// </summary>
    public bool AttemptBulkInsert { get; set; }

    /// <summary>
    /// Get or set to force use of a BULK INSERT on a destination SQL Server connection regardless of whether or not it looks
    /// like the referential integrity definition supports this.
    /// </summary>
    public bool ForceBulkInsert { get; set; }

    /// <summary>
    /// This setting defines the SQL Server BULK INSERT settings that will be used if a BULK INSERT is performed. 
    /// </summary>
    public string BulkInsertSettings { get; set; } = "FIELDTERMINATOR = '\\t', ROWTERMINATOR = '\\n', CODEPAGE = 'OEM', FIRE_TRIGGERS, KEEPNULLS";

    /// <summary>
    /// This setting defines the text encoding that will be used when writing a temporary BULK INSERT file that will be needed
    /// if a SQL Server BULK INSERT is performed - make sure the encoding output matches the specified CODEPAGE value in the
    /// BulkInsertSettings property.
    /// </summary>
    public Encoding BulkInsertEncoding { get; set; } = Encoding.ASCII;

    /// <summary>
    /// This setting defines the file path that will be used when writing a temporary BULK INSERT file that will be needed if a
    /// SQL Server BULK INSERT is performed - make sure the destination SQL Server has rights to this path.
    /// </summary>
    public string BulkInsertFilePath { get; set; } = AssemblyInfo.ExecutingAssembly.Location;

    /// <summary>
    /// This specifies the string that will be substituted for the field terminator or row terminator if encountered in a database
    /// value while creating a BULK INSERT file.  The field terminator and row terminator values are defined in the BulkInsertSettings
    /// property specified by the FIELDTERMINATOR and ROWTERMINATOR keywords respectively.
    /// </summary>
    public string DelimiterReplacement { get; set; } = " - ";

    /// <summary>
    /// Set to True to clear all data from the destination database before processing data inserts.
    /// </summary>
    public bool ClearDestinationTables { get; set; }

    /// <summary>
    /// Set to True to attempt use of a TRUNCATE TABLE on a destination SQL Server connection if ClearDestinationTables is True
    /// and it looks like the referential integrity definition supports this.  Your SQL Server connection will need the rights
    /// to perform this operation.
    /// </summary>
    public bool AttemptTruncateTable { get; set; }

    /// <summary>
    /// Set to True to force use of a TRUNCATE TABLE on a destination SQL Server connection if ClearDestinationTables is True regardless
    /// of whether or not it looks like the referential integrity definition supports this.  Your SQL Server connection will need the
    /// rights to perform this operation
    /// </summary>
    public bool ForceTruncateTable { get; set; }

    /// <summary>
    /// Set to True to preserve primary key value data to the destination database before processing data inserts.
    /// </summary>
    public bool PreserveAutoIncValues { get; set; }

    #endregion

    #region [ Methods ]

    /// <summary>
    /// Close
    /// </summary>
    public override void Close()
    {
        base.Close();

        Disposed?.SafeInvoke(this, EventArgs.Empty); //-V3083
    }

    /// <summary>
    /// Execute this <see cref="DataInserter"/>
    /// </summary>
    public override void Execute()
    {
        List<Table> tablesList = [];
        Table tableLookup;
        Table table;
        int x;

        OverallProgress = 0;
        OverallTotal = 0;

        if (TableCollection.Count == 0)
            Analyze();

        if (TableCollection.Count == 0)
            throw new NullReferenceException("No tables to process even after analyze.");

        // Clear data from destination tables, if requested
        if (ClearDestinationTables)
        {
            // We do not consider table exclusions when deleting data from destination tables as these may have triggered inserts
            List<Table> allSourceTables = [..FromSchema.Tables];

            // Clear data in a child to parent direction to help avoid potential constraint issues
            allSourceTables.Sort((table1, table2) => table1.Priority > table2.Priority ? 1 : table1.Priority < table2.Priority ? -1 : 0);

            for (x = allSourceTables.Count - 1; x >= 0; x += -1)
            {
                // Lookup table name in destination data source
                tableLookup = ToSchema.Tables.FindByMapName(allSourceTables[x].MapName);

                if (tableLookup is not null)
                {
                    if (ClearTable(tableLookup) && tableLookup.HasAutoIncField)
                        ResetAutoIncValues(tableLookup);
                }
            }
        }

        // We copy the tables into an array list so we can sort and process them in priority order
        foreach (Table sourceTable in TableCollection)
        {
            if (sourceTable.Process)
            {
                sourceTable.CalculateRowCount();

                if (sourceTable.RowCount > 0)
                {
                    tablesList.Add(sourceTable);
                    OverallTotal += sourceTable.RowCount;
                }
            }
        }

        tablesList.Sort((table1, table2) => table1.Priority > table2.Priority ? 1 : table1.Priority < table2.Priority ? -1 : 0);

        // Begin inserting data into destination tables
        for (x = 0; x <= tablesList.Count - 1; x++)
        {
            table = tablesList[x];

            // Lookup table name in destination data source
            tableLookup = ToSchema.Tables.FindByMapName(table.MapName);

            if (tableLookup is not null)
            {
                if (table.RowCount > 0)
                {
                    // Inform clients of table copy
                    OnTableProgress(table.Name, true, x + 1, tablesList.Count);

                    // Copy source table to destination
                    ExecuteInserts(table, tableLookup);
                }
                else
                {
                    // Inform clients of table skip
                    OnTableProgress(table.Name, false, x + 1, tablesList.Count);
                }
            }
            else
            {
                // Inform clients of table skip
                OnTableProgress(table.Name, false, x + 1, tablesList.Count);
            }
        }

        // Perform final update of progress information
        OnTableProgress("", false, tablesList.Count, tablesList.Count);
    }

    /// <summary>
    /// Clear destination schema table
    /// </summary>
    /// <param name="table">schema table</param>
    private bool ClearTable(Table table)
    {
        string deleteSql;
        bool useTruncateTable = false;

        if (AttemptTruncateTable || ForceTruncateTable)
        {
            // We only attempt a truncate table if the destination data source type is SQL Server
            // and table has no foreign key dependencies (or user forces procedure)
            useTruncateTable = ForceTruncateTable || table.Parent.Parent.DataSourceType == DatabaseType.SQLServer && !table.ReferencedByForeignKeys;
        }

        if (useTruncateTable)
            deleteSql = $"TRUNCATE TABLE {table.SQLEscapedName}";
        else
            deleteSql = $"DELETE FROM {table.SQLEscapedName}";

        try
        {
            table.Connection.ExecuteNonQuery(deleteSql, Timeout);

            TableCleared?.SafeInvoke(this, new EventArgs<string>(table.Name)); //-V3083

            return true;
        }
        catch (Exception ex)
        {
            if (useTruncateTable)
            {
                // SQL Server connection may not have rights to use TRUNCATE TABLE, fall back on DELETE FROM
                AttemptTruncateTable = false;
                ForceTruncateTable = false;

                return ClearTable(table);
            }

            OnSQLFailure(deleteSql, ex);
        }

        return false;
    }

    private void ResetAutoIncValues(Table table)
    {
        string resetAutoIncValueSQL = "unknown";

        try
        {
            switch (table.Parent.Parent.DataSourceType) //-V3002
            {
                case DatabaseType.SQLServer:
                    resetAutoIncValueSQL = $"DBCC CHECKIDENT('{table.SQLEscapedName}', RESEED)";
                    table.Connection.ExecuteNonQuery(Timeout, resetAutoIncValueSQL);

                    break;
                case DatabaseType.MySQL:
                    resetAutoIncValueSQL = $"ALTER TABLE {table.SQLEscapedName} AUTO_INCREMENT = 1";
                    table.Connection.ExecuteNonQuery(Timeout, resetAutoIncValueSQL);

                    break;
                case DatabaseType.SQLite:
                    resetAutoIncValueSQL = $"DELETE FROM sqlite_sequence WHERE name = '{table.Name}'";
                    table.Connection.ExecuteNonQuery(Timeout, resetAutoIncValueSQL);

                    break;
                case DatabaseType.PostgreSQL:
                    // The escaping of names here is very deliberate; for certain table names,
                    // it is necessary to escape the table name in the pg_get_serial_sequence() call,
                    // but the call will fail if you attempt to escape the autoIncField name
                    resetAutoIncValueSQL = $"SELECT setval(pg_get_serial_sequence('{table.SQLEscapedName}', '{table.AutoIncField.Name.ToLower()}'), (SELECT MAX({table.AutoIncField.SQLEscapedName}) FROM {table.SQLEscapedName}))";
                    table.Connection.ExecuteNonQuery(resetAutoIncValueSQL, Timeout);

                    break;
            }
        }
        catch (Exception ex)
        {
            OnSQLFailure(resetAutoIncValueSQL, new InvalidOperationException($"Failed to reset auto-increment seed for table \"{table.Name}\": {ex.Message}", ex));
        }
    }

    /// <summary>
    /// Execute a command to insert or update data from source to destination table
    /// </summary>
    /// <param name="fromTable">Source table</param>
    /// <param name="toTable">Destination table</param>
    private void ExecuteInserts(Table fromTable, Table toTable)
    {
        Table sourceTable = UseFromSchemaRi ? fromTable : toTable;
        Field autoIncField = null;
        Field lookupField;
        Field commonField;
        bool usingIdentityInsert;

        // Progress process variables
        int progressIndex = 0;

        // Bulk insert variables
        bool useBulkInsert = false;
        string bulkInsertFile = "";
        string fieldTerminator = "";
        string rowTerminator = "";
        FileStream bulkInsertFileStream = null;

        // Create a field list of all of the common fields in both tables
        Fields fieldCollection = new(toTable);

        foreach (Field field in fromTable.Fields)
        {
            // Lookup field name in destination table                
            lookupField = toTable.Fields[field.Name];

            if (lookupField is not null)
            {
                // We currently don't handle binary fields...
                if (!(field.Type is OleDbType.Binary or OleDbType.LongVarBinary or OleDbType.VarBinary) && !(lookupField.Type is OleDbType.Binary or OleDbType.LongVarBinary or OleDbType.VarBinary))
                {
                    // Copy field information from destination field
                    if (UseFromSchemaRi)
                    {
                        commonField = new Field(field.Name, field.Type);
                        commonField.AutoIncrement = field.AutoIncrement;
                    }
                    else
                    {
                        commonField = new Field(lookupField.Name, lookupField.Type);
                        commonField.AutoIncrement = lookupField.AutoIncrement;
                    }

                    fieldCollection.Add(commonField);
                }
            }
        }

        // Exit if no common field names were found
        if (fieldCollection.Count == 0)
        {
            OverallProgress += fromTable.RowCount;

            return;
        }

        int progressTotal = fromTable.RowCount;
        OnRowProgress(fromTable.Name, 0, progressTotal);
        OnOverallProgress((int)OverallProgress, (int)OverallTotal);

        // Setup to track to and from auto-inc values if table has an identity field
        if (sourceTable.HasAutoIncField)
        {
            foreach (Field field in fieldCollection)
            {
                lookupField = sourceTable.Fields[field.Name];

                if (lookupField is { AutoIncrement: true, ForeignKeys.Count: > 0 })
                    // We need only track auto inc translations when field is referenced by foreign keys
                {
                    // Create a new hash-table to hold auto-inc translations
                    lookupField.AutoIncrementTranslations = new Hashtable();

                    // Create a new auto-inc field to hold source value
                    autoIncField = new Field(field.Name, lookupField.Type);
                    autoIncField.AutoIncrementTranslations = lookupField.AutoIncrementTranslations;

                    break;
                }
            }
        }

        // See if this table is a candidate for bulk inserts
        if (AttemptBulkInsert || ForceBulkInsert)
            useBulkInsert = SetupBulkInsert(toTable, autoIncField, ref bulkInsertFile, ref fieldTerminator, ref rowTerminator, ref bulkInsertFileStream);

        string selectString = $"SELECT {fieldCollection.GetList(sqlEscapeFunction: FromSchema.SQLEscapeName)} FROM {fromTable.SQLEscapedName}";
        bool skipKeyValuePreservation = false;

        // Handle special case of self-referencing table
        if (sourceTable.IsReferencedBy(sourceTable)) //-V3062
        {
            // We need a special order-by for this scenario to make sure referenced rows are inserted before other rows - this also
            // means no auto-inc preservation is possible on this table
            skipKeyValuePreservation = true;
            selectString += " ORDER BY ";
            int index = 0;

            foreach (Field field in sourceTable.Fields)
            {
                foreach (ForeignKeyField foreignKey in field.ForeignKeys)
                {
                    if (string.Compare(sourceTable.Name, foreignKey.ForeignKey.Table.Name, StringComparison.OrdinalIgnoreCase) == 0)
                    {
                        // If Oracle, force it to sort NULLs at a higher level - note coalesce may fail for non-integer based primary keys for self-referencing tables
                        if (FromSchema.DataSourceType is DatabaseType.Oracle or DatabaseType.PostgreSQL)
                            selectString += $"{(index > 0 ? ", " : "")}COALESCE({FromSchema.SQLEscapeName(foreignKey.ForeignKey.Name)}, 0)";
                        else
                            selectString += (index > 0 ? ", " : "") + FromSchema.SQLEscapeName(foreignKey.ForeignKey.Name);

                        index++;
                    }
                }
            }
        }
        else
        {
            // Order by auto increment field to help preserve the original value while transferring data to destination table
            if (autoIncField is not null)
                selectString += $" ORDER BY {FromSchema.SQLEscapeName(autoIncField.Name)}";
        }

        // We use an optimization available to some databases when we are preserving the original primary key values
        if (!skipKeyValuePreservation && PreserveAutoIncValues && autoIncField is not null)
        {
            switch (ToSchema.DataSourceType)
            {
                case DatabaseType.SQLServer:
                    try
                    {
                        toTable.Connection.ExecuteNonQuery($"SET IDENTITY_INSERT {toTable.SQLEscapedName} ON", Timeout);
                        usingIdentityInsert = true;
                    }
                    catch
                    {
                        // This may fail if connected user doesn't have alter rights to destination connection or has
                        // selected the wrong destination database type, in these cases we just fall back on the
                        // brute force method of auto-inc identity synchronization
                        usingIdentityInsert = false;
                    }

                    break;
                case DatabaseType.MySQL:
                case DatabaseType.SQLite:
                case DatabaseType.PostgreSQL:
                    usingIdentityInsert = true;

                    break;
                default:
                    usingIdentityInsert = false;

                    break;
            }
        }
        else
        {
            usingIdentityInsert = false;
        }

        string insertSQLStub = $"INSERT INTO {toTable.SQLEscapedName} ({fieldCollection.GetList(usingIdentityInsert)}) VALUES (";
        string updateSQLStub = $"UPDATE {toTable.SQLEscapedName} SET ";
        string countSQLStub = $"SELECT COUNT(*) AS Total FROM {toTable.SQLEscapedName}";

        // Execute source query
        (DbDataReader fromReader, DbCommand command) = fromTable.Connection.ExecuteReader(selectString, CommandBehavior.SequentialAccess, Timeout);
        
        using (fromReader)
        using (command)
        {
            // Read source records and write each to destination
            while (fromReader.Read())
            {
                if (useBulkInsert)
                    WriteBulkInsertRecord(toTable, fieldCollection, sourceTable, fieldTerminator, rowTerminator, bulkInsertFileStream, fromReader);
                else
                    InsertDestinationRecord(toTable, fieldCollection, insertSQLStub, updateSQLStub, countSQLStub, usingIdentityInsert, sourceTable, autoIncField, skipKeyValuePreservation, fromReader);

                progressIndex++;
                OverallProgress++;

                OnRowProgress(fromTable.Name, progressIndex, progressTotal);

                if (progressIndex % RowReportInterval == 0)
                    OnOverallProgress((int)OverallProgress, (int)OverallTotal);
            }
        }

        // Turn off identity inserts and reset auto-inc values if needed
        if (usingIdentityInsert)
        {
            if (ToSchema.DataSourceType == DatabaseType.SQLServer)
            {
                string setIndentityInsertSQL = $"SET IDENTITY_INSERT {toTable.SQLEscapedName} OFF";

                try
                {
                    // Turn off identity inserts
                    toTable.Connection.ExecuteNonQuery(setIndentityInsertSQL, Timeout);
                }
                catch (Exception ex)
                {
                    OnSQLFailure(setIndentityInsertSQL, new InvalidOperationException($"Failed to turn off identity inserts on table \"{toTable.Name}\": {ex.Message}", ex));
                }
            }

            ResetAutoIncValues(toTable);
        }

        if (useBulkInsert && bulkInsertFileStream is not null)
            CompleteBulkInsert(toTable, progressIndex, bulkInsertFile, bulkInsertFileStream);

        OnRowProgress(fromTable.Name, progressTotal, progressTotal);
        OnOverallProgress((int)OverallProgress, (int)OverallTotal);
    }

    private void InsertDestinationRecord(Table toTable, Fields fieldCollection, string insertSQLStub, string updateSqlStub, string countSqlStub, bool usingIdentityInsert, Table sourceTable, Field autoIncField, bool skipKeyValuePreservation, IDataReader fromReader)
    {
        Field lookupField;
        string value;
        bool isPrimary;

        bool addedFirstInsertField = false;
        bool addedFirstUpdateField = false;

        // Handle creating SQL for inserts or updates for each row...
        StringBuilder insertSQL = new(insertSQLStub);
        StringBuilder updateSQL = new(updateSqlStub);
        StringBuilder countSQL = new(countSqlStub);
        StringBuilder whereSQL = new();

        // Coerce all field data into proper SQL formats
        foreach (Field field in fieldCollection)
        {
            try
            {
                field.Value = fromReader[field.Name];
            }
            catch (Exception ex)
            {
                field.Value = "";
                OnSQLFailure($"Failed to get field value for '{toTable.Name}.{field.Name}'", ex);
            }

            // Get translated auto-inc value for field if necessary...
            field.Value = DereferenceValue(sourceTable, field.Name, field.Value);

            // If this field is auto-inc we need to track original value
            if (field.AutoIncrement)
            {
                if (autoIncField is not null)
                {
                    // Even if database supports multiple auto-inc fields, we can only support automatic
                    // ID translation for one because the identity SQL can only return one value...
                    if (string.Compare(field.Name, autoIncField.Name, StringComparison.OrdinalIgnoreCase) == 0)
                    {
                        // Track original auto-inc value
                        autoIncField.Value = field.Value;
                    }
                }
            }

            // We don't attempt to insert values into auto-inc fields unless we are using identity inserts
            if (usingIdentityInsert || !field.AutoIncrement)
            {
                // Get SQL encoded field value
                value = field.SQLEncodedValue;

                // Reference source field to check RI properties
                lookupField = sourceTable.Fields[field.Name];

                if (lookupField is not null)
                {
                    // Check for cases where a NULL value is not allowed
                    if (!lookupField.AllowsNulls && value.Equals("NULL", StringComparison.CurrentCultureIgnoreCase))
                        value = lookupField.NonNullNativeValue;

                    // Check for possible values that should be interpreted as NULL values in nullable foreign key fields
                    if (lookupField is { AllowsNulls: true, IsForeignKey: true } && value.Equals(lookupField.NonNullNativeValue, StringComparison.OrdinalIgnoreCase))
                        value = "NULL";

                    // Check to see if this is a key field
                    isPrimary = lookupField.IsPrimaryKey;
                }
                else
                {
                    isPrimary = false;
                }

                // Construct SQL statements
                if (addedFirstInsertField)
                    insertSQL.Append(", ");
                else
                    addedFirstInsertField = true;

                insertSQL.Append(value);

                if (string.Compare(value, "NULL", StringComparison.OrdinalIgnoreCase) != 0)
                {
                    if (isPrimary)
                    {
                        if (whereSQL.Length == 0)
                            whereSQL.Append(" WHERE ");
                        else
                            whereSQL.Append(" AND ");

                        whereSQL.Append(field.SQLEscapedName);
                        whereSQL.Append(" = ");
                        whereSQL.Append(value);
                    }
                    else
                    {
                        if (addedFirstUpdateField)
                            updateSQL.Append(", ");
                        else
                            addedFirstUpdateField = true;

                        updateSQL.Append(field.SQLEscapedName);
                        updateSQL.Append(" = ");
                        updateSQL.Append(value);
                    }
                }
            }
        }

        insertSQL.Append(")");

        if (autoIncField is not null || whereSQL.Length == 0)
        {
            try
            {
                // Insert record into destination table
                if (addedFirstInsertField || autoIncField is not null)
                {
                    // Added check to preserve ID number for auto-inc fields
                    if (!usingIdentityInsert && !skipKeyValuePreservation && PreserveAutoIncValues && autoIncField is not null)
                    {
                        int toTableRowCount = int.Parse(ToNonNullString(toTable.Connection.ExecuteScalar($"SELECT MAX({autoIncField.SQLEscapedName}) FROM {toTable.SQLEscapedName}", Timeout), "0")) + 1;
                        int sourceTablePrimaryFieldValue = int.Parse(ToNonNullString(autoIncField.Value, "0"));
                        int synchronizations = 0;

                        for (int i = toTableRowCount; i < sourceTablePrimaryFieldValue; i++)
                        {
                            // Insert record into destination table up to identity field value
                            toTable.Connection.ExecuteNonQuery(insertSQL.ToString(), Timeout);
                            int currentIdentityValue = int.Parse(ToNonNullString(toTable.Connection.ExecuteScalar(toTable.IdentitySQL, Timeout), "0"));

                            // Delete record which was just inserted
                            toTable.Connection.ExecuteNonQuery($"DELETE FROM {toTable.SQLEscapedName} WHERE {autoIncField.SQLEscapedName} = {currentIdentityValue}", Timeout);

                            // For very long spans of auto-inc identity gaps we at least provide some level of feedback
                            if (synchronizations++ % 50 == 0)
                                OnTableProgress($"Processed {synchronizations} auto-increment identity synchronizations...", false, 0, 0);
                        }
                    }

                    // Insert record into destination table
                    if (whereSQL.Length > 0)
                        InsertOrUpdate(toTable, insertSQL, updateSQL, countSQL, whereSQL, addedFirstInsertField, addedFirstUpdateField);
                    else
                        toTable.Connection.ExecuteNonQuery(insertSQL.ToString(), Timeout);
                }

                // Save new destination auto-inc value
                if (autoIncField is not null)
                {
                    try
                    {
                        if (usingIdentityInsert || whereSQL.Length > 0)
                            autoIncField.AutoIncrementTranslations.Add(Convert.ToString(autoIncField.Value), Convert.ToString(autoIncField.Value));
                        else
                            autoIncField.AutoIncrementTranslations.Add(Convert.ToString(autoIncField.Value), toTable.Connection.ExecuteScalar(toTable.IdentitySQL, Timeout));
                    }
                    catch (Exception ex)
                    {
                        OnSQLFailure(toTable.IdentitySQL, ex);
                    }
                }
            }
            catch (Exception ex)
            {
                OnSQLFailure(insertSQL.ToString(), ex);
            }
        }
        else
        {
            InsertOrUpdate(toTable, insertSQL, updateSQL, countSQL, whereSQL, addedFirstInsertField, addedFirstUpdateField);
        }
    }

    private void InsertOrUpdate(Table toTable, StringBuilder insertSQL, StringBuilder updateSql, StringBuilder countSql, StringBuilder whereSql, bool addedFirstInsertField, bool addedFirstUpdateField)
    {
        // Add where criteria to SQL count statement
        countSql.Append(whereSql);

        // Make sure record doesn't already exist
        try
        {
            // If record already exists due to triggers or other means we must update it instead of inserting it
            if (int.Parse(ToNonNullString(toTable.Connection.ExecuteScalar(countSql.ToString(), Timeout), "0")) > 0)
            {
                // Add where criteria to SQL update statement
                updateSql.Append(whereSql);

                try
                {
                    // Update record in destination table
                    if (addedFirstUpdateField)
                        toTable.Connection.ExecuteNonQuery(updateSql.ToString(), Timeout);
                }
                catch (Exception ex)
                {
                    OnSQLFailure(updateSql.ToString(), ex);
                }
            }
            else
            {
                try
                {
                    // Insert record into destination table
                    if (addedFirstInsertField)
                        toTable.Connection.ExecuteNonQuery(insertSQL.ToString(), Timeout);
                }
                catch (Exception ex)
                {
                    OnSQLFailure(insertSQL.ToString(), ex);
                }
            }
        }
        catch (Exception ex)
        {
            OnSQLFailure(countSql.ToString(), ex);
        }
    }

    private bool SetupBulkInsert(Table toTable, Field autoIncField, ref string bulkInsertFile, ref string fieldTerminator, ref string rowTerminator, ref FileStream bulkInsertFileStream)
    {
        Schema parentSchema = toTable.Parent.Parent;

        // We only attempt a bulk insert if the destination data source type is SQL Server and we are inserting
        // fields into a table that has no auto-inc fields with foreign key dependencies (or user forces procedure)
        bool useBulkInsert = ForceBulkInsert || parentSchema.DataSourceType == DatabaseType.SQLServer && (autoIncField is null || TableCollection.Count == 1);

        if (useBulkInsert)
        {
            ParseBulkInsertSettings(out fieldTerminator, out rowTerminator);

            if (BulkInsertFilePath.Substring(BulkInsertFilePath.Length - 1) != "\\")
                BulkInsertFilePath += "\\";

            bulkInsertFile = $"{BulkInsertFilePath}{new Guid()}.tmp";
            bulkInsertFileStream = File.Create(bulkInsertFile);
        }

        return useBulkInsert;
    }

    private void WriteBulkInsertRecord(Table toTable, Fields fieldCollection, Table sourceTable, string fieldTerminator, string rowTerminator, FileStream bulkInsertFileStream, IDataReader fromReader)
    {
        Field commonField = new("Unused", OleDbType.Integer);
        StringBuilder bulkInsertRow = new();
        string value;

        bool addedFirstInsertField =
            // Handle creating bulk insert file data for each row...
            false;

        // Get all field data to create row for bulk insert
        foreach (Field field in toTable.Fields)
        {
            try
            {
                // Lookup field in common field list
                commonField = fieldCollection[field.Name];

                if (commonField is not null)
                {
                    // Found it, so use it...
                    commonField.Value = fromReader[field.Name];
                }
                else
                {
                    // Otherwise just use existing destination field
                    commonField = field;
                    commonField.Value = "";
                }
            }
            catch (Exception ex)
            {
                if (commonField is not null)
                {
                    commonField.Value = "";
                    OnSQLFailure($"Failed to get field value for '{toTable.Name}.{commonField.Name}'", ex);
                }
                else
                {
                    OnSQLFailure("Failed to get field value - field unknown.", ex);
                }
            }

            if (commonField is null)
                continue;

            // Get translated auto-inc value for field if possible...
            commonField.Value = DereferenceValue(sourceTable, commonField.Name, commonField.Value);

            // Get field value
            value = Convert.ToString(ToNonNullString(commonField.Value)).Trim();

            // We manually parse data type here instead of using SqlEncodedValue because data inserted
            // into bulk insert file doesn't need SQL encoding...
            switch (commonField.Type)
            {
                case OleDbType.Boolean:
                    if (value.Length > 0)
                    {
                        if (int.TryParse(value, out int tempValue))
                        {
                            if (Convert.ToInt32(tempValue) == 0)
                            {
                                value = "0";
                            }
                            else
                            {
                                value = "1";
                            }
                        }
                        else if (Convert.ToBoolean(value))
                        {
                            value = "1";
                        }
                        else
                        {
                            switch (value.Substring(0, 1).ToUpper())
                            {
                                case "Y":
                                case "T":
                                    value = "1";

                                    break;
                                case "N":
                                case "F":
                                    value = "0";

                                    break;
                                default:
                                    value = "0";

                                    break;
                            }
                        }
                    }

                    break;
                case OleDbType.DBTimeStamp:
                case OleDbType.DBDate:
                case OleDbType.Date:
                    if (value.Length > 0)
                    {
                        if (DateTime.TryParse(value, out DateTime tempValue))
                            value = tempValue.ToString("MM/dd/yyyy HH:mm:ss");
                    }

                    break;
                case OleDbType.DBTime:
                    if (value.Length > 0)
                    {
                        if (DateTime.TryParse(value, out DateTime tempValue))
                            value = tempValue.ToString("HH:mm:ss");
                    }

                    break;
            }

            // Make sure field value does not contain field terminator or row terminator
            value = value.Replace(fieldTerminator, DelimiterReplacement);
            value = value.Replace(rowTerminator, DelimiterReplacement);

            // Construct bulk insert row
            if (addedFirstInsertField)
                bulkInsertRow.Append(fieldTerminator);
            else
                addedFirstInsertField = true;

            bulkInsertRow.Append(value);
        }

        bulkInsertRow.Append(rowTerminator);

        // Add new row to temporary bulk insert file
        byte[] dataRow = BulkInsertEncoding.GetBytes(bulkInsertRow.ToString());
        bulkInsertFileStream.Write(dataRow, 0, dataRow.Length);
    }

    private void CompleteBulkInsert(Table toTable, int progressIndex, string bulkInsertFile, FileStream bulkInsertFileStream)
    {
        string bulkInsertSql = $"BULK INSERT {toTable.SQLEscapedName} FROM '{bulkInsertFile}'{(BulkInsertSettings.Length > 0 ? $" WITH ({BulkInsertSettings})" : "")}";

        double startTime = 0;
        double stopTime;

        DateTime todayMidNight = new(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day, 0, 0, 0);

        // Close bulk insert file stream
        bulkInsertFileStream.Close();

        BulkInsertExecuting?.SafeInvoke(this, new EventArgs<string>(toTable.Name)); //-V3083

        try
        {
            // Give system a few seconds to close bulk insert file (might have been real big)
            FilePath.WaitForReadLock(bulkInsertFile, 15);

            TimeSpan difference = DateTime.Now - todayMidNight;
            startTime = difference.TotalSeconds;

            toTable.Connection.ExecuteNonQuery(bulkInsertSql, Timeout);
        }
        catch (Exception ex)
        {
            BulkInsertException?.SafeInvoke(this, new EventArgs<string, string, Exception>(toTable.Name, bulkInsertSql, ex)); //-V3083
        }
        finally
        {
            TimeSpan difference = DateTime.Now - todayMidNight;
            stopTime = difference.TotalSeconds;

            if (Convert.ToInt32(startTime) == 0)
                startTime = stopTime;
        }

        try
        {
            FilePath.WaitForWriteLock(bulkInsertFile, 15);
            File.Delete(bulkInsertFile);
        }
        catch (Exception ex)
        {
            BulkInsertException?.SafeInvoke(this, new EventArgs<string, string, Exception>(toTable.Name, bulkInsertSql, new InvalidOperationException($"Failed to delete temporary bulk insert file \"{bulkInsertFile}\" due to exception [{ex.Message}], file will need to be manually deleted.", ex)));
        }

        BulkInsertCompleted?.SafeInvoke(this, new EventArgs<string, int, int>(toTable.Name, progressIndex, Convert.ToInt32(stopTime - startTime))); //-V3083
    }

    /// <summary>
    /// Lookup referential value for source table and update their information
    /// </summary>
    /// <param name="sourceTable"></param>
    /// <param name="fieldName"></param>
    /// <param name="value"></param>
    /// <param name="fieldStack"></param>
    /// <returns></returns>
    internal object DereferenceValue(Table sourceTable, string fieldName, object value, ArrayList? fieldStack = null)
    {
        // No need to attempt to deference null value
        if (Convert.IsDBNull(value) || value is null)
            return value;

        // If this field is referenced as a foreign key field by a primary key field that is auto-incremented, we
        // translate the auto-inc value if possible
        Field lookupField = sourceTable.Fields[fieldName];

        if (lookupField is { IsForeignKey: true })
        {
            Field referenceByField = lookupField.ReferencedBy;

            if (referenceByField.AutoIncrement)
            {
                // Return new auto-inc value
                if (referenceByField.AutoIncrementTranslations is null)
                    return value;

                object tempValue = referenceByField.AutoIncrementTranslations[Convert.ToString(value)];

                return tempValue ?? value;
            }

            bool inStack = false;
            int x;

            fieldStack ??= new ArrayList();

            // We don't want to circle back on ourselves
            for (x = 0; x <= fieldStack.Count - 1; x++)
            {
                if (ReferenceEquals(lookupField.ReferencedBy, fieldStack[x]))
                {
                    inStack = true;

                    break;
                }
            }

            // Traverse path to parent auto-inc field if it exists
            if (!inStack)
            {
                fieldStack.Add(lookupField.ReferencedBy);

                return DereferenceValue(referenceByField.Table, referenceByField.Name, value, fieldStack);
            }
        }

        return value;
    }

    private void ParseBulkInsertSettings(out string fieldTerminator, out string rowTerminator)
    {
        fieldTerminator = "";
        rowTerminator = "";

        foreach (string setting in BulkInsertSettings.Split(','))
        {
            string[] keyValue = setting.Split('=');

            if (keyValue.Length == 2)
            {
                if (string.Compare(keyValue[0].Trim(), "FIELDTERMINATOR", StringComparison.OrdinalIgnoreCase) == 0)
                    fieldTerminator = keyValue[1].Trim();
                else if (string.Compare(keyValue[0].Trim(), "ROWTERMINATOR", StringComparison.OrdinalIgnoreCase) == 0)
                    rowTerminator = keyValue[1].Trim();
            }
        }

        if (fieldTerminator.Length == 0)
            fieldTerminator = "\\t";

        if (rowTerminator.Length == 0)
            rowTerminator = "\\n";

        fieldTerminator = UnEncodeSetting(fieldTerminator);
        rowTerminator = UnEncodeSetting(rowTerminator);
    }

    // Generate un-encoded value for SQL statement
    private string UnEncodeSetting(string setting)
    {
        setting = RemoveQuotes(setting);

        setting = setting.Replace("\\\\", "\\");
        setting = setting.Replace("\\'", "'");
        setting = setting.Replace("\\\"", "\"");
        setting = setting.Replace("\\t", "\t");
        setting = setting.Replace("\\n", "\n");

        return setting;
    }

    // Remove single quotes from SQL statement
    private string RemoveQuotes(string value)
    {
        if (value.Substring(0, 1) == "'")
            value = value.Substring(2);

        if (value.Substring(value.Length - 1) == "'")
            value = value.Substring(0, value.Length - 1);

        return value;
    }

    private string ToNonNullString(object value, string nonNullValue = "") => StringExtensions.StringExtensions.ToNonNullString(value, nonNullValue);

    #endregion
}
