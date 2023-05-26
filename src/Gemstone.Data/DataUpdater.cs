//******************************************************************************************************
//  DataUpdater.cs - Gbtc
//
//  Copyright © 2018, Grid Protection Alliance.  All Rights Reserved.
//
//  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements. See
//  the NOTICE file distributed with this work for additional information regarding copyright ownership.
//  The GPA licenses this file to you under the MIT License (MIT), the "License"; you may not use this
//  file except in compliance with the License. You may obtain a copy of the License at:
//
//      http://opensource.org/licenses/MIT
//
//  Unless agreed to in writing, the subject software distributed under the License is distributed on an
//  "AS-IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. Refer to the
//  License for the specific language governing permissions and limitations.
//
//  Code Modification History:
//  ----------------------------------------------------------------------------------------------------
//  11/12/2018 - Stephen Wills
//       Generated original version of source code from code written in 2003.
//  12/13/2019 - J. Ritchie Carroll
//      Migrated to Gemstone libraries.
//
//******************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using Gemstone.Data.DataExtensions;
using Gemstone.EventHandlerExtensions;

#pragma warning disable CS8618

namespace Gemstone.Data
{
    // Note: user must define primary key fields in the database or through code to be used for updates for each table
    // in the table collection in order for the updates to occur, hence any tables which have no key fields defined
    // yet appear in the table collection will not be updated...

    /// <summary>
    /// This class defines a common set of functionality that any data operation implementation can use 
    /// </summary>
    public class DataUpdater : BulkDataOperationBase
    {
        #region [ Members ]

        // Events

        /// <summary>
        /// Disposed event.
        /// </summary>
        public event EventHandler Disposed;

        #endregion

        #region [ Constructors ]

        /// <summary>
        /// Creates a new <see cref="DataUpdater"/>.
        /// </summary>
        public DataUpdater()
        {
        }

        /// <summary>
        /// Creates a new <see cref="DataUpdater"/>.
        /// </summary>
        public DataUpdater(string fromConnectString, string toConnectString)
            : base(fromConnectString, toConnectString)
        {
        }

        /// <summary>
        /// Creates a new <see cref="DataUpdater"/>.
        /// </summary>
        public DataUpdater(Schema fromSchema, Schema toSchema)
            : base(fromSchema, toSchema)
        {
        }

        #endregion

        #region [ Methods ]

        /// <summary>
        /// Close
        /// </summary>
        public override void Close()
        {
            base.Close();

            Disposed?.SafeInvoke(this, EventArgs.Empty);
        }

        /// <summary>
        /// Execute this <see cref="DataUpdater"/>
        /// </summary>
        public override void Execute()
        {
            List<Table> tablesList = new();
            Table tableLookup;
            int x;

            if (TableCollection.Count == 0)
                Analyze();

            if (TableCollection.Count == 0)
                throw new NullReferenceException("No tables to process even after analyze.");

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

            // Begin updating data in destination tables
            for (x = 0; x <= tablesList.Count - 1; x++)
            {
                Table table = tablesList[x];

                // Lookup table name in destination datasource
                tableLookup = ToSchema.Tables.FindByMapName(table.MapName);

                if (tableLookup is not null)
                {
                    // We can only do Sql updates where key fields are defined...
                    if (table.RowCount > 0 && (UseFromSchemaRi ? table.PrimaryKeyFieldCount : tableLookup.PrimaryKeyFieldCount) > 0)
                    {
                        // Inform clients of table update
                        OnTableProgress(table.Name, true, x + 1, tablesList.Count);

                        // Update destination table based on source table records
                        ExecuteUpdates(table, tableLookup);
                    }
                    else
                    {
                        // Inform clients of table skip
                        OnTableProgress(table.Name, false, x + 1, tablesList.Count);

                        // If we skipped rows because of lack of key fields, make sure and
                        // synchronize overall progress event
                        if (table.RowCount > 0)
                        {
                            OverallProgress += table.RowCount;
                            OnOverallProgress((int)OverallProgress, (int)OverallTotal);
                        }
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
        /// Execute a command to update data from source to destination table
        /// </summary>
        /// <param name="fromTable">Source table</param>
        /// <param name="toTable">Destination table</param>
        private void ExecuteUpdates(Table fromTable, Table toTable)
        {
            Table sourceTable = UseFromSchemaRi ? fromTable : toTable;
            Field lookupField;
            Field commonField;

            // Progress process variables
            int progress = 0;

            // Bulk update variables
            string updateSQLStub;
            StringBuilder updateSQL;
            StringBuilder whereSQL;
            string value;
            bool isPrimary;
            bool addedFirstUpdate;

            // Create a field list of all of the common fields in both tables
            // Use toTable as the parent to retrieve the appropriate SQLEncodedValues
            Fields fieldsCollection = new(toTable);

            foreach (Field fld in fromTable.Fields)
            {
                // Lookup field name in destination table
                lookupField = toTable.Fields[fld.Name];

                if (lookupField is not null)
                {
                    // We currently don't handle binary fields...
                    if (!(fld.Type is OleDbType.Binary or OleDbType.LongVarBinary or OleDbType.VarBinary) && !(lookupField.Type is OleDbType.Binary or OleDbType.LongVarBinary or OleDbType.VarBinary))
                    {
                        // Copy field information from destination field
                        if (UseFromSchemaRi)
                        {
                            commonField = new Field(fld.Name, fld.Type);
                            commonField.AutoIncrement = fld.AutoIncrement;
                        }
                        else
                        {
                            commonField = new Field(lookupField.Name, lookupField.Type);
                            commonField.AutoIncrement = lookupField.AutoIncrement;
                        }

                        fieldsCollection.Add(commonField);
                    }
                }
            }

            // Exit if no common field names were found
            if (fieldsCollection.Count == 0)
            {
                OverallProgress += fromTable.RowCount;

                return;
            }

            int totalProgress = fromTable.RowCount;
            OnRowProgress(fromTable.Name, 0, totalProgress);
            OnOverallProgress((int)OverallProgress, (int)OverallTotal);

            // Execute source query
            using (IDataReader fromReader = fromTable.Connection.ExecuteReader($"SELECT {fieldsCollection.GetList(sqlEscapeFunction: fromTable.Parent.Parent.SQLEscapeName)} FROM {fromTable.SQLEscapedName}", CommandBehavior.SequentialAccess, Timeout))
            {
                // Create Sql update stub
                updateSQLStub = $"UPDATE {toTable.SQLEscapedName} SET ";

                // Insert data for each row...
                while (fromReader.Read())
                {
                    updateSQL = new StringBuilder(updateSQLStub);
                    whereSQL = new StringBuilder();
                    addedFirstUpdate = false;

                    // Coerce all field data into proper Sql format
                    foreach (Field fld in fieldsCollection)
                    {
                        try
                        {
                            fld.Value = fromReader[fld.Name];
                        }
                        catch (Exception ex)
                        {
                            fld.Value = "";
                            OnSQLFailure($"Failed to get field value for [{sourceTable.Name}.{fld.Name}]", ex);
                        }

                        // Check to see if this is a key field
                        lookupField = sourceTable.Fields[fld.Name];

                        if (lookupField is not null)
                            isPrimary = lookupField.IsPrimaryKey;
                        else
                            isPrimary = false;

                        value = fld.SQLEncodedValue;

                        if (!value.Equals("NULL", StringComparison.CurrentCultureIgnoreCase))
                        {
                            if (isPrimary)
                            {
                                if (whereSQL.Length == 0)
                                    whereSQL.Append(" WHERE ");
                                else
                                    whereSQL.Append(" AND ");

                                whereSQL.Append(fld.SQLEscapedName);
                                whereSQL.Append(" = ");
                                whereSQL.Append(value);
                            }
                            else
                            {
                                if (addedFirstUpdate)
                                    updateSQL.Append(", ");
                                else
                                    addedFirstUpdate = true;

                                updateSQL.Append(fld.SQLEscapedName);
                                updateSQL.Append(" = ");
                                updateSQL.Append(value);
                            }
                        }
                    }

                    if (whereSQL.Length > 0)
                    {
                        // Add where criteria to Sql update statement
                        updateSQL.Append(whereSQL);

                        try
                        {
                            // Update record in destination table
                            if (addedFirstUpdate)
                                toTable.Connection.ExecuteNonQuery(updateSQL.ToString(), Timeout);
                        }
                        catch (Exception ex)
                        {
                            OnSQLFailure(updateSQL.ToString(), ex);
                        }
                    }
                    else
                    {
                        OnSQLFailure(updateSQL.ToString(), new DataException("ERROR: No \"WHERE\" criteria was generated for Sql update statement, primary key value missing?  Update not performed"));
                    }

                    progress += 1;
                    OverallProgress += 1;

                    if (progress % RowReportInterval == 0)
                    {
                        OnRowProgress(fromTable.Name, progress, totalProgress);
                        OnOverallProgress((int)OverallProgress, (int)OverallTotal);
                    }
                }

                fromReader.Close();
            }

            OnRowProgress(fromTable.Name, totalProgress, totalProgress);
            OnOverallProgress((int)OverallProgress, (int)OverallTotal);
        }

        #endregion
    }
}
