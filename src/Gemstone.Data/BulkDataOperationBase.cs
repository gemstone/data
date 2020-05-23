//******************************************************************************************************
//  BulkDataOperationBase.cs - Gbtc
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
//  08/21/2008 - Mihir Brahmbhatt
//       Converted to C# extensions.
//  09/27/2010 - Mihir Brahmbhatt
//       Edited code comments.
//  12/20/2012 - Starlynn Danyelle Gilliam
//       Modified Header.
//  12/13/2019 - J. Ritchie Carroll
//      Migrated to Gemstone libraries.
//
//******************************************************************************************************

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Gemstone.EventHandlerExtensions;

#pragma warning disable CA2235
#pragma warning disable CS8603
#pragma warning disable CS8604
#pragma warning disable CS8618
#pragma warning disable CS8625

// ReSharper disable InconsistentNaming
// ReSharper disable UnusedMemberInSuper.Global
namespace Gemstone.Data
{
    #region [ IBulkDataOperation ]

    /// <summary>
    /// This is the common interface for any bulk data operation
    /// </summary>
    public interface IBulkDataOperation
    {
        /// <summary>
        /// Get the status information of table progress
        /// </summary>
        event EventHandler<EventArgs> TableProgressStatus;

        /// <summary>
        /// Get the information of Row progress of table
        /// </summary>
        event EventHandler<EventArgs> RowProgressStatus;

        /// <summary>
        /// Get the information of overall progress of Migration utility
        /// </summary>
        event EventHandler<EventArgs> OverallProgressStatus;

        /// <summary>
        /// Get the information of exception while processing SQL statement
        /// </summary>
        event EventHandler<EventArgs> SQLFailure;

        /// <summary>
        /// Get the work table information
        /// </summary>
        Tables WorkTables { get; }

        /// <summary>
        /// From schema information
        /// </summary>
        Schema FromSchema { get; set; }

        /// <summary>
        /// To Schema information
        /// </summary>
        Schema ToSchema { get; set; }

        /// <summary>
        /// Get the row report interval information
        /// </summary>
        int RowReportInterval { get; set; }

        /// <summary>
        /// Get or set time out for SQL statement
        /// </summary>
        int Timeout { get; set; }

        /// <summary>
        /// Execute a method of object
        /// </summary>
        void Execute();

        /// <summary>
        /// Close the object connection
        /// </summary>
        void Close();
    }

    #endregion

    /// <summary>
    /// This class defines a common set of functionality that any bulk data operation implementation can use 
    /// </summary>
    public abstract class BulkDataOperationBase : IBulkDataOperation, IDisposable
    {
        #region [ Members ]

        // Fields - Variables Declaration

        /// <summary>
        /// Implementer can use this variable to track overall progress 
        /// </summary>
        protected long OverallProgress;

        /// <summary>
        /// This is initialized to the overall total number of records to be processed 
        /// </summary>
        protected long OverallTotal;

        /// <summary>
        /// Defines interval for reporting row progress 
        /// </summary>
        private int m_rowReportInterval;

        /// <summary>
        /// Tables value 
        /// </summary>
        protected Tables TableCollection;

        /// <summary>
        /// Flag to check referential integrity
        /// </summary>
        protected bool UseFromSchemaRi;

        // Events

        /// <summary>
        /// Get the status information of table progress
        /// </summary>
        public event EventHandler<EventArgs> TableProgressStatus;

        /// <summary>
        /// Get the information of Row progress of table
        /// </summary>
        public event EventHandler<EventArgs> RowProgressStatus;

        /// <summary>
        /// Get the information of overall progress of Migration utility
        /// </summary>
        public event EventHandler<EventArgs> OverallProgressStatus;

        /// <summary>
        /// Get the information of exception while processing SQL statement
        /// </summary>
        public event EventHandler<EventArgs> SQLFailure;

        private Schema m_fromSchema;

        #endregion

        #region [ Constructors ]

        /// <summary>
        /// Default Constructor
        /// </summary>
        protected BulkDataOperationBase()
        {
            m_rowReportInterval = 5;
            Timeout = 120;
            UseFromSchemaRi = true;
        }

        /// <summary>
        /// Constructor with parameters
        /// </summary>
        /// <param name="fromConnectString">Source database connection string</param>
        /// <param name="toConnectString">Destination database connection string</param>
        protected BulkDataOperationBase(string fromConnectString, string toConnectString) : this()
        {
            FromSchema = new Schema(fromConnectString, TableType.Table, false, false);
            ToSchema = new Schema(toConnectString, TableType.Table, false, false);
            TableCollection = new Tables(FromSchema);
        }

        /// <summary>
        /// Constructor with parameters
        /// </summary>
        /// <param name="fromSchema">Source Schema</param>
        /// <param name="toSchema">Destination Schema</param>
        protected BulkDataOperationBase(Schema fromSchema, Schema toSchema) : this()
        {
            FromSchema = fromSchema;
            ToSchema = toSchema;
            TableCollection = new Tables(FromSchema);
        }

        #endregion

        #region [ Properties ]

        /// <summary>
        /// Get or set Source schema
        /// </summary>
        public Schema FromSchema
        {
            get => m_fromSchema;
            set
            {
                m_fromSchema = value;
                TableCollection.Parent = value;
            }
        }

        /// <summary>
        /// Get or set destination schema
        /// </summary>
        public Schema ToSchema { get; set; }

        /// <summary>
        /// Get or set number of rows to process before raising progress events
        /// </summary>
        public virtual int RowReportInterval
        {
            get => m_rowReportInterval;
            set => m_rowReportInterval = value;
        }

        /// <summary>
        /// Get or set Maximum number of seconds to wait when processing a SQL command before timing out.
        /// </summary>
        public int Timeout { get; set; }

        /// <summary>
        /// Get or set - use referential integrity information from source/to destination database during data processing
        /// </summary>
        public virtual bool UseFromSchemaReferentialIntegrity
        {
            get => UseFromSchemaRi;
            set => UseFromSchemaRi = value;
        }

        /// <summary>
        /// These are the tables that were found in both source and destination to be used for data operation...
        /// </summary>
        public virtual Tables WorkTables => TableCollection;

        /// <summary>
        /// Get list of tables to be excluded during data processing
        /// </summary>
        public List<string> ExcludedTables { get; } = new List<string>();

        #endregion

        #region [ Methods ]

        /// <summary>
        /// Dispose
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            Close();
        }

        /// <summary>
        /// Close source and destination schema
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA1816:CallGCSuppressFinalizeCorrectly")]
        public virtual void Close()
        {
            if (FromSchema != null)
                FromSchema.Close();

            if (ToSchema != null)
                ToSchema.Close();

            FromSchema = null;
            ToSchema = null;

            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Dispose
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA1816:CallGCSuppressFinalizeCorrectly")]
        [SuppressMessage("Microsoft.Design", "CA1063:ImplementIDisposableCorrectly")]
        void IDisposable.Dispose()
        {
            Close();
        }

        /// <summary>
        /// Analyze data schema before data processing
        /// </summary>
        public virtual void Analyze()
        {
            Table lookupTable;

            FromSchema.ImmediateClose = false;
            FromSchema.Analyze();

            ToSchema.ImmediateClose = false;
            ToSchema.Analyze();

            ExcludedTables.Sort();

            TableCollection.Clear();

            // We preprocess which tables we are going to access for data operation...
            foreach (Table table in FromSchema.Tables)
            {
                // Bypass excluded tables
                if (ExcludedTables.BinarySearch(table.MapName) < 0)
                {
                    // Lookup table name in destination data source by map name
                    lookupTable = ToSchema.Tables.FindByMapName(table.MapName);

                    if (lookupTable != null)
                    {
                        // If user requested to use referential integrity of destination tables then
                        // we use process priority of those tables instead...
                        if (!UseFromSchemaRi)
                            table.Priority = lookupTable.Priority;

                        table.Process = true;
                        TableCollection.Add(table);
                    }
                }
            }
        }

        /// <summary>
        /// Executes bulk data operation.
        /// </summary>
        public abstract void Execute();

        /// <summary>
        /// Raise an event if table change in data processing
        /// </summary>
        /// <param name="tableName">Table name in data processing</param>
        /// <param name="executed">Status of data processing on table</param>
        /// <param name="currentTable">current table index in data processing</param>
        /// <param name="totalTables">total table count in data processing</param>
        protected virtual void OnTableProgress(string tableName, bool executed, int currentTable, int totalTables)
        {
            TableProgressStatus?.SafeInvoke(this, new EventArgs<string, bool, int, int>(tableName, executed, currentTable, totalTables)); //-V3083
        }

        /// <summary>
        /// Raise an event while change row in data processing
        /// </summary>
        /// <param name="tableName">Table name in data processing</param>
        /// <param name="currentRow">current row index in data processing</param>
        /// <param name="totalRows">total rows needs to be process in data processing</param>
        protected virtual void OnRowProgress(string tableName, int currentRow, int totalRows)
        {
            RowProgressStatus?.SafeInvoke(this, new EventArgs<string, int, int>(tableName, currentRow, totalRows)); //-V3083
        }

        /// <summary>
        /// Raise an event to show overall progress of data processing
        /// </summary>
        /// <param name="current">Current index of tables in data processing</param>
        /// <param name="total">Total table count in data processing</param>
        protected virtual void OnOverallProgress(int current, int total)
        {
            OverallProgressStatus?.SafeInvoke(this, new EventArgs<int, int>(current, total)); //-V3083
        }

        /// <summary>
        /// Raise an event if SQL statement fail
        /// </summary>
        /// <param name="sql">SQL statement information</param>
        /// <param name="ex">exception information</param>
        protected virtual void OnSQLFailure(string sql, Exception ex)
        {
            SQLFailure?.SafeInvoke(this, new EventArgs<string, Exception>(sql, ex)); //-V3083
        }

        #endregion
    }
}
