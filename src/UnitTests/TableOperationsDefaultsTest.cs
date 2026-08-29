//******************************************************************************************************
//  TableOperationsDefaultsTest.cs - Gbtc
//
//  Copyright © 2026, Grid Protection Alliance.  All Rights Reserved.
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
//  06/25/2026 - J. Ritchie Carroll
//       Generated original version of source code.
//
//******************************************************************************************************

using System;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using Gemstone.Data.Model;
using Gemstone.Expressions.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Gemstone.Data.UnitTests
{
    [TestClass]
    public class TableOperationsDefaultsTest
    {
        // Sample model with a standard constant default value and a value-expression default value.
        // The value expression uses only the always-registered "Guid" type, so it requires no external
        // setup (e.g., Settings) -- this lets us verify the create path without a live database.
        public class SampleModel
        {
            [PrimaryKey(true)]
            public int ID { get; set; }

            [DefaultValue(42)]
            public int Code { get; set; }

            [DefaultValueExpression("Guid.NewGuid()")]
            public Guid Token { get; set; }
        }

        private static AdoDataConnection CreateConnection()
        {
            // Connect using a no-op connection type so no live database is required.
            return new AdoDataConnection(null!, typeof(TestConnection));
        }

        [TestMethod]
        public void BaseTableOperations_NewRecord_AppliesOnlyDefaultValueAttribute()
        {
            using AdoDataConnection connection = CreateConnection();

            TableOperations<SampleModel> table = new(connection);
            SampleModel record = table.NewRecord();

            Assert.IsNotNull(record);
            Assert.AreEqual(42, record.Code);              // DefaultValueAttribute applied
            Assert.AreEqual(Guid.Empty, record.Token);     // DefaultValueExpressionAttribute NOT applied
        }

        [TestMethod]
        public void ExpressionTableOperations_NewRecord_AppliesValueExpressions()
        {
            using AdoDataConnection connection = CreateConnection();

            ExpressionTableOperations<SampleModel> table = new(connection);
            SampleModel record = table.NewRecord();

            Assert.IsNotNull(record);
            Assert.AreEqual(42, record.Code);              // DefaultValueAttribute applied
            Assert.AreNotEqual(Guid.Empty, record.Token);  // DefaultValueExpressionAttribute evaluated
        }

        [TestMethod]
        public void BaseTableOperations_ApplyRecordDefaults_AppliesOnlyDefaultValueAttribute()
        {
            using AdoDataConnection connection = CreateConnection();

            TableOperations<SampleModel> table = new(connection);
            SampleModel record = new();
            table.ApplyRecordDefaults(record);

            Assert.AreEqual(42, record.Code);
            Assert.AreEqual(Guid.Empty, record.Token);
        }

        // No-op connection that allows constructing an AdoDataConnection without a live database.
        public class TestConnection : DbConnection
        {
            public override string ConnectionString { get; set; } = string.Empty;
            public override int ConnectionTimeout => 0;
            public override string Database => string.Empty;
            public override string DataSource => string.Empty;
            public override string ServerVersion => string.Empty;
            public override ConnectionState State => ConnectionState.Open;
            public override void Open() { }
            public override void Close() { }
            protected override DbCommand CreateDbCommand() => null!;
            protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => null!;
            public override void ChangeDatabase(string databaseName) { }
        }
    }
}
