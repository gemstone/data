﻿//******************************************************************************************************
//  TableNameAttribute.cs - Gbtc
//
//  Copyright © 2016, Grid Protection Alliance.  All Rights Reserved.
//
//  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements. See
//  the NOTICE file distributed with this work for additional information regarding copyright ownership.
//  The GPA licenses this file to you under the MIT License (MIT), the "License"; you may
//  not use this file except in compliance with the License. You may obtain a copy of the License at:
//
//      http://opensource.org/licenses/MIT
//
//  Unless agreed to in writing, the subject software distributed under the License is distributed on an
//  "AS-IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. Refer to the
//  License for the specific language governing permissions and limitations.
//
//  Code Modification History:
//  ----------------------------------------------------------------------------------------------------
//  01/30/2016 - J. Ritchie Carroll
//       Generated original version of source code.
//  12/13/2019 - J. Ritchie Carroll
//      Migrated to Gemstone libraries.
//
//******************************************************************************************************

using System;

namespace gemstone.data.model
{
    /// <summary>
    /// Defines an attribute that will allow a custom table name for a modeled table instead of using
    /// the class name.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public sealed class TableNameAttribute : Attribute
    {
        /// <summary>
        /// Gets field name to use for property.
        /// </summary>
        public string TableName { get; }

        /// <summary>
        /// Creates a new <see cref="TableNameAttribute"/>.
        /// </summary>
        /// <param name="tableName">Table name to use for class.</param>
        public TableNameAttribute(string tableName) => TableName = tableName;
    }
}