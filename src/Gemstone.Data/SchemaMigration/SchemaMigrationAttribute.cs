//******************************************************************************************************
//  GemstoneMigrationAttribute.cs - Gbtc
//
//  Copyright © 2024, Grid Protection Alliance.  All Rights Reserved.
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
//  04/22/2024 - Christoph Lackner
//       Generated original version of source code.
//
//******************************************************************************************************

using FluentMigrator;

namespace Gemstone.Data.SchemaMigration;

/// <summary>
/// Defines an attribute that will mark a class as the target of a database schema migration.
/// </summary>
/// <remarks>
/// In Gemstone, use this instead of the Fluent  <see cref="MigrationAttribute"/> for consistent versioning.
/// </remarks>
public class SchemaMigrationAttribute : MigrationAttribute
{
    /// <summary>
    /// Creates a new <see cref="SchemaMigrationAttribute"/>.
    /// </summary>
    /// <param name="branchNumber">Branch number of the migration.</param>
    /// <param name="year">Year of the migration.</param>
    /// <param name="month">Month of the migration.</param>
    /// <param name="day">Day of the migration.</param>
    /// <param name="author">Author of the migration.</param>
    public SchemaMigrationAttribute(int branchNumber, int year, int month, int day, string author)
        : base(CalculateValue(branchNumber, year, month, day))
    {
        Author = author;
    }

    /// <summary>
    /// Gets the author of the migration.
    /// </summary>
    public string Author { get; private set; }
    
    private static long CalculateValue(int branchNumber, int year, int month, int day)
    {
        return branchNumber * 1000000000000L + year * 100000000L + month * 1000000L + day * 10000L;
    }
}
