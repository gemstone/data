//******************************************************************************************************
//  IRecordFilter.cs - Gbtc
//
//  Copyright © 2024, Grid Protection Alliance.  All Rights Reserved.
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
//  03/04/2024 - C. Lackner
//       Generated original version of source code.
//
//******************************************************************************************************

using System.Reflection;

namespace Gemstone.Data.Model;

/// <summary>
/// Defines an interface for a filter that can be applied to queries.
/// </summary>
/// <remarks>
/// For Backend Restrictions <see cref="RecordRestriction"/> should be used.
/// This is inteded to be used for user initiated seraches and filters in the User Interface.
/// </remarks>
public interface IRecordFilter
{
    #region [ Properties ]

    /// <summary>
    /// The Name of the field to be searched.
    /// </summary>
    string FieldName { get; set; }

    /// <summary>
    /// The value to be searched for.
    /// </summary>
    object? SearchParameter { get; set; }

    /// <summary>
    /// The Operator to be used for the Search.
    /// </summary>
    public string Operator { get; set; }

    /// <summary>
    /// Indicates whether this <see cref="IRecordFilter"/> will work on encrypted fields.
    /// </summary>
    public bool SupportsEncrypted { get; }

    /// <summary>
    /// The <see cref="PropertyInfo"/> of the Model that this <see cref="IRecordFilter"/> applies to.
    /// </summary>
    /// <remarks>
    /// This will return null if the <see cref="IRecordFilter"/> is not associated with a Property.
    /// </remarks>
    public PropertyInfo? ModelProperty { get; }

    #endregion

    #region [ Methods ]

    /// <summary>
    /// Generates a <see cref="RecordRestriction"/> that corresponds to this <see cref="IRecordFilter"/>.
    /// </summary>
    public RecordRestriction GenerateRestriction();
        
    #endregion
}
