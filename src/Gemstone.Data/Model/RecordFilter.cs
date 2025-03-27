//******************************************************************************************************
//  RecordFilter.cs - Gbtc
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
// ReSharper disable StaticMemberInGenericType
// ReSharper disable RedundantCatchClause

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Gemstone.Data.Model;

/// <summary>
/// Defines a filter that can be applied to queries.
/// </summary>
/// <remarks>
/// For backend restrictions <see cref="RecordRestriction"/> should be used. This class
/// is intended to be used for user initiated searches and filters in the user interface.
/// </remarks>
public class RecordFilter<T> : IRecordFilter where T : class, new()
{
    #region [ Members ]

    // Fields
    private string m_operator = s_validOperators[0];

    #endregion

    #region [ Properties ]

    /// <inheritdoc/>
    public string FieldName { get; set; } = string.Empty;

    /// <inheritdoc/>
    public required string SearchParameter { get; set; }

    /// <inheritdoc/>
    public string Operator
    {
        get => m_operator;
        set
        {
            if (s_validOperators.Contains(value, StringComparer.OrdinalIgnoreCase))
                m_operator = value;
            else
                throw new NotSupportedException($"{value} is not a valid operator");
        }
    }

    /// <inheritdoc/>
    public bool SupportsEncrypted => s_encryptedOperators.Contains(m_operator);

    /// <inheritdoc/>
    public PropertyInfo? ModelProperty => typeof(T).GetProperty(FieldName);

    #endregion

    #region [ Methods ]

    /// <inheritdoc/>
    public RecordRestriction GenerateRestriction(ITableOperations tableOperations)
    {
        Func<IRecordFilter, RecordRestriction>? transform = TableOperations<T>.GetSearchExtensionMethod(FieldName);

        if (transform is not null)
        {
            try
            {
                return transform(this);
            }
            catch
            {
                // Fall through to normal search if not debugging
            #if DEBUG
                throw;
            #endif
            }
        }

        if (ModelProperty is null && !TableOperations<T>.IsSearchableField(FieldName))
            throw new ArgumentException($"{FieldName} is not a valid field for {typeof(T).Name}");

        if (string.IsNullOrEmpty(SearchParameter))
            return new RecordRestriction($"{FieldName} {m_operator} NULL");

        // Convert search parameters to the interpreted value for the specified field, i.e., encrypting or
        // returning any intermediate IDbDataParameter value as needed:
        string interpretedValue = tableOperations.GetInterpretedFieldValue(FieldName, SearchParameter) as string ?? string.Empty;

        if (m_operator.Equals("LIKE", StringComparison.OrdinalIgnoreCase) || m_operator.Equals("NOT LIKE", StringComparison.OrdinalIgnoreCase))
        {
            interpretedValue = string.IsNullOrEmpty(SearchParameter) ? tableOperations.WildcardChar : SearchParameter.Replace("*", tableOperations.WildcardChar);
            interpretedValue = $"'{interpretedValue}'";
        }
        else if (m_operator.Equals("IN", StringComparison.OrdinalIgnoreCase) || m_operator.Equals("NOT IN", StringComparison.OrdinalIgnoreCase))
        {
            // Split the SearchParameter on commas, trim whitespace, and wrap each value in single quotes
            IEnumerable<string> values = SearchParameter
                .Split(',')
                .Select(value => $"'{value.Trim()}'");

            interpretedValue = string.Join(", ", values);
        }
        else
        {
            interpretedValue = $"'{interpretedValue}'";
        }

        return s_groupOperators.Contains(m_operator, StringComparer.OrdinalIgnoreCase) ? 
            new RecordRestriction($"{FieldName} {m_operator} ({interpretedValue})") : 
            new RecordRestriction($"{FieldName} {m_operator} {interpretedValue}");
    }

    #endregion

    #region [ Static ]

    // Static Fields
    private static readonly string[] s_validOperators = ["=", "<>", "<", ">", "IN", "NOT IN", "LIKE", "NOT LIKE", "<=", ">=", "IS", "IS NOT"];
    private static readonly string[] s_groupOperators = ["IN", "NOT IN"];
    private static readonly string[] s_encryptedOperators = ["IN", "NOT IN", "=", "<>", "IS", "IS NOT"];

    #endregion
}
