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
using System.Text.RegularExpressions;
using Gemstone.Reflection.MemberInfoExtensions;

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
    public object? SearchParameter { get; set; }

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
        if (!IsValidField(FieldName))
            throw new ArgumentException($"{FieldName} is not a valid field for {typeof(T).Name}");

        IEnumerable<MethodInfo> methods = typeof(T).GetMethods(BindingFlags.Public | BindingFlags.Static);

        MethodInfo? transform = methods.FirstOrDefault(info =>
            info.TryGetAttribute(out SearchExtensionAttribute? searchExtension) &&
            Regex.IsMatch(FieldName, searchExtension.FieldMatch));

        if (transform is not null)
        {
            try
            {
                if (transform.Invoke(null, [this]) is RecordRestriction recordRestriction)
                    return recordRestriction;
            }
            catch
            {
                // Fall through to normal search if not debugging
            #if DEBUG
                throw;
            #endif
            }
        }

        if (SearchParameter is not object?[] searchParameters) 
            searchParameters = SearchParameter is not null ? [SearchParameter] : [];

        int parameterCount = searchParameters.Length;

        if (parameterCount == 0)
            return new RecordRestriction($"{FieldName} {m_operator} NULL");

        // Convert search parameters to the interpreted value for the specified field, i.e., encrypting or
        // returning any intermediate IDbDataParameter value as needed:
        for (int i = 0; i < parameterCount; i++) 
            searchParameters[i] = tableOperations.GetInterpretedFieldValue(FieldName, searchParameters[i]);

        if (!s_groupOperators.Contains(m_operator, StringComparer.OrdinalIgnoreCase))
            return new RecordRestriction($"{FieldName} {m_operator} {{0}}", searchParameters);

        string[] parameters = new string[parameterCount];

        for (int i = 0; i < parameterCount; i++)
            parameters[i] = $"{{{i}}}";

        return new RecordRestriction($"{FieldName} {m_operator} ({string.Join(',', parameters)})", searchParameters);
    }

    private bool IsValidField(string fieldName)
    {
        if (ModelProperty is not null)
            return true;

        if (typeof(T).TryGetAttribute(out SearchableAttribute? searchableAttribute))
        {
            if (searchableAttribute.FieldNames.Contains(FieldName))
                return true;
        }

        IEnumerable<MethodInfo> methods = typeof(T).GetMethods(BindingFlags.Public | BindingFlags.Static);

        return methods.Any(info =>
            info.TryGetAttribute(out SearchExtensionAttribute? searchExtension) &&
            Regex.IsMatch(fieldName, searchExtension.FieldMatch));
    }

    #endregion

    #region [ Static ]

    // Static Fields
    private static readonly string[] s_validOperators = ["=", "<>", "<", ">", "IN", "NOT IN", "LIKE", "NOT LIKE", "<=", ">=", "IS", "IS NOT"];
    private static readonly string[] s_groupOperators = ["IN", "NOT IN"];
    private static readonly string[] s_encryptedOperators = ["IN", "NOT IN", "=", "<>", "IS", "IS NOT"];

    #endregion
}
