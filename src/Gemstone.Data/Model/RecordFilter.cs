﻿//******************************************************************************************************
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
/// For Backend Restrictions <see cref="RecordRestriction"/> should be used.
/// This is inteded to be used for user initiated seraches and filters in the User Interface.
/// </remarks>
public class RecordFilter<T> : IRecordFilter where T : class, new()
{
    #region [ Members ]

    // Fields
    private string m_operator = s_validOperators[0];

    #endregion

    #region [ Properties ]

    /// <summary>
    /// Gets or sets the Name of the field to be searched.
    /// </summary>
    public string FieldName { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the value to be searched for.
    /// </summary>
    public object? SearchParameter { get; set; }

    /// <summary>
    /// Gets or sets the Operator to be used for the Search.
    /// </summary>
    /// <remarks>
    /// <para>The list of supported operators includes:</para>
    ///
    /// <list type="bullet">
    ///   <item>=</item>
    ///   <item><![CDATA[<>]]></item>
    ///   <item><![CDATA[<]]></item>
    ///   <item><![CDATA[>]]></item>
    ///   <item>IN</item>
    ///   <item>NOT IN</item>
    ///   <item>LIKE</item>
    ///   <item>NOT LIKE</item>
    ///   <item><![CDATA[<=]]></item>
    ///   <item><![CDATA[>=]]></item>
    /// </list>
    /// </remarks>
    /// <exception cref="NotSupportedException">Attempted to assign an operator that is not supported.</exception>
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

    /// <summary>
    /// Indicates whether this <see cref="RecordFilter{T}"/> will work on encrypted fields.
    /// </summary>
    public bool SupportsEncrypted => s_encryptedOperators.Contains(m_operator);

    /// <inheritdoc/>
    public PropertyInfo? ModelProperty => typeof(T).GetProperty(FieldName);

    #endregion

    #region [ Methods ]

    /// <summary>
    /// Generates a <see cref="RecordRestriction"/> that corresponds to this <see cref="RecordFilter{T}"/>.
    /// </summary>
    public RecordRestriction GenerateRestriction()
    {
        if (!IsValidField(FieldName))
            throw new ArgumentException($"{FieldName} is not a valid field for {typeof(T).Name}");

        IEnumerable<MethodInfo> methods = typeof(T).GetMethods(BindingFlags.Public | BindingFlags.Static);

        MethodInfo? transform = methods.FirstOrDefault(t =>
            t.TryGetAttribute(out SearchExtensionAttribute? searchExtension) &&
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

        if (s_groupOperators.Contains(m_operator, StringComparer.OrdinalIgnoreCase))
        {
            if (SearchParameter is not object[] searchParameter)
            {
                searchParameter = SearchParameter is not null
                    ? [SearchParameter]
                    : [];
            }

            int nParameters = searchParameter.Length;

            string[] parameters = new string[nParameters];
            for (int i = 0; i < nParameters; i++)
                parameters[i] = $"{{{i}}}";

            return new RecordRestriction($"{FieldName} {m_operator} ({string.Join(',', parameters)})", searchParameter);
        }

        return new RecordRestriction($"{FieldName} {m_operator} {{0}}", SearchParameter);
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

        return methods.Any(m =>
            m.TryGetAttribute(out SearchExtensionAttribute? searchExtension) &&
            Regex.IsMatch(fieldName, searchExtension.FieldMatch));
    }

    #endregion

    #region [ Static ]

    // Static Fields
    private static readonly string[] s_validOperators = ["=", "<>", "<", ">", "IN", "NOT IN", "LIKE", "NOT LIKE", "<=", ">="];
    private static readonly string[] s_groupOperators = ["IN", "NOT IN"];
    private static readonly string[] s_encryptedOperators = ["IN", "NOT IN", "=", "<>"];

    #endregion
}
