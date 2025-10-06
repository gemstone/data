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
using System.Text.Json;
using Gemstone.Diagnostics;

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
    public PropertyInfo? ModelProperty => field ??= typeof(T).GetProperty(FieldName);

    /// <inheritdoc/>
    public required object? SearchParameter
    {
        get;
        set
        {
            switch (value)
            {
                case null:
                    field = DBNull.Value;
                    break;
                case Array array:
                    {
                        object?[] typedArray = new object[array.Length];

                        for (int i = 0; i < array.Length; i++)
                        {
                            object? element = array.GetValue(i);
                            object? typedElement = ModelProperty is null ? element : Common.TypeConvertFromString(element?.ToString() ?? "", ModelProperty.PropertyType);
                            typedArray[i] = typedElement;
                        }

                        field = typedArray;
                        break;
                    }
                default:
                    {
                        if (value is JsonElement el)
                        {
                            //try to cast based on ValueKind
                            if (el.ValueKind == JsonValueKind.String && ModelProperty is not null && ModelProperty.PropertyType == typeof(DateTime))
                                field = Common.TypeConvertFromString(el.GetString() ?? "", typeof(DateTime));
                            else if (el.ValueKind == JsonValueKind.String)
                                field = el.GetString();
                            else if (el.ValueKind == JsonValueKind.Number)
                                field = el.GetDouble();
                            else if (el.ValueKind == JsonValueKind.True || el.ValueKind == JsonValueKind.False)
                                field = el.GetBoolean();
                            else if (el.ValueKind == JsonValueKind.Array)
                            {
                                //This doesnt handle DateTimes
                                field = el.EnumerateArray()
                                          .Select(e =>
                                              e.ValueKind switch
                                              {
                                                  JsonValueKind.String => (object?)e.GetString(),
                                                  JsonValueKind.Number => e.TryGetInt64(out var i64) ? i64 : e.GetDouble(),
                                                  JsonValueKind.True => true,
                                                  JsonValueKind.False => false,
                                                  JsonValueKind.Null => DBNull.Value,
                                                  _ => e.ToString()
                                              })
                                          .ToArray();
                            }
                            else
                                field = el.ToString();
                        }
                        //This isn't properly working at all times.
                        else if (ModelProperty is not null)
                        {
                            string image = (value.ToString() ?? "").Trim();

                            // Check for JSON formatted array
                            if (image.StartsWith('[') && image.EndsWith(']'))
                            {
                                using var doc = JsonDocument.Parse(image);
                                string[] elements = [.. doc.RootElement.EnumerateArray().Select(e => e.GetString() ?? e.ToString())];

                                object?[] typedArray = new object[elements.Length];

                                for (int i = 0; i < elements.Length; i++)
                                {
                                    string element = elements[i];
                                    object? typedElement = ModelProperty is null ? element : Common.TypeConvertFromString(element, ModelProperty.PropertyType);
                                    typedArray[i] = typedElement;
                                }

                                field = typedArray;
                            }
                            else
                            {
                                field = Common.TypeConvertFromString(image, ModelProperty.PropertyType);
                            }
                        }
                        else
                        {
                            field = value.ToString();
                        }

                        break;
                    }
            }
        }
    }

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

    /// <summary>
    /// Gets the collection of supported wildcard operators.
    /// </summary>
    public static IReadOnlyCollection<string> WildCardOperators => s_wildCardOperators;
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
            catch (Exception ex)
            {
                Logger.SwallowException(ex, $"{nameof(RecordFilter<>)}.{nameof(GenerateRestriction)} transform operation");
            }
        }

        if (ModelProperty is null && !TableOperations<T>.IsSearchableField(FieldName))
            throw new ArgumentException($"{FieldName} is not a valid field for {typeof(T).Name}");

        if (SearchParameter is not object?[] searchParameters)
            searchParameters = SearchParameter is not null ? [SearchParameter] : [];

        int parameterCount = searchParameters.Length;

        if (parameterCount == 0)
            return new RecordRestriction($"{FieldName} {m_operator} NULL");

        // Convert search parameters to the interpreted value for the specified field, i.e., encrypting or
        // returning any intermediate IDbDataParameter value as needed:
        for (int i = 0; i < parameterCount; i++)
        {
            searchParameters[i] = tableOperations.GetInterpretedFieldValue(FieldName, searchParameters[i]);

            if (s_wildCardOperators.Contains(m_operator, StringComparer.OrdinalIgnoreCase) && searchParameters[i] is string stringVal)
            {
                searchParameters[i] = stringVal.Replace("*", tableOperations.WildcardChar);
            }
        }

        if (!s_groupOperators.Contains(m_operator, StringComparer.OrdinalIgnoreCase))
            return new RecordRestriction($"{FieldName} {m_operator} {{0}}", searchParameters);

        string[] parameters = new string[parameterCount];

        for (int i = 0; i < parameterCount; i++)
            parameters[i] = $"{{{i}}}";

        return new RecordRestriction($"{FieldName} {m_operator} ({string.Join(',', parameters)})", searchParameters);
    }

    #endregion

    #region [ Static ]

    // Static Fields
    private static readonly string[] s_validOperators = ["=", "<>", "<", ">", "IN", "NOT IN", "LIKE", "NOT LIKE", "<=", ">=", "IS", "IS NOT"];
    private static readonly string[] s_groupOperators = ["IN", "NOT IN"];
    private static readonly string[] s_encryptedOperators = ["IN", "NOT IN", "=", "<>", "IS", "IS NOT"];
    private static readonly string[] s_wildCardOperators = ["NOT LIKE", "LIKE"];
    #endregion
}
