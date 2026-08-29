//******************************************************************************************************
//  ExpressionTableOperations.cs - Gbtc
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
// ReSharper disable StaticMemberInGenericType

using System;
using System.Collections.Generic;
using Gemstone.Expressions.Evaluator;
using Gemstone.Expressions.Model;

namespace Gemstone.Data.Model;

/// <summary>
/// Defines database operations for a modeled table that additionally evaluates attribute-based value
/// expressions, i.e., <see cref="DefaultValueExpressionAttribute"/> and
/// <see cref="UpdateValueExpressionAttribute"/>, when creating, defaulting and updating records.
/// </summary>
/// <typeparam name="T">Modeled table.</typeparam>
/// <remarks>
/// <para>
/// The base <see cref="TableOperations{T}"/> is a pure POCO operator: it honors only the standard
/// <see cref="System.ComponentModel.DefaultValueAttribute"/> and carries no dependency on the
/// <see cref="Gemstone.Expressions.Evaluator.TypeRegistry"/>. This keeps the simple case working without
/// any external setup, e.g., <c>new TableOperations&lt;MyModel&gt;(connection).NewRecord()</c> will never
/// fail because of an unconfigured value-expression dependency such as <c>Settings</c>.
/// </para>
/// <para>
/// Use <see cref="ExpressionTableOperations{T}"/> to opt in to the richer value-expression behavior. When
/// using this type, the caller is responsible for ensuring any dependencies referenced by the model's
/// value expressions (e.g., <c>Settings</c>, <c>UserInfo</c>, custom symbols registered via the
/// <see cref="TypeRegistry"/>) are properly initialized.
/// </para>
/// </remarks>
public class ExpressionTableOperations<T> : TableOperations<T> where T : class, new()
{
    // Nested Types
    private class CurrentScope : ValueExpressionScopeBase<T>
    {
        // Define instance variables exposed to ValueExpressionAttributeBase expressions
        #pragma warning disable 169, 414, 649, CS8618
        public TableOperations<T> TableOperations;
        public AdoDataConnection Connection;
        #pragma warning restore 169, 414, 649, CS8618
    }

    /// <summary>
    /// Creates a new <see cref="ExpressionTableOperations{T}"/> instance.
    /// </summary>
    /// <param name="connection"><see cref="AdoDataConnection"/> instance to use for database operations.</param>
    /// <param name="customTokens">Custom run-time tokens to apply to any modeled <see cref="AmendExpressionAttribute"/> values.</param>
    public ExpressionTableOperations(AdoDataConnection connection, IEnumerable<KeyValuePair<string, string>>? customTokens = null)
        : base(connection, customTokens)
    {
    }

    /// <summary>
    /// Creates a new <see cref="ExpressionTableOperations{T}"/> instance.
    /// </summary>
    /// <param name="connection"><see cref="AdoDataConnection"/> instance to use for database operations.</param>
    /// <param name="exceptionHandler">Delegate to handle table operation exceptions.</param>
    /// <param name="customTokens">Custom run-time tokens to apply to any modeled <see cref="AmendExpressionAttribute"/> values.</param>
    /// <remarks>
    /// When exception handler is provided, table operations will not throw exceptions for database calls, any
    /// encountered exceptions will be passed to handler for processing. Otherwise, exceptions will be thrown
    /// on the call stack.
    /// </remarks>
    public ExpressionTableOperations(AdoDataConnection connection, Action<Exception> exceptionHandler, IEnumerable<KeyValuePair<string, string>>? customTokens = null)
        : base(connection, exceptionHandler, customTokens)
    {
    }

    /// <inheritdoc/>
    protected override T CreateRecordInstance()
    {
        return s_createRecordInstance(new CurrentScope { TableOperations = this, Connection = Connection });
    }

    /// <inheritdoc/>
    protected override void ApplyRecordDefaultValues(T record)
    {
        s_applyRecordDefaults(new CurrentScope { Instance = record, TableOperations = this, Connection = Connection });
    }

    /// <inheritdoc/>
    protected override void ApplyRecordUpdateValues(T record)
    {
        s_updateRecordInstance(new CurrentScope { Instance = record, TableOperations = this, Connection = Connection });
    }

    // Static Fields
    private static readonly Func<CurrentScope, T> s_createRecordInstance;
    private static readonly Action<CurrentScope> s_updateRecordInstance;
    private static readonly Action<CurrentScope> s_applyRecordDefaults;
    private static TypeRegistry? s_typeRegistry;

    // Static Constructor
    static ExpressionTableOperations()
    {
        // Create an instance of modeled table to allow any static functionality to be initialized,
        // such as registering any custom types or symbols that may be useful for value expressions
        ValueExpressionParser<T>.InitializeType();

        // Generate compiled "create new", "apply defaults" and "update" record functions for modeled table,
        // honoring DefaultValueAttribute, DefaultValueExpressionAttribute and UpdateValueExpressionAttribute.
        // Note: RecordProperties is sourced from the base table operations for the same modeled type T.
        s_createRecordInstance = ValueExpressionParser<T>.CreateInstance<CurrentScope>(RecordProperties, s_typeRegistry);
        s_updateRecordInstance = ValueExpressionParser<T>.UpdateInstance<CurrentScope>(RecordProperties, s_typeRegistry);
        s_applyRecordDefaults = ValueExpressionParser<T>.ApplyDefaults<CurrentScope>(RecordProperties, s_typeRegistry);
    }

    // Static Properties

    /// <summary>
    /// Gets or sets <see cref="Gemstone.Expressions.Evaluator.TypeRegistry"/> instance used for evaluating encountered instances
    /// of the <see cref="ValueExpressionAttributeBase"/> on modeled table properties.
    /// </summary>
    /// <remarks>
    /// Accessing this property will create a unique type registry for the current type <typeparamref name="T"/> which
    /// will initially contain the values found in the <see cref="ValueExpressionParser.DefaultTypeRegistry"/>
    /// and can be augmented with custom types. Set to <c>null</c> to restore use of the default type registry.
    /// </remarks>
    public static TypeRegistry TypeRegistry
    {
        get => s_typeRegistry ??= ValueExpressionParser.DefaultTypeRegistry.Clone();
        set => s_typeRegistry = value;
    }
}
