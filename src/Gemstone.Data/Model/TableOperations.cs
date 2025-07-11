﻿//******************************************************************************************************
//  TableOperations.cs - Gbtc
//
//  Copyright © 2016, Grid Protection Alliance.  All Rights Reserved.
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
//  02/01/2016 - J. Ritchie Carroll
//       Generated original version of source code.
//  12/13/2019 - J. Ritchie Carroll
//      Migrated to Gemstone libraries.
//
//******************************************************************************************************
// ReSharper disable ConditionalAccessQualifierIsNonNullableAccordingToAPIContract
// ReSharper disable UnusedMember.Global
// ReSharper disable StaticMemberInGenericType
// ReSharper disable UnusedMember.Local
// ReSharper disable AssignNullToNotNullAttribute
// ReSharper disable NotAccessedField.Local
// ReSharper disable ArrangeRedundantParentheses

using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Gemstone.Collections.CollectionExtensions;
using Gemstone.Collections.IAsyncEnumerableExtensions;
using Gemstone.Data.DataExtensions;
using Gemstone.Expressions.Evaluator;
using Gemstone.Expressions.Model;
using Gemstone.Reflection.MemberInfoExtensions;
using Gemstone.Security.Cryptography;
using Gemstone.StringExtensions;

namespace Gemstone.Data.Model;

/// <summary>
/// Defines database operations for a modeled table.
/// </summary>
/// <typeparam name="T">Modeled table.</typeparam>
public class TableOperations<T> : ITableOperations where T : class, new()
{
    #region [ Members ]

    // Nested Types
    private class CurrentScope : ValueExpressionScopeBase<T>
    {
        // Define instance variables exposed to ValueExpressionAttributeBase expressions
        #pragma warning disable 169, 414, 649, CS8618
        public TableOperations<T> TableOperations;
        public AdoDataConnection Connection;
        #pragma warning restore 169, 414, 649, CS8618
    }

    private class NullConnection : DbConnection
    {
        [AllowNull]
        public override string ConnectionString { get; set; }
        public override int ConnectionTimeout => 0;
        public override string Database => null!;
        public override string DataSource => null!;
        public override string ServerVersion => null!;
        public override ConnectionState State => ConnectionState.Open;
        public override void Open() {}
        public override void Close() {}
        protected override DbCommand CreateDbCommand() => null!;
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => null!;
        public override void ChangeDatabase(string databaseName) {}
    }

    private class IntermediateParameter : IDbDataParameter
    {
        public DbType DbType { get; set; }
        public ParameterDirection Direction { get; set; }
        public bool IsNullable { get; } = false;
        [AllowNull]
        public string ParameterName { get; set; } = string.Empty;
        [AllowNull]
        public string SourceColumn { get; set; } = string.Empty;
        public DataRowVersion SourceVersion { get; set; }
        public object? Value { get; set; }
        public byte Precision { get; set; }
        public byte Scale { get; set; }
        public int Size { get; set; }
    }

    // Constants
    private const string SelectCountSqlFormat = "SELECT COUNT(*) FROM {0}";
    private const string SelectSetSqlFormat = "SELECT {0} FROM {1} ORDER BY {{0}}";
    private const string SelectSetWhereSqlFormat = "SELECT {0} FROM {1} WHERE {{0}} ORDER BY {{1}}";
    private const string SelectRowSqlFormat = "SELECT * FROM {0} WHERE {1}";
    private const string AddNewSqlFormat = "INSERT INTO {0}({1}) VALUES ({2})";
    private const string UpdateSqlFormat = "UPDATE {0} SET {1} WHERE {2}";
    private const string DeleteSqlFormat = "DELETE FROM {0} WHERE {1}";
    private const string TableNamePrefixToken = "<!TNP/>";
    private const string TableNameSuffixToken = "<!TNS/>";
    private const string FieldListPrefixToken = "<!FLP/>";
    private const string FieldListSuffixToken = "<!FLS/>";
    private const string DefaultWildcardChar = "%";

    // Fields
    private readonly string m_selectCountSql;
    private readonly string m_selectSetSql;
    private readonly string m_selectSetWhereSql;
    private readonly string m_selectKeysSql;
    private readonly string m_selectKeysWhereSql;
    private readonly string m_selectRowSql;
    private readonly string m_addNewSql;
    private readonly string m_updateSql;
    private readonly string m_updateWhereSql;
    private readonly string m_deleteSql;
    private readonly string m_deleteWhereSql;
    private string? m_lastSortField;
    private RecordRestriction? m_lastRestriction;

    #endregion

    #region [ Constructors ]

    /// <summary>
    /// Creates a new <see cref="TableOperations{T}"/>.
    /// </summary>
    /// <param name="connection"><see cref="AdoDataConnection"/> instance to use for database operations.</param>
    /// <param name="customTokens">Custom run-time tokens to apply to any modeled <see cref="AmendExpressionAttribute"/> values.</param>
    /// <exception cref="ArgumentNullException"><paramref name="connection"/> cannot be <c>null</c>.</exception>
    /// <remarks>
    /// The <paramref name="customTokens"/> can be used to apply run-time tokens to any defined <see cref="AmendExpressionAttribute"/> values,
    /// for example, given the following amendment expression applied to a modeled class:
    /// <code>
    /// [AmendExpression("TOP {count}", 
    ///     TargetExpression = TargetExpression.FieldList,
    ///     AffixPosition = AffixPosition.Prefix,
    ///     StatementTypes = StatementTypes.SelectSet)]]
    /// </code>
    /// The <paramref name="customTokens"/> key/value pairs could be set as follows at run-time:
    /// <code>
    /// int count = 200;
    /// customTokens = new[] { new KeyValuePair&lt;string, string&gt;("{count}", $"{count}") };
    /// </code>
    /// </remarks>
    public TableOperations(AdoDataConnection connection, IEnumerable<KeyValuePair<string, string>>? customTokens = null)
    {
        Connection = connection ?? throw new ArgumentNullException(nameof(connection));
        m_selectCountSql = s_selectCountSql;
        m_selectSetSql = s_selectSetSql;
        m_selectSetWhereSql = s_selectSetWhereSql;
        m_selectKeysSql = s_selectKeysSql;
        m_selectKeysWhereSql = s_selectKeysWhereSql;
        m_selectRowSql = s_selectRowSql;
        m_addNewSql = s_addNewSql;
        m_updateSql = s_updateSql;
        m_updateWhereSql = s_updateWhereSql;
        m_deleteSql = s_deleteSql;
        m_deleteWhereSql = s_deleteWhereSql;

        // Establish any modeled root query restriction parameters
        if (s_rootQueryRestrictionAttribute is not null)
        {
            // Copy parameters array so that modifications to parameter values do not affect other instances
            object?[] parameters = s_rootQueryRestrictionAttribute.Parameters.ToArray();
            RootQueryRestriction = new RecordRestriction(s_rootQueryRestrictionAttribute.FilterExpression, parameters);
            ApplyRootQueryRestrictionToUpdates = s_rootQueryRestrictionAttribute.ApplyToUpdates;
            ApplyRootQueryRestrictionToDeletes = s_rootQueryRestrictionAttribute.ApplyToDeletes;
        }

        // When any escape targets are defined for the modeled identifiers, i.e., table or field names,
        // the static SQL statements are defined with ANSI standard escape delimiters. We check if the
        // user model has opted to instead use common database escape delimiters, or no delimiters, that
        // will apply to the active database type and make any needed adjustments. As a result, it will
        // be slightly faster to construct this class when ANSI standard escape delimiters are used.
        if (s_escapedTableNameTargets is not null)
        {
            string derivedTableName = GetEscapedTableName();
            string ansiEscapedTableName = $"\"{s_tableName}\"";

            if (!derivedTableName.Equals(ansiEscapedTableName))
            {
                m_selectCountSql = m_selectCountSql.Replace(ansiEscapedTableName, derivedTableName);
                m_selectSetSql = m_selectSetSql.Replace(ansiEscapedTableName, derivedTableName);
                m_selectSetWhereSql = m_selectSetWhereSql.Replace(ansiEscapedTableName, derivedTableName);
                m_selectKeysSql = m_selectKeysSql.Replace(ansiEscapedTableName, derivedTableName);
                m_selectKeysWhereSql = m_selectKeysWhereSql.Replace(ansiEscapedTableName, derivedTableName);
                m_selectRowSql = m_selectRowSql.Replace(ansiEscapedTableName, derivedTableName);
                m_addNewSql = m_addNewSql.Replace(ansiEscapedTableName, derivedTableName);
                m_updateSql = m_updateSql.Replace(ansiEscapedTableName, derivedTableName);
                m_updateWhereSql = m_updateWhereSql.Replace(ansiEscapedTableName, derivedTableName);
                m_deleteSql = m_deleteSql.Replace(ansiEscapedTableName, derivedTableName);
                m_deleteWhereSql = m_deleteWhereSql.Replace(ansiEscapedTableName, derivedTableName);
            }
        }

        if (s_escapedFieldNameTargets is not null)
        {
            foreach (KeyValuePair<string, Dictionary<DatabaseType, bool>?> escapedFieldNameTarget in s_escapedFieldNameTargets)
            {
                string fieldName = escapedFieldNameTarget.Key;
                string derivedFieldName = GetEscapedFieldName(fieldName, escapedFieldNameTarget.Value);
                string ansiEscapedFieldName = $"\"{fieldName}\"";

                if (derivedFieldName.Equals(ansiEscapedFieldName))
                    continue;

                m_selectKeysSql = m_selectKeysSql.Replace(ansiEscapedFieldName, derivedFieldName);
                m_selectKeysWhereSql = m_selectKeysWhereSql.Replace(ansiEscapedFieldName, derivedFieldName);
                m_selectRowSql = m_selectRowSql.Replace(ansiEscapedFieldName, derivedFieldName);
                m_addNewSql = m_addNewSql.Replace(ansiEscapedFieldName, derivedFieldName);
                m_updateSql = m_updateSql.Replace(ansiEscapedFieldName, derivedFieldName);
                m_updateWhereSql = m_updateWhereSql.Replace(ansiEscapedFieldName, derivedFieldName);
                m_deleteSql = m_deleteSql.Replace(ansiEscapedFieldName, derivedFieldName);
                m_deleteWhereSql = m_deleteWhereSql.Replace(ansiEscapedFieldName, derivedFieldName);
            }
        }

        if (s_expressionAmendments is null)
            return;

        // Handle any modeled expression amendments
        foreach ((DatabaseType, TargetExpression, StatementTypes, AffixPosition, string) expressionAmendment in s_expressionAmendments)
        {
            // Deconstruct expression amendment properties
            (
                DatabaseType databaseType, 
                TargetExpression targetExpression, 
                StatementTypes statementTypes, 
                AffixPosition affixPosition, 
                string amendmentText
            )
            = expressionAmendment;

            // See if expression amendment applies to current database type
            if (databaseType != Connection.DatabaseType)
                continue;

            string tableNameToken = affixPosition == AffixPosition.Prefix ? TableNamePrefixToken : TableNameSuffixToken;
            string fieldListToken = affixPosition == AffixPosition.Prefix ? FieldListPrefixToken : FieldListSuffixToken;
            string targetToken = targetExpression == TargetExpression.TableName ? tableNameToken : fieldListToken;

            // Apply amendment to target statement types
            if (statementTypes.HasFlag(StatementTypes.SelectCount) && targetExpression == TargetExpression.TableName)
                m_selectCountSql = m_selectCountSql.Replace(targetToken, amendmentText);

            if (statementTypes.HasFlag(StatementTypes.SelectSet))
            {
                m_selectSetSql = m_selectSetSql.Replace(targetToken, amendmentText);
                m_selectSetWhereSql = m_selectSetWhereSql.Replace(targetToken, amendmentText);
                m_selectKeysSql = m_selectKeysSql.Replace(targetToken, amendmentText);
                m_selectKeysWhereSql = m_selectKeysWhereSql.Replace(targetToken, amendmentText);
            }

            if (statementTypes.HasFlag(StatementTypes.SelectRow))
                m_selectRowSql = m_selectRowSql.Replace(targetToken, amendmentText);

            if (statementTypes.HasFlag(StatementTypes.Insert))
                m_addNewSql = m_addNewSql.Replace(targetToken, amendmentText);

            if (statementTypes.HasFlag(StatementTypes.Update))
            {
                m_updateSql = m_updateSql.Replace(targetToken, amendmentText);
                m_updateWhereSql = m_updateWhereSql.Replace(targetToken, amendmentText);
            }

            if (statementTypes.HasFlag(StatementTypes.Delete))
            {
                m_deleteSql = m_deleteSql.Replace(targetToken, amendmentText);
                m_deleteWhereSql = m_deleteWhereSql.Replace(targetToken, amendmentText);
            }
        }

        // Remove any remaining tokens from instance expressions
        static string removeRemainingTokens(string sql)
        {
            return sql
                .Replace(TableNamePrefixToken, "")
                .Replace(TableNameSuffixToken, "")
                .Replace(FieldListPrefixToken, "")
                .Replace(FieldListSuffixToken, "");
        }

        m_selectCountSql = removeRemainingTokens(m_selectCountSql);
        m_selectSetSql = removeRemainingTokens(m_selectSetSql);
        m_selectSetWhereSql = removeRemainingTokens(m_selectSetWhereSql);
        m_selectKeysSql = removeRemainingTokens(m_selectKeysSql);
        m_selectKeysWhereSql = removeRemainingTokens(m_selectKeysWhereSql);
        m_selectRowSql = removeRemainingTokens(m_selectRowSql);
        m_addNewSql = removeRemainingTokens(m_addNewSql);
        m_updateSql = removeRemainingTokens(m_updateSql);
        m_updateWhereSql = removeRemainingTokens(m_updateWhereSql);
        m_deleteSql = removeRemainingTokens(m_deleteSql);
        m_deleteWhereSql = removeRemainingTokens(m_deleteWhereSql);

        if (customTokens is null)
            return;

        // Execute replacements on any provided custom run-time tokens
        foreach (KeyValuePair<string, string> customToken in customTokens)
        {
            m_selectCountSql = m_selectCountSql.Replace(customToken.Key, customToken.Value);
            m_selectSetSql = m_selectSetSql.Replace(customToken.Key, customToken.Value);
            m_selectSetWhereSql = m_selectSetWhereSql.Replace(customToken.Key, customToken.Value);
            m_selectKeysSql = m_selectKeysSql.Replace(customToken.Key, customToken.Value);
            m_selectKeysWhereSql = m_selectKeysWhereSql.Replace(customToken.Key, customToken.Value);
            m_selectRowSql = m_selectRowSql.Replace(customToken.Key, customToken.Value);
            m_addNewSql = m_addNewSql.Replace(customToken.Key, customToken.Value);
            m_updateSql = m_updateSql.Replace(customToken.Key, customToken.Value);
            m_updateWhereSql = m_updateWhereSql.Replace(customToken.Key, customToken.Value);
            m_deleteSql = m_deleteSql.Replace(customToken.Key, customToken.Value);
            m_deleteWhereSql = m_deleteWhereSql.Replace(customToken.Key, customToken.Value);
        }

        
    }

    /// <summary>
    /// Creates a new <see cref="TableOperations{T}"/> using provided <paramref name="exceptionHandler"/>.
    /// </summary>
    /// <param name="connection"><see cref="AdoDataConnection"/> instance to use for database operations.</param>
    /// <param name="exceptionHandler">Delegate to handle table operation exceptions.</param>
    /// <param name="customTokens">Custom run-time tokens to apply to any modeled <see cref="AmendExpressionAttribute"/> values.</param>
    /// <remarks>
    /// <para>
    /// When exception handler is provided, table operations will not throw exceptions for database calls, any
    /// encountered exceptions will be passed to handler for processing.
    /// </para>
    /// <para>
    /// The <paramref name="customTokens"/> can be used to apply run-time tokens to any defined <see cref="AmendExpressionAttribute"/> values,
    /// for example, given the following amendment expression applied to a modeled class:
    /// <code>
    /// [AmendExpression("TOP {count}", 
    ///     TargetExpression = TargetExpression.FieldList,
    ///     AffixPosition = AffixPosition.Prefix,
    ///     StatementTypes = StatementTypes.SelectSet)]]
    /// </code>
    /// The <paramref name="customTokens"/> key/value pairs could be set as follows at run-time:
    /// <code>
    /// int count = 200;
    /// customTokens = new[] { new KeyValuePair&lt;string, string&gt;("{count}", $"{count}") };
    /// </code>
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="connection"/> cannot be <c>null</c>.</exception>
    public TableOperations(AdoDataConnection connection, Action<Exception> exceptionHandler, IEnumerable<KeyValuePair<string, string>>? customTokens = null)
        : this(connection, customTokens)
    {
        ExceptionHandler = exceptionHandler;
    }

    #endregion

    #region [ Properties ]

    /// <inheritdoc/>
    public AdoDataConnection Connection { get; }

    /// <inheritdoc/>
    public string TableName => GetEscapedTableName();

    /// <inheritdoc/>
    public string UnescapedTableName => s_tableName;

    ///  <inheritdoc/>
    public string WildcardChar { get; init; } = DefaultWildcardChar; 

    /// <inheritdoc/>
    public bool HasPrimaryKeyIdentityField => s_hasPrimaryKeyIdentityField;

    /// <inheritdoc/>
    public Action<Exception>? ExceptionHandler { get; init; }

    /// <inheritdoc/>
    public bool UseCaseSensitiveFieldNames { get; init; }

    /// <inheritdoc/>
    public DataTable? PrimaryKeyCache { get; set; }

    /// <inheritdoc/>
    public RecordRestriction? RootQueryRestriction { get; init; }

    /// <inheritdoc/>
    public bool ApplyRootQueryRestrictionToUpdates { get; init; }

    /// <inheritdoc/>
    public bool ApplyRootQueryRestrictionToDeletes { get; init; }

    #endregion

    #region [ Methods ]

    /// <summary>
    /// Creates a new modeled record instance, applying any modeled default values as specified by a
    /// <see cref="DefaultValueAttribute"/> or <see cref="DefaultValueExpressionAttribute"/> on the
    /// model properties.
    /// </summary>
    /// <returns>New modeled record instance with any defined default values applied.</returns>
    public T? NewRecord()
    {
        try
        {
            return s_createRecordInstance(new CurrentScope { TableOperations = this, Connection = Connection });
        }
        catch (Exception ex)
        {
            if (ExceptionHandler is null)
                throw;

            ExceptionHandler(ex);

            return null;
        }
    }

    object? ITableOperations.NewRecord()
    {
        return NewRecord();
    }

    /// <summary>
    /// Applies the default values on the specified modeled table <paramref name="record"/>
    /// where any of the properties are marked with either <see cref="DefaultValueAttribute"/>
    /// or <see cref="DefaultValueExpressionAttribute"/>.
    /// </summary>
    /// <param name="record">Record to update.</param>
    public void ApplyRecordDefaults(T record)
    {
        try
        {
            s_applyRecordDefaults(new CurrentScope { Instance = record, TableOperations = this, Connection = Connection });
        }
        catch (Exception ex)
        {
            if (ExceptionHandler is null)
                throw;

            ExceptionHandler(ex);
        }
    }

    void ITableOperations.ApplyRecordDefaults(object value)
    {
        if (value is not T record)
            throw new ArgumentException($"Cannot apply defaults for record of type \"{value?.GetType().Name ?? "null"}\", expected \"{typeof(T).Name}\"", nameof(value));

        ApplyRecordDefaults(record);
    }

    /// <summary>
    /// Applies the update values on the specified modeled table <paramref name="record"/> where
    /// any of the properties are marked with <see cref="UpdateValueExpressionAttribute"/>.
    /// </summary>
    /// <param name="record">Record to update.</param>
    public void ApplyRecordUpdates(T record)
    {
        try
        {
            s_updateRecordInstance(new CurrentScope { Instance = record, TableOperations = this, Connection = Connection });
        }
        catch (Exception ex)
        {
            if (ExceptionHandler is null)
                throw;

            ExceptionHandler(ex);
        }
    }

    void ITableOperations.ApplyRecordUpdates(object value)
    {
        if (value is not T record)
            throw new ArgumentException($"Cannot apply updates for record of type \"{value?.GetType().Name ?? "null"}\", expected \"{typeof(T).Name}\"", nameof(value));

        ApplyRecordUpdates(record);
    }

    /// <summary>
    /// Queries database and returns a single modeled table record for the specified <paramref name="restriction"/>.
    /// </summary>
    /// <param name="restriction">Record restriction to apply.</param>
    /// <returns>A single modeled table record for the queried record.</returns>
    /// <remarks>
    /// <para>
    /// If no record is found for specified <paramref name="restriction"/>, <c>null</c> will be returned.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="QueryRecords(string, RecordRestriction, int)"/>
    /// specifying the <see cref="RecordRestriction"/> parameter with a limit of 1 record.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// </remarks>
    public T? QueryRecord(RecordRestriction? restriction)
    {
        return QueryRecord(null, restriction);
    }

    object? ITableOperations.QueryRecord(RecordRestriction? restriction)
    {
        return QueryRecord(restriction);
    }

    /// <summary>
    /// Queries database and returns a single modeled table record for the specified <paramref name="restriction"/>.
    /// </summary>
    /// <param name="restriction">Record restriction to apply.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <returns>A single modeled table record for the queried record.</returns>
    /// <remarks>
    /// <para>
    /// If no record is found for specified <paramref name="restriction"/>, <c>null</c> will be returned.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="QueryRecords(string, RecordRestriction, int)"/>
    /// specifying the <see cref="RecordRestriction"/> parameter with a limit of 1 record.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// </remarks>
    public ValueTask<T?> QueryRecordAsync(RecordRestriction? restriction, CancellationToken cancellationToken)
    {
        return QueryRecordAsync(null, restriction, cancellationToken);
    }

    ValueTask<object?> ITableOperations.QueryRecordAsync(RecordRestriction? restriction, CancellationToken cancellationToken)
    {
        return ((ITableOperations)this).QueryRecordAsync(null, restriction, cancellationToken);
    }

    /// <summary>
    /// Queries database and returns a single modeled table record for the specified <paramref name="restriction"/>,
    /// execution of query will apply <paramref name="orderByExpression"/>.
    /// </summary>
    /// <param name="orderByExpression">Field name expression used for sort order, include ASC or DESC as needed - does not include ORDER BY; defaults to primary keys.</param>
    /// <param name="restriction">Record restriction to apply.</param>
    /// <returns>A single modeled table record for the queried record.</returns>
    /// <remarks>
    /// <para>
    /// If no record is found for specified <paramref name="restriction"/>, <c>null</c> will be returned.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="QueryRecords(string, RecordRestriction, int)"/>
    /// specifying the <see cref="RecordRestriction"/> parameter with a limit of 1 record.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// </remarks>
    public T? QueryRecord(string? orderByExpression, RecordRestriction? restriction)
    {
        return QueryRecords(orderByExpression, restriction, 1).FirstOrDefault();
    }

    object? ITableOperations.QueryRecord(string? orderByExpression, RecordRestriction? restriction)
    {
        return QueryRecord(orderByExpression, restriction);
    }

    /// <summary>
    /// Queries database and returns a single modeled table record for the specified <paramref name="restriction"/>,
    /// execution of query will apply <paramref name="orderByExpression"/>.
    /// </summary>
    /// <param name="orderByExpression">Field name expression used for sort order, include ASC or DESC as needed - does not include ORDER BY; defaults to primary keys.</param>
    /// <param name="restriction">Record restriction to apply.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <returns>A single modeled table record for the queried record.</returns>
    /// <remarks>
    /// <para>
    /// If no record is found for specified <paramref name="restriction"/>, <c>null</c> will be returned.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="QueryRecords(string, RecordRestriction, int)"/>
    /// specifying the <see cref="RecordRestriction"/> parameter with a limit of 1 record.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// </remarks>
    public ValueTask<T?> QueryRecordAsync(string? orderByExpression, RecordRestriction? restriction, CancellationToken cancellationToken)
    {
        return QueryRecordsAsync(orderByExpression, restriction, 1, cancellationToken).FirstOrDefaultAsync(cancellationToken);
    }

    ValueTask<object?> ITableOperations.QueryRecordAsync(string? orderByExpression, RecordRestriction? restriction, CancellationToken cancellationToken)
    {
        return ((ITableOperations)this).QueryRecordsAsync(orderByExpression, restriction, 1, cancellationToken).FirstOrDefaultAsync(cancellationToken);
    }

    /// <summary>
    /// Queries database and returns a single modeled table record for the specified SQL filter
    /// expression and parameters.
    /// </summary>
    /// <param name="filterExpression">
    /// Filter SQL expression for restriction as a composite format string - does not include WHERE.
    /// When escaping is needed for field names, use standard ANSI quotes.
    /// </param>
    /// <param name="parameters">Restriction parameter values.</param>
    /// <returns>A single modeled table record for the queried record.</returns>
    /// <remarks>
    /// <para>
    /// If no record is found for specified filter expression and parameters, <c>null</c> will be returned.
    /// </para>
    /// <para>
    /// Each indexed parameter, e.g., "{0}", in the composite format <paramref name="filterExpression"/>
    /// will be converted into query parameters where each of the corresponding values in the
    /// <paramref name="parameters"/> collection will be applied as <see cref="IDbDataParameter"/>
    /// values to an executed <see cref="IDbCommand"/> query.
    /// </para>
    /// <para>
    /// If any of the specified <paramref name="parameters"/> reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// <para>
    /// If needed, field names that are escaped with standard ANSI quotes in the filter expression
    /// will be updated to reflect what is defined in the user model.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="QueryRecords(string, RecordRestriction, int)"/>
    /// specifying the <see cref="RecordRestriction"/> parameter with a limit of 1 record.
    /// </para>
    /// </remarks>
    public T? QueryRecordWhere(string? filterExpression, params object?[] parameters)
    {
        return QueryRecord(new RecordRestriction(filterExpression, parameters));
    }

    object? ITableOperations.QueryRecordWhere(string? filterExpression, params object?[] parameters)
    {
        return QueryRecordWhere(filterExpression, parameters);
    }

    /// <summary>
    /// Queries database and returns a single modeled table record for the specified SQL filter
    /// expression and parameters.
    /// </summary>
    /// <param name="filterExpression">
    /// Filter SQL expression for restriction as a composite format string - does not include WHERE.
    /// When escaping is needed for field names, use standard ANSI quotes.
    /// </param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">Restriction parameter values.</param>
    /// <returns>A single modeled table record for the queried record.</returns>
    /// <remarks>
    /// <para>
    /// If no record is found for specified filter expression and parameters, <c>null</c> will be returned.
    /// </para>
    /// <para>
    /// Each indexed parameter, e.g., "{0}", in the composite format <paramref name="filterExpression"/>
    /// will be converted into query parameters where each of the corresponding values in the
    /// <paramref name="parameters"/> collection will be applied as <see cref="IDbDataParameter"/>
    /// values to an executed <see cref="IDbCommand"/> query.
    /// </para>
    /// <para>
    /// If any of the specified <paramref name="parameters"/> reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// <para>
    /// If needed, field names that are escaped with standard ANSI quotes in the filter expression
    /// will be updated to reflect what is defined in the user model.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="QueryRecords(string, RecordRestriction, int)"/>
    /// specifying the <see cref="RecordRestriction"/> parameter with a limit of 1 record.
    /// </para>
    /// </remarks>
    public ValueTask<T?> QueryRecordWhereAsync(string? filterExpression, CancellationToken cancellationToken, params object?[] parameters)
    {
        return QueryRecordAsync(new RecordRestriction(filterExpression, parameters), cancellationToken);
    }

    ValueTask<object?> ITableOperations.QueryRecordWhereAsync(string? filterExpression, CancellationToken cancellationToken, params object?[] parameters)
    {
        return ((ITableOperations)this).QueryRecordAsync(new RecordRestriction(filterExpression, parameters), cancellationToken);
    }

    /// <summary>
    /// Queries database and returns modeled table records for the specified parameters.
    /// </summary>
    /// <param name="orderByExpression">Field name expression used for sort order, include ASC or DESC as needed - does not include ORDER BY; defaults to primary keys.</param>
    /// <param name="restriction">Record restriction to apply, if any.</param>
    /// <param name="limit">Limit of number of record to return.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// If no record <paramref name="restriction"/> or <paramref name="limit"/> is provided, all rows will be returned.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// </remarks>
    public IEnumerable<T?> QueryRecords(string? orderByExpression = null, RecordRestriction? restriction = null, int limit = -1)
    {
        orderByExpression = ValidateOrderByExpression(orderByExpression);

        if (string.IsNullOrWhiteSpace(orderByExpression))
            orderByExpression = UpdateFieldNames(s_primaryKeyFields);

        string? sqlExpression = null;

        try
        {
            if (RootQueryRestriction is not null)
                restriction = (RootQueryRestriction + restriction)!;

            if (limit < 1)
            {
                // No record limit specified
                if (restriction is null)
                {
                    sqlExpression = string.Format(m_selectSetSql, orderByExpression);
                    return Connection.RetrieveData(sqlExpression).AsEnumerable().Select(LoadRecord);
                }

                sqlExpression = string.Format(m_selectSetWhereSql, UpdateFieldNames(restriction.FilterExpression), orderByExpression);

                return Connection.RetrieveData(sqlExpression, restriction.Parameters).AsEnumerable().Select(LoadRecord);
            }

            if (restriction is null)
            {
                sqlExpression = string.Format(m_selectSetSql, orderByExpression);
                return Connection.RetrieveData(sqlExpression).AsEnumerable().Take(limit).Select(LoadRecord);
            }

            sqlExpression = string.Format(m_selectSetWhereSql, UpdateFieldNames(restriction.FilterExpression), orderByExpression);

            return Connection.RetrieveData(sqlExpression, restriction.Parameters).AsEnumerable().Take(limit).Select(LoadRecord);
        }
        catch (Exception ex)
        {
            InvalidOperationException opex = new($"Exception during record query for {typeof(T).Name} \"{sqlExpression ?? "undefined"}, {ValueList(restriction?.Parameters)}\": {ex.Message}", ex);

            if (ExceptionHandler is null)
                throw opex;

            ExceptionHandler(opex);

            return [];
        }
    }

    IEnumerable ITableOperations.QueryRecords(string? orderByExpression, RecordRestriction? restriction, int limit)
    {
        return QueryRecords(orderByExpression, restriction, limit);
    }

    /// <summary>
    /// Queries database and returns modeled table records for the specified parameters.
    /// </summary>
    /// <param name="orderByExpression">Field name expression used for sort order, include ASC or DESC as needed - does not include ORDER BY; defaults to primary keys.</param>
    /// <param name="restriction">Record restriction to apply, if any.</param>
    /// <param name="limit">Limit of number of record to return.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// If no record <paramref name="restriction"/> or <paramref name="limit"/> is provided, all rows will be returned.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// </remarks>
    public IAsyncEnumerable<T?> QueryRecordsAsync(string? orderByExpression = null, RecordRestriction? restriction = null, int limit = -1, CancellationToken cancellationToken = default)
    {
        orderByExpression = ValidateOrderByExpression(orderByExpression);

        if (string.IsNullOrWhiteSpace(orderByExpression))
            orderByExpression = UpdateFieldNames(s_primaryKeyFields);

        string? sqlExpression = null;

        try
        {
            if (RootQueryRestriction is not null)
                restriction = (RootQueryRestriction + restriction)!;

            if (limit < 1)
            {
                // No record limit specified
                if (restriction is null)
                {
                    sqlExpression = string.Format(m_selectSetSql, orderByExpression);
                    return Connection.RetrieveDataAsAsyncEnumerable(s_tableSchema, sqlExpression, cancellationToken).Select(LoadRecord);
                }

                sqlExpression = string.Format(m_selectSetWhereSql, UpdateFieldNames(restriction.FilterExpression), orderByExpression);

                return Connection.RetrieveDataAsAsyncEnumerable(s_tableSchema, sqlExpression, cancellationToken, restriction.Parameters).Select(LoadRecord);
            }

            if (restriction is null)
            {
                sqlExpression = string.Format(m_selectSetSql, orderByExpression);
                return Connection.RetrieveDataAsAsyncEnumerable(s_tableSchema, sqlExpression, cancellationToken).Take(limit).Select(LoadRecord);
            }

            sqlExpression = string.Format(m_selectSetWhereSql, UpdateFieldNames(restriction.FilterExpression), orderByExpression);

            return Connection.RetrieveDataAsAsyncEnumerable(s_tableSchema, sqlExpression, cancellationToken, restriction.Parameters).Take(limit).Select(LoadRecord);
        }
        catch (Exception ex)
        {
            InvalidOperationException opex = new($"Exception during record query for {typeof(T).Name} \"{sqlExpression ?? "undefined"}, {ValueList(restriction?.Parameters)}\": {ex.Message}", ex);

            if (ExceptionHandler is null)
                throw opex;

            ExceptionHandler(opex);

            return AsyncEnumerable.Empty<T>();
        }
    }

    IAsyncEnumerable<object?> ITableOperations.QueryRecordsAsync(string? orderByExpression, RecordRestriction? restriction, int limit, CancellationToken cancellationToken)
    {
        return QueryRecordsAsync(orderByExpression, restriction, limit, cancellationToken)!.Cast<object?>();
    }

    /// <summary>
    /// Queries database and returns modeled table records for the specified <paramref name="restriction"/>.
    /// </summary>
    /// <param name="restriction">Record restriction to apply.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// This is a convenience call to <see cref="QueryRecords(string, RecordRestriction, int)"/> only
    /// specifying the <see cref="RecordRestriction"/> parameter.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// </remarks>
    public IEnumerable<T?> QueryRecords(RecordRestriction? restriction)
    {
        return QueryRecords(null, restriction);
    }

    IEnumerable ITableOperations.QueryRecords(RecordRestriction? restriction)
    {
        return QueryRecords(restriction);
    }

    /// <summary>
    /// Queries database and returns modeled table records for the specified <paramref name="restriction"/>.
    /// </summary>
    /// <param name="restriction">Record restriction to apply.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// This is a convenience call to <see cref="QueryRecords(string, RecordRestriction, int)"/> only
    /// specifying the <see cref="RecordRestriction"/> parameter.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// </remarks>
    public IAsyncEnumerable<T?> QueryRecordsAsync(RecordRestriction? restriction, CancellationToken cancellationToken)
    {
        return QueryRecordsAsync(null, restriction, -1, cancellationToken);
    }

    IAsyncEnumerable<object?> ITableOperations.QueryRecordsAsync(RecordRestriction? restriction, CancellationToken cancellationToken)
    {
        return QueryRecordsAsync(restriction, cancellationToken)!.Cast<object?>();
    }

    /// <summary>
    /// Queries database and returns modeled table records for the specified SQL filter expression
    /// and parameters.
    /// </summary>
    /// <param name="filterExpression">
    /// Filter SQL expression for restriction as a composite format string - does not include WHERE.
    /// When escaping is needed for field names, use standard ANSI quotes.
    /// </param>
    /// <param name="parameters">Restriction parameter values.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// Each indexed parameter, e.g., "{0}", in the composite format <paramref name="filterExpression"/>
    /// will be converted into query parameters where each of the corresponding values in the
    /// <paramref name="parameters"/> collection will be applied as <see cref="IDbDataParameter"/>
    /// values to an executed <see cref="IDbCommand"/> query.
    /// </para>
    /// <para>
    /// If any of the specified <paramref name="parameters"/> reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// <para>
    /// If needed, field names that are escaped with standard ANSI quotes in the filter expression
    /// will be updated to reflect what is defined in the user model.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="QueryRecords(string, RecordRestriction, int)"/> only
    /// specifying the <see cref="RecordRestriction"/> parameter.
    /// </para>
    /// </remarks>
    public IEnumerable<T?> QueryRecordsWhere(string? filterExpression, params object?[] parameters)
    {
        return QueryRecords(new RecordRestriction(filterExpression, parameters));
    }

    IEnumerable ITableOperations.QueryRecordsWhere(string? filterExpression, params object?[] parameters)
    {
        return QueryRecordsWhere(filterExpression, parameters);
    }

    /// <summary>
    /// Queries database and returns modeled table records for the specified SQL filter expression
    /// and parameters.
    /// </summary>
    /// <param name="filterExpression">
    /// Filter SQL expression for restriction as a composite format string - does not include WHERE.
    /// When escaping is needed for field names, use standard ANSI quotes.
    /// </param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">Restriction parameter values.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// Each indexed parameter, e.g., "{0}", in the composite format <paramref name="filterExpression"/>
    /// will be converted into query parameters where each of the corresponding values in the
    /// <paramref name="parameters"/> collection will be applied as <see cref="IDbDataParameter"/>
    /// values to an executed <see cref="IDbCommand"/> query.
    /// </para>
    /// <para>
    /// If any of the specified <paramref name="parameters"/> reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// <para>
    /// If needed, field names that are escaped with standard ANSI quotes in the filter expression
    /// will be updated to reflect what is defined in the user model.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="QueryRecords(string, RecordRestriction, int)"/> only
    /// specifying the <see cref="RecordRestriction"/> parameter.
    /// </para>
    /// </remarks>
    public IAsyncEnumerable<T?> QueryRecordsWhereAsync(string? filterExpression, CancellationToken cancellationToken, params object?[] parameters)
    {
        return QueryRecordsAsync(new RecordRestriction(filterExpression, parameters), cancellationToken);
    }

    IAsyncEnumerable<object?> ITableOperations.QueryRecordsWhereAsync(string? filterExpression, CancellationToken cancellationToken, params object?[] parameters)
    {
        return QueryRecordsWhereAsync(filterExpression, cancellationToken, parameters)!.Cast<object?>();
    }

    /// <summary>
    /// Queries database and returns modeled table records for the specified sorting and paging parameters.
    /// </summary>
    /// <param name="sortField">Field name to order-by.</param>
    /// <param name="ascending">Sort ascending flag; set to <c>false</c> for descending.</param>
    /// <param name="page">Page number of records to return (1-based).</param>
    /// <param name="pageSize">Current page size.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// This function is used for record paging. Primary keys are cached server-side, typically per user session,
    /// to maintain desired per-page sort order. Call <see cref="ClearPrimaryKeyCache"/> to manually clear cache
    /// when table contents are known to have changed.
    /// </para>
    /// <para>
    /// If the specified <paramref name="sortField"/> has been marked with <see cref="EncryptDataAttribute"/>,
    /// establishing the primary key cache operation will take longer to execute since query data will need to
    /// be downloaded locally and decrypted so the proper sort order can be determined.
    /// </para>
    /// </remarks>
    public IEnumerable<T> QueryRecords(string? sortField, bool ascending, int page, int pageSize)
    {
        return QueryRecords(sortField, ascending, page, pageSize, (RecordRestriction?[]?)null);
    }

    IEnumerable ITableOperations.QueryRecords(string? sortField, bool ascending, int page, int pageSize)
    {
        return QueryRecords(sortField, ascending, page, pageSize);
    }

    /// <summary>
    /// Queries database and returns modeled table records for the specified sorting and paging parameters.
    /// </summary>
    /// <param name="sortField">Field name to order-by.</param>
    /// <param name="ascending">Sort ascending flag; set to <c>false</c> for descending.</param>
    /// <param name="page">Page number of records to return (1-based).</param>
    /// <param name="pageSize">Current page size.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// This function is used for record paging. Primary keys are cached server-side, typically per user session,
    /// to maintain desired per-page sort order. Call <see cref="ClearPrimaryKeyCache"/> to manually clear cache
    /// when table contents are known to have changed.
    /// </para>
    /// <para>
    /// If the specified <paramref name="sortField"/> has been marked with <see cref="EncryptDataAttribute"/>,
    /// establishing the primary key cache operation will take longer to execute since query data will need to
    /// be downloaded locally and decrypted so the proper sort order can be determined.
    /// </para>
    /// </remarks>
    public IAsyncEnumerable<T> QueryRecordsAsync(string? sortField, bool ascending, int page, int pageSize, CancellationToken cancellationToken)
    {
        return QueryRecordsAsync(sortField, ascending, page, pageSize, cancellationToken, (RecordRestriction?[]?)null);
    }

    IAsyncEnumerable<object> ITableOperations.QueryRecordsAsync(string? sortField, bool ascending, int page, int pageSize, CancellationToken cancellationToken)
    {
        return QueryRecordsAsync(sortField, ascending, page, pageSize, cancellationToken).Cast<object>();
    }

    /// <summary>
    /// Queries database and returns modeled table records for the specified sorting, paging and search parameters.
    /// Search executed against fields modeled with <see cref="SearchableAttribute"/>.
    /// </summary>
    /// <param name="sortField">Field name to order-by.</param>
    /// <param name="ascending">Sort ascending flag; set to <c>false</c> for descending.</param>
    /// <param name="page">Page number of records to return (1-based).</param>
    /// <param name="pageSize">Current page size.</param>
    /// <param name="recordFilters">Record Filters to be applied.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// This function is used for record paging. Primary keys are cached server-side, typically per user session,
    /// to maintain desired per-page sort order. Call <see cref="ClearPrimaryKeyCache"/> to manually clear cache
    /// when table contents are known to have changed.
    /// </para>
    /// <para>
    /// If the specified <paramref name="sortField"/> has been marked with <see cref="EncryptDataAttribute"/>,
    /// establishing the primary key cache operation will take longer to execute since query data will need to
    /// be downloaded locally and decrypted so the proper sort order can be determined.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="QueryRecords(string, bool, int, int, RecordRestriction[])"/> where restriction
    /// is generated by <see cref="GetSearchRestrictions(IRecordFilter[])"/> using <paramref name="recordFilters"/>.
    /// </para>
    /// </remarks>
    public IEnumerable<T> QueryRecords(string? sortField, bool ascending, int page, int pageSize, params IRecordFilter?[]? recordFilters)
    {
        return QueryRecords(sortField, ascending, page, pageSize, GetSearchRestrictions(recordFilters));
    }

    IEnumerable ITableOperations.QueryRecords(string? sortField, bool ascending, int page, int pageSize, params IRecordFilter?[]? recordFilters)
    {
        return QueryRecords(sortField, ascending, page, pageSize, recordFilters);
    }

    /// <summary>
    /// Queries database and returns modeled table records for the specified sorting, paging and search parameters.
    /// Search executed against fields modeled with <see cref="SearchableAttribute"/>.
    /// </summary>
    /// <param name="sortField">Field name to order-by.</param>
    /// <param name="ascending">Sort ascending flag; set to <c>false</c> for descending.</param>
    /// <param name="page">Page number of records to return (1-based).</param>
    /// <param name="pageSize">Current page size.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="recordFilters">Record Filters to be applied.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// This function is used for record paging. Primary keys are cached server-side, typically per user session,
    /// to maintain desired per-page sort order. Call <see cref="ClearPrimaryKeyCache"/> to manually clear cache
    /// when table contents are known to have changed.
    /// </para>
    /// <para>
    /// If the specified <paramref name="sortField"/> has been marked with <see cref="EncryptDataAttribute"/>,
    /// establishing the primary key cache operation will take longer to execute since query data will need to
    /// be downloaded locally and decrypted so the proper sort order can be determined.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="QueryRecords(string, bool, int, int, RecordRestriction[])"/> where restriction
    /// is generated by <see cref="GetSearchRestrictions(IRecordFilter[])"/> using <paramref name="recordFilters"/>.
    /// </para>
    /// </remarks>
    public IAsyncEnumerable<T> QueryRecordsAsync(string? sortField, bool ascending, int page, int pageSize, CancellationToken cancellationToken, params IRecordFilter?[]? recordFilters)
    {
        return QueryRecordsAsync(sortField, ascending, page, pageSize, cancellationToken, GetSearchRestrictions(recordFilters));
    }

    IAsyncEnumerable<object> ITableOperations.QueryRecordsAsync(string? sortField, bool ascending, int page, int pageSize, CancellationToken cancellationToken, params IRecordFilter?[]? recordFilters)
    {
        return QueryRecordsAsync(sortField, ascending, page, pageSize, cancellationToken, recordFilters).Cast<object>();
    }

    /// <summary>
    /// Queries database and returns modeled table records for the specified sorting and paging parameters.
    /// </summary>
    /// <param name="sortField">Field name to order-by.</param>
    /// <param name="ascending">Sort ascending flag; set to <c>false</c> for descending.</param>
    /// <param name="page">Page number of records to return (1-based).</param>
    /// <param name="pageSize">Current page size.</param>
    /// <param name="restrictions">Record restrictions to apply, if any.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// This function is used for record paging. Primary keys are cached server-side, typically per user session,
    /// to maintain desired per-page sort order. Call <see cref="ClearPrimaryKeyCache"/> to manually clear cache
    /// when table contents are known to have changed.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restrictions"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// <para>
    /// If the specified <paramref name="sortField"/> has been marked with <see cref="EncryptDataAttribute"/>,
    /// establishing the primary key cache operation will take longer to execute since query data will need to
    /// be downloaded locally and decrypted so the proper sort order can be determined.
    /// </para>
    /// </remarks>
    public IEnumerable<T> QueryRecords(string? sortField, bool ascending, int page, int pageSize, params RecordRestriction?[]? restrictions)
    {
        RecordRestriction? restriction = null;

        if (restrictions is not null)
            restriction = restrictions.Aggregate(restriction, (current, item) => current + item);

        sortField = ValidateOrderByExpression(sortField);

        if (string.IsNullOrWhiteSpace(sortField))
            sortField = s_fieldNames[s_primaryKeyProperties[0].Name];

        ArgumentNullException.ThrowIfNull(sortField);

        bool sortFieldIsEncrypted = FieldIsEncrypted(sortField);

        // Records that have been deleted since primary key cache was established will return null and be filtered out which will throw
        // off the record count. Local delete operations automatically clear the primary key cache, however, if record set is known to
        // have changed outside purview of this class, the "ClearPrimaryKeyCache()" method should be manually called so that primary key
        // cache can be reestablished.
        if (PrimaryKeyCache is null || !sortField.Equals(m_lastSortField, StringComparison.OrdinalIgnoreCase) || restriction != m_lastRestriction)
        {
            string orderByExpression = sortFieldIsEncrypted ? s_fieldNames[s_primaryKeyProperties[0].Name] : $"{sortField}{(ascending ? "" : " DESC")}";
            string? sqlExpression = null;

            try
            {
                if (RootQueryRestriction is not null)
                    restriction = (RootQueryRestriction + restriction)!;

                if (restriction is null)
                {
                    sqlExpression = string.Format(m_selectKeysSql, orderByExpression);
                    PrimaryKeyCache = Connection.RetrieveData(sqlExpression);
                }
                else
                {
                    sqlExpression = string.Format(m_selectKeysWhereSql, UpdateFieldNames(restriction.FilterExpression), orderByExpression);
                    PrimaryKeyCache = Connection.RetrieveData(sqlExpression, restriction.Parameters);
                }

                // If sort field is encrypted, execute a local sort and update primary key cache
                if (sortFieldIsEncrypted && s_propertyNames.TryGetValue(sortField, out string? propertyName) && s_properties.TryGetValue(propertyName, out PropertyInfo? sortFieldProperty))
                {
                    // Reduce properties to load only primary key fields and sort field
                    HashSet<PropertyInfo> properties = [..s_primaryKeyProperties, sortFieldProperty];
                    IEnumerable<T> sortResult = LocalOrderBy(PrimaryKeyCache.AsEnumerable().Select(row => LoadRecordFromCachedKeys(row.ItemArray, properties)).Where(record => record is not null), sortField, ascending)!;
                    DataTable sortedKeyCache = new(s_tableName);

                    foreach (DataColumn column in PrimaryKeyCache.Columns)
                        sortedKeyCache.Columns.Add(column.ColumnName, column.DataType);

                    foreach (T record in sortResult)
                        sortedKeyCache.Rows.Add(GetPrimaryKeys(record));

                    PrimaryKeyCache = sortedKeyCache;
                }
            }
            catch (Exception ex)
            {
                InvalidOperationException opex = new($"Exception during record query for {typeof(T).Name} \"{sqlExpression ?? "undefined"}, {ValueList(restriction?.Parameters)}\": {ex.Message}", ex);

                if (ExceptionHandler is null)
                    throw opex;

                ExceptionHandler(opex);

                return [];
            }

            m_lastSortField = sortField;
            m_lastRestriction = restriction;
        }

        // Paginate on cached data rows so paging does no work except to skip through records, then only load records for a given page of data 
        return PrimaryKeyCache.AsEnumerable().ToPagedList(page, pageSize, PrimaryKeyCache.Rows.Count).Select(row => LoadRecordFromCachedKeys(row.ItemArray)).Where(record => record is not null)!;
    }

    IEnumerable ITableOperations.QueryRecords(string? sortField, bool ascending, int page, int pageSize, params RecordRestriction?[]? restrictions)
    {
        return QueryRecords(sortField, ascending, page, pageSize, restrictions);
    }

    /// <summary>
    /// Queries database and returns modeled table records for the specified sorting and paging parameters.
    /// </summary>
    /// <param name="sortField">Field name to order-by.</param>
    /// <param name="ascending">Sort ascending flag; set to <c>false</c> for descending.</param>
    /// <param name="page">Page number of records to return (1-based).</param>
    /// <param name="pageSize">Current page size.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="restrictions">Record restrictions to apply, if any.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// This function is used for record paging. Primary keys are cached server-side, typically per user session,
    /// to maintain desired per-page sort order. Call <see cref="ClearPrimaryKeyCache"/> to manually clear cache
    /// when table contents are known to have changed.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restrictions"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// <para>
    /// If the specified <paramref name="sortField"/> has been marked with <see cref="EncryptDataAttribute"/>,
    /// establishing the primary key cache operation will take longer to execute since query data will need to
    /// be downloaded locally and decrypted so the proper sort order can be determined.
    /// </para>
    /// </remarks>
    public async IAsyncEnumerable<T> QueryRecordsAsync(string? sortField, bool ascending, int page, int pageSize, [EnumeratorCancellation] CancellationToken cancellationToken, params RecordRestriction?[]? restrictions)
    {
        RecordRestriction? restriction = null;

        if (restrictions is not null) 
            restriction = restrictions.Aggregate(restriction, (current, item) => current + item);

        sortField = ValidateOrderByExpression(sortField);

        if (string.IsNullOrWhiteSpace(sortField))
            sortField = s_fieldNames[s_primaryKeyProperties[0].Name];

        ArgumentNullException.ThrowIfNull(sortField);
        
        bool sortFieldIsEncrypted = FieldIsEncrypted(sortField);

        // Records that have been deleted since primary key cache was established will return null and be filtered out which will throw
        // off the record count. Local delete operations automatically clear the primary key cache, however, if record set is known to
        // have changed outside purview of this class, the "ClearPrimaryKeyCache()" method should be manually called so that primary key
        // cache can be reestablished.
        if (PrimaryKeyCache is null || !sortField.Equals(m_lastSortField, StringComparison.OrdinalIgnoreCase) || restriction != m_lastRestriction)
        {
            string orderByExpression = sortFieldIsEncrypted ? s_fieldNames[s_primaryKeyProperties[0].Name] : $"{sortField}{(ascending ? "" : " DESC")}";
            string? sqlExpression = null;

            try
            {
                if (RootQueryRestriction is not null)
                    restriction = (RootQueryRestriction + restriction)!;

                if (restriction is null)
                {
                    sqlExpression = string.Format(m_selectKeysSql, orderByExpression);
                    PrimaryKeyCache = await Connection.RetrieveDataAsync(sqlExpression, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    sqlExpression = string.Format(m_selectKeysWhereSql, UpdateFieldNames(restriction.FilterExpression), orderByExpression);
                    PrimaryKeyCache = await Connection.RetrieveDataAsync(sqlExpression, cancellationToken, restriction.Parameters).ConfigureAwait(false);
                }

                // If sort field is encrypted, execute a local sort and update primary key cache
                if (sortFieldIsEncrypted && s_propertyNames.TryGetValue(sortField, out string? propertyName) && s_properties.TryGetValue(propertyName, out PropertyInfo? sortFieldProperty))
                {
                    // Reduce properties to load only primary key fields and sort field
                    HashSet<PropertyInfo> properties = [.. s_primaryKeyProperties, sortFieldProperty];

                    async IAsyncEnumerable<T?> loadKeyRecordFieldsFromCache()
                    {
                        await foreach (DataRow row in PrimaryKeyCache.AsAwaitConfiguredCancelableAsyncEnumerable(cancellationToken))
                        {
                            T? record = await LoadRecordFromCachedKeysAsync(row.ItemArray, cancellationToken, properties).ConfigureAwait(false);

                            if (record is not null)
                                yield return record;
                        }
                    }

                    IAsyncEnumerable<T?> sortResult = LocalOrderByAsync(loadKeyRecordFieldsFromCache(), sortField, ascending);
                    DataTable sortedKeyCache = new(s_tableName);

                    foreach (DataColumn column in PrimaryKeyCache.Columns)
                        sortedKeyCache.Columns.Add(column.ColumnName, column.DataType);

                    await foreach (T? record in sortResult.WithAwaitConfiguredCancellation(cancellationToken))
                        sortedKeyCache.Rows.Add(GetPrimaryKeys(record!));

                    PrimaryKeyCache = sortedKeyCache;
                }
            }
            catch (Exception ex)
            {
                InvalidOperationException opex = new($"Exception during record query for {typeof(T).Name} \"{sqlExpression ?? "undefined"}, {ValueList(restriction?.Parameters)}\": {ex.Message}", ex);

                if (ExceptionHandler is null)
                    throw opex;

                ExceptionHandler(opex);
                yield break;
            }

            m_lastSortField = sortField;
            m_lastRestriction = restriction;
        }

        // Paginate on cached data rows so paging does no work except to skip through records, then only load records for a given page of data 
        await foreach (Task<T?> recordTask in PrimaryKeyCache.AsAsyncEnumerable().Skip(page * pageSize).Take(pageSize).Select(row => LoadRecordFromCachedKeysAsync(row.ItemArray, cancellationToken)).WithAwaitConfiguredCancellation(cancellationToken))
        {
            T? record = await recordTask.ConfigureAwait(false);

            if (record is not null)
                yield return record;
        }
    }

    IAsyncEnumerable<object> ITableOperations.QueryRecordsAsync(string? sortField, bool ascending, int page, int pageSize, CancellationToken cancellationToken, params RecordRestriction?[]? restrictions)
    {
        return QueryRecordsAsync(sortField, ascending, page, pageSize, cancellationToken, restrictions).Cast<object>();
    }

    /// <inheritdoc/>
    public int QueryRecordCount()
    {
        return QueryRecordCount((RecordRestriction?[]?)null);
    }

    /// <inheritdoc/>
    public Task<int> QueryRecordCountAsync(CancellationToken cancellationToken)
    {
        return QueryRecordCountAsync(cancellationToken, (RecordRestriction?[]?)null);
    }

    /// <inheritdoc/>
    public int QueryRecordCount(params IRecordFilter?[]? recordFilter)
    {
        return QueryRecordCount(GetSearchRestrictions(recordFilter));
    }

    /// <inheritdoc/>
    public Task<int> QueryRecordCountAsync(CancellationToken cancellationToken, params IRecordFilter?[]? recordFilter)
    {
        return QueryRecordCountAsync(cancellationToken, GetSearchRestrictions(recordFilter));
    }

    /// <inheritdoc/>
    public int QueryRecordCount(params RecordRestriction?[]? restrictions)
    {
        string? sqlExpression = null;
        RecordRestriction? restriction = null;

        if (restrictions is not null)
            restriction = restrictions.Aggregate(restriction, (current, item) => current + item);

        try
        {
            if (RootQueryRestriction is not null)
                restriction = (RootQueryRestriction + restriction)!;

            if (restriction is null)
            {
                sqlExpression = m_selectCountSql;

                return Connection.ExecuteScalar<int>(sqlExpression);
            }

            sqlExpression = $"{m_selectCountSql} WHERE {UpdateFieldNames(restriction.FilterExpression)}";

            return Connection.ExecuteScalar<int>(sqlExpression, restriction.Parameters);
        }
        catch (Exception ex)
        {
            InvalidOperationException opex = new($"Exception during record count query for {typeof(T).Name} \"{sqlExpression ?? "undefined"}, {ValueList(restriction?.Parameters)}\": {ex.Message}", ex);

            if (ExceptionHandler is null)
                throw opex;

            ExceptionHandler(opex);

            return -1;
        }
    }

    /// <inheritdoc/>
    public Task<int> QueryRecordCountAsync(CancellationToken cancellationToken, params RecordRestriction?[]? restrictions)
    {
        string? sqlExpression = null;
        RecordRestriction? restriction = null;

        if (restrictions is not null)
            restriction = restrictions.Aggregate(restriction, (current, item) => current + item);

        try
        {
            if (RootQueryRestriction is not null)
                restriction = (RootQueryRestriction + restriction)!;

            if (restriction is null)
            {
                sqlExpression = m_selectCountSql;

                return Connection.ExecuteScalarAsync<int>(sqlExpression, cancellationToken);
            }

            sqlExpression = $"{m_selectCountSql} WHERE {UpdateFieldNames(restriction.FilterExpression)}";

            return Connection.ExecuteScalarAsync<int>(sqlExpression, cancellationToken, restriction.Parameters);
        }
        catch (Exception ex)
        {
            InvalidOperationException opex = new($"Exception during record count query for {typeof(T).Name} \"{sqlExpression ?? "undefined"}, {ValueList(restriction?.Parameters)}\": {ex.Message}", ex);

            if (ExceptionHandler is null)
                throw opex;

            ExceptionHandler(opex);

            return Task.FromResult(-1);
        }
    }

    /// <inheritdoc/>
    public int QueryRecordCountWhere(string? filterExpression, params object?[] parameters)
    {
        return QueryRecordCount(new RecordRestriction(filterExpression, parameters));
    }

    /// <inheritdoc/>
    public Task<int> QueryRecordCountWhereAsync(string? filterExpression, CancellationToken cancellationToken, params object?[] parameters)
    {
        return QueryRecordCountAsync(cancellationToken, new RecordRestriction(filterExpression, parameters));
    }

    /// <summary>
    /// Locally searches retrieved table records after queried from database for the specified sorting and search parameters.
    /// Search executed against fields modeled with <see cref="SearchableAttribute"/>.
    /// Function only typically used for record models that apply the <see cref="EncryptDataAttribute"/>.
    /// </summary>
    /// <param name="sortField">Field name to order-by.</param>
    /// <param name="ascending">Sort ascending flag; set to <c>false</c> for descending.</param>
    /// <param name="comparison"><see cref="StringComparison"/> to use when searching string fields; defaults to ordinal ignore case.</param>
    /// <param name="recordFilters">Record Filters to be applied.</param>
    /// <returns>An array of modeled table row instances for the queried records that match the search.</returns>
    /// <remarks>
    /// <para>
    /// This function searches records locally after query from database, this way Search functionality will work
    /// even with fields that are modeled with the <see cref="EncryptDataAttribute"/> and use restrictions not being = or =/=.
    /// Primary keys for this function will not be cached server-side and this function will be slower and more expensive than similar calls
    /// to <see cref="QueryRecords(string, bool, int, int, IRecordFilter[])"/>. Usage should be restricted to cases searching for field data that has
    /// been modeled with the <see cref="EncryptDataAttribute"/>.
    /// </para>
    /// <para>
    /// This function does not paginate records, instead a full list of search records is returned. User can cache returned records and page
    /// through them using the <see cref="GetPageOfRecords"/> function. As a result, usage should be restricted to smaller data sets. 
    /// </para>
    /// </remarks>
    public T?[]? SearchRecords(string? sortField, bool ascending, StringComparison comparison = StringComparison.OrdinalIgnoreCase, params IRecordFilter?[]? recordFilters)
    {
        if (recordFilters is null)
            return null;

        sortField = ValidateOrderByExpression(sortField);

        if (string.IsNullOrWhiteSpace(sortField))
            sortField = s_fieldNames[s_primaryKeyProperties[0].Name];

        bool sortFieldIsEncrypted = FieldIsEncrypted(sortField);
        string? orderByExpression = sortFieldIsEncrypted ? null : $"{sortField}{(ascending ? "" : " DESC")}";

        IRecordFilter[] validFilters = recordFilters.Where(filter => filter is not null).ToArray()!;

        RecordRestriction? restriction = validFilters.Aggregate((RecordRestriction?)null, (restriction, filter) => 
            filter.GenerateRestriction(this) + restriction);

        IEnumerable<T?> queryResult = QueryRecords(orderByExpression, restriction);

        if (sortFieldIsEncrypted)
            queryResult = LocalOrderBy(queryResult, sortField, ascending, comparison.GetComparer());

        return queryResult.ToArray();
    }

    // ReSharper disable once CoVariantArrayConversion
    object?[]? ITableOperations.SearchRecords(string? sortField, bool ascending, StringComparison comparison, params IRecordFilter?[]? recordFilter)
    {
        return SearchRecords(sortField, ascending, comparison, recordFilter);
    }

    /// <summary>
    /// Locally searches retrieved table records after queried from database for the specified sorting and search parameters.
    /// Search executed against fields modeled with <see cref="SearchableAttribute"/>.
    /// Function only typically used for record models that apply the <see cref="EncryptDataAttribute"/>.
    /// </summary>
    /// <param name="sortField">Field name to order-by.</param>
    /// <param name="ascending">Sort ascending flag; set to <c>false</c> for descending.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="comparison"><see cref="StringComparison"/> to use when searching string fields; defaults to ordinal ignore case.</param>
    /// <param name="recordFilters">Record Filters to be applied.</param>
    /// <returns>An array of modeled table row instances for the queried records that match the search.</returns>
    /// <remarks>
    /// <para>
    /// This function searches records locally after query from database, this way Search functionality will work
    /// even with fields that are modeled with the <see cref="EncryptDataAttribute"/> and use restrictions not being = or =/=.
    /// Primary keys for this function will not be cached server-side and this function will be slower and more expensive than similar calls
    /// to <see cref="QueryRecords(string, bool, int, int, IRecordFilter[])"/>. Usage should be restricted to cases searching for field data that has
    /// been modeled with the <see cref="EncryptDataAttribute"/>.
    /// </para>
    /// <para>
    /// This function does not paginate records, instead a full list of search records is returned. User can cache returned records and page
    /// through them using the <see cref="GetPageOfRecordsAsync"/> function. As a result, usage should be restricted to smaller data sets. 
    /// </para>
    /// </remarks>
    public IAsyncEnumerable<T?> SearchRecordsAsync(string? sortField, bool ascending, CancellationToken cancellationToken, StringComparison comparison = StringComparison.OrdinalIgnoreCase, params IRecordFilter?[]? recordFilters)
    {
        if (recordFilters is null)
            return AsyncEnumerable.Empty<T?>();

        sortField = ValidateOrderByExpression(sortField);

        if (string.IsNullOrWhiteSpace(sortField))
            sortField = s_fieldNames[s_primaryKeyProperties[0].Name];

        bool sortFieldIsEncrypted = FieldIsEncrypted(sortField);
        string? orderByExpression = sortFieldIsEncrypted ? null : $"{sortField}{(ascending ? "" : " DESC")}";

        IRecordFilter[] validFilters = recordFilters.Where(filter => filter is not null).ToArray()!;

        RecordRestriction? restriction = validFilters.Aggregate((RecordRestriction?)null, (restriction, filter) => 
            filter.GenerateRestriction(this) + restriction);

        IAsyncEnumerable<T?> queryResult = QueryRecordsAsync(orderByExpression, restriction, -1, cancellationToken);

        return sortFieldIsEncrypted ? 
            LocalOrderByAsync(queryResult, sortField, ascending, comparison.GetComparer()) : 
            queryResult;
    }

    IAsyncEnumerable<object?> ITableOperations.SearchRecordsAsync(string? sortField, bool ascending, CancellationToken cancellationToken, StringComparison comparison, params IRecordFilter?[]? recordFilter)
    {
        return SearchRecordsAsync(sortField, ascending, cancellationToken, comparison, recordFilter)!.Cast<object?>();
    }

    /// <summary>
    /// Gets the specified <paramref name="page"/> of records from the provided source <paramref name="records"/> array.
    /// </summary>
    /// <param name="records">Source records array.</param>
    /// <param name="page">Desired page of records.</param>
    /// <param name="pageSize">Desired page size.</param>
    /// <returns>A page of records.</returns>
    public IEnumerable<T?> GetPageOfRecords(T?[] records, int page, int pageSize)
    {
        return records.ToPagedList(page, pageSize, records.Length);
    }

    IEnumerable ITableOperations.GetPageOfRecords(object?[] records, int page, int pageSize)
    {
        try
        {
            return GetPageOfRecords(records.Cast<T?>().ToArray(), page, pageSize);
        }
        catch (InvalidCastException ex)
        {
            throw new ArgumentException($"One of the provided records cannot be converted to type \"{typeof(T).Name}\": {ex.Message}", nameof(records), ex);
        }
    }

    /// <summary>
    /// Gets the specified <paramref name="page"/> of records from the provided source <paramref name="records"/> array.
    /// </summary>
    /// <param name="records">Source records array.</param>
    /// <param name="page">Desired page of records.</param>
    /// <param name="pageSize">Desired page size.</param>
    /// <returns>A page of records.</returns>
    public IAsyncEnumerable<T?> GetPageOfRecordsAsync(IAsyncEnumerable<T?> records, int page, int pageSize)
    {
        return records.Skip(page * pageSize).Take(pageSize);
    }

    IAsyncEnumerable<object?> ITableOperations.GetPageOfRecordsAsync(IAsyncEnumerable<object?> records, int page, int pageSize)
    {
        try
        {
            return GetPageOfRecordsAsync(records!.Cast<T?>(), page, pageSize)!.Cast<object?>();
        }
        catch (InvalidCastException ex)
        {
            throw new ArgumentException($"One of the provided records cannot be converted to type \"{typeof(T).Name}\": {ex.Message}", nameof(records), ex);
        }
    }

    /// <summary>
    /// Creates a new modeled table record queried from the specified <paramref name="primaryKeys"/>.
    /// </summary>
    /// <param name="primaryKeys">Primary keys values of the record to load.</param>
    /// <returns>New modeled table record queried from the specified <paramref name="primaryKeys"/>.</returns>
    public T? LoadRecord(params object[] primaryKeys)
    {
        try
        {
            return Connection.TryRetrieveRow(m_selectRowSql, out DataRow? row, GetInterpretedPrimaryKeys(primaryKeys)) ? LoadRecord(row!) : null;

        }
        catch (Exception ex)
        {
            InvalidOperationException opex = new($"Exception during record load for {typeof(T).Name} \"{m_selectRowSql}, {ValueList(primaryKeys)}\": {ex.Message}", ex);

            if (ExceptionHandler is null)
                throw opex;

            ExceptionHandler(opex);

            return null;
        }
    }

    object? ITableOperations.LoadRecord(params object[] primaryKeys)
    {
        return LoadRecord(primaryKeys);
    }

    /// <summary>
    /// Creates a new modeled table record queried from the specified <paramref name="primaryKeys"/>.
    /// </summary>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="primaryKeys">Primary keys values of the record to load.</param>
    /// <returns>New modeled table record queried from the specified <paramref name="primaryKeys"/>.</returns>
    public async Task<T?> LoadRecordAsync(CancellationToken cancellationToken, params object[] primaryKeys)
    {
        try
        {
            (DataRow? row, bool success) = await Connection.TryRetrieveRowAsync(m_selectRowSql, cancellationToken, GetInterpretedPrimaryKeys(primaryKeys)).ConfigureAwait(false);
            return success ? LoadRecord(row!) : null;

        }
        catch (Exception ex)
        {
            InvalidOperationException opex = new($"Exception during record load for {typeof(T).Name} \"{m_selectRowSql}, {ValueList(primaryKeys)}\": {ex.Message}", ex);

            if (ExceptionHandler is null)
                throw opex;

            ExceptionHandler(opex);

            return null;
        }
    }

    async Task<object?> ITableOperations.LoadRecordAsync(CancellationToken cancellationToken, params object[] primaryKeys)
    {
        return await LoadRecordAsync(cancellationToken, primaryKeys).ConfigureAwait(false);
    }

    // Cached keys are not decrypted, so any needed record interpretation steps should skip encryption
    private T? LoadRecordFromCachedKeys(object?[] primaryKeys, IEnumerable<PropertyInfo>? properties = null)
    {
        try
        {
            return Connection.TryRetrieveRow(m_selectRowSql, out DataRow? row, GetInterpretedPrimaryKeys(primaryKeys, true)) ? LoadRecord(row!, properties ?? s_properties.Values) : null;

        }
        catch (Exception ex)
        {
            InvalidOperationException opex = new($"Exception during record load from primary key cache for {typeof(T).Name} \"{m_selectRowSql}, {ValueList(primaryKeys)}\": {ex.Message}", ex);

            if (ExceptionHandler is null)
                throw opex;

            ExceptionHandler(opex);

            return null;
        }
    }

    // Cached keys are not decrypted, so any needed record interpretation steps should skip encryption
    private async Task<T?> LoadRecordFromCachedKeysAsync(object?[] primaryKeys, CancellationToken cancellationToken, IEnumerable<PropertyInfo>? properties = null)
    {
        try
        {
            (DataRow? row, bool success) = await Connection.TryRetrieveRowAsync(m_selectRowSql, cancellationToken, GetInterpretedPrimaryKeys(primaryKeys, true)).ConfigureAwait(false);
            return success ? LoadRecord(row!, properties ?? s_properties.Values) : null;

        }
        catch (Exception ex)
        {
            InvalidOperationException opex = new($"Exception during record load from primary key cache for {typeof(T).Name} \"{m_selectRowSql}, {ValueList(primaryKeys)}\": {ex.Message}", ex);

            if (ExceptionHandler is null)
                throw opex;

            ExceptionHandler(opex);

            return null;
        }
    }

    /// <summary>
    /// Creates a new modeled table record queried from the specified <paramref name="row"/>.
    /// </summary>
    /// <param name="row"><see cref="DataRow"/> of queried data to be loaded.</param>
    /// <returns>New modeled table record queried from the specified <paramref name="row"/>.</returns>
    public T? LoadRecord(DataRow row)
    {
        return LoadRecord(row, s_properties.Values);
    }

    // This is the primary function where records are loaded from a DataRow into a modeled record of type T
    private T? LoadRecord(DataRow row, IEnumerable<PropertyInfo> properties)
    {
        try
        {
            T record = new();

            foreach (PropertyInfo property in properties)
            {
                try
                {
                    object? value = row.ConvertField(s_fieldNames[property.Name], property.PropertyType);

                    if (s_encryptDataTargets is not null && value is not null && s_encryptDataTargets.TryGetValue(property, out string? keyReference))
                        value = value.ToString()!.Decrypt(keyReference, CipherStrength.Aes256);

                    property.SetValue(record, value, null);
                }
                catch (Exception ex)
                {
                    InvalidOperationException opex = new($"Exception during record load field assignment for \"{typeof(T).Name}.{property.Name} = {row[s_fieldNames[property.Name]]}\": {ex.Message}", ex);

                    if (ExceptionHandler is null)
                        throw opex;

                    ExceptionHandler(opex);
                }
            }

            return record;
        }
        catch (Exception ex)
        {
            InvalidOperationException opex = new($"Exception during record load for {typeof(T).Name} from data row: {ex.Message}", ex);

            if (ExceptionHandler is null)
                throw opex;

            ExceptionHandler(opex);

            return null;
        }
    }

    object? ITableOperations.LoadRecord(DataRow row)
    {
        return LoadRecord(row);
    }

    /// <summary>
    /// Converts the given collection of <paramref name="records"/> into a <see cref="DataTable"/>.
    /// </summary>
    /// <param name="records">The collection of records to be inserted into the data table.</param>
    /// <returns>A data table containing data from the given records.</returns>
    public DataTable ToDataTable(IEnumerable<T?> records)
    {
        DataTable dataTable = s_tableSchema.Clone();

        foreach (T? record in records)
        {
            if (record is null)
                continue;

            DataRow row = dataTable.NewRow();

            foreach (PropertyInfo property in s_properties.Values)
                row[s_fieldNames[property.Name]] = property.GetValue(record);

            dataTable.Rows.Add(row);
        }

        return dataTable;
    }

    DataTable ITableOperations.ToDataTable(IEnumerable records)
    {
        try
        {
            return ToDataTable(records.Cast<T?>());
        }
        catch (InvalidCastException ex)
        {
            throw new ArgumentException($"One of the provided records cannot be converted to type \"{typeof(T).Name}\": {ex.Message}", nameof(records), ex);
        }
    }

    /// <summary>
    /// Converts the given collection of <paramref name="records"/> into a <see cref="DataTable"/>.
    /// </summary>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="records">The collection of records to be inserted into the data table.</param>
    /// <returns>A data table containing data from the given records.</returns>
    public async Task<DataTable> ToDataTableAsync(IAsyncEnumerable<T?> records, CancellationToken cancellationToken)
    {
        DataTable dataTable = s_tableSchema.Clone();

        await foreach (T? record in records.WithAwaitConfiguredCancellation(cancellationToken))
        {
            if (record is null)
                continue;

            DataRow row = dataTable.NewRow();

            foreach (PropertyInfo property in s_properties.Values)
                row[s_fieldNames[property.Name]] = property.GetValue(record);

            dataTable.Rows.Add(row);
        }

        return dataTable;
    }

    async Task<DataTable> ITableOperations.ToDataTableAsync(IAsyncEnumerable<object?> records, CancellationToken cancellationToken)
    {
        try
        {
            return await ToDataTableAsync(records!.Cast<T?>(), cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidCastException ex)
        {
            throw new ArgumentException($"One of the provided records cannot be converted to type \"{typeof(T).Name}\": {ex.Message}", nameof(records), ex);
        }
    }

    /// <inheritdoc/>
    public int DeleteRecord(params object[] primaryKeys)
    {
        try
        {
            int affectedRecords = Connection.ExecuteNonQuery(m_deleteSql, GetInterpretedPrimaryKeys(primaryKeys));

            if (affectedRecords > 0)
                PrimaryKeyCache = null;

            return affectedRecords;
        }
        catch (Exception ex)
        {
            InvalidOperationException opex = new($"Exception during record delete for {typeof(T).Name} \"{m_deleteSql}, {ValueList(primaryKeys)}\": {ex.Message}", ex);

            if (ExceptionHandler is null)
                throw opex;

            ExceptionHandler(opex);

            return 0;
        }
    }

    /// <inheritdoc/>
    public async Task<int> DeleteRecordAsync(CancellationToken cancellationToken, params object[] primaryKeys)
    {
        try
        {
            int affectedRecords = await Connection.ExecuteNonQueryAsync(m_deleteSql, cancellationToken, GetInterpretedPrimaryKeys(primaryKeys)).ConfigureAwait(false);

            if (affectedRecords > 0)
                PrimaryKeyCache = null;

            return affectedRecords;
        }
        catch (Exception ex)
        {
            InvalidOperationException opex = new($"Exception during record delete for {typeof(T).Name} \"{m_deleteSql}, {ValueList(primaryKeys)}\": {ex.Message}", ex);

            if (ExceptionHandler is null)
                throw opex;

            ExceptionHandler(opex);

            return 0;
        }
    }

    /// <summary>
    /// Deletes the specified modeled table <paramref name="record"/> from the database.
    /// </summary>
    /// <param name="record">Record to delete.</param>
    /// <returns>Number of rows affected.</returns>
    public int DeleteRecord(T record)
    {
        return DeleteRecord(GetPrimaryKeys(record));
    }

    int ITableOperations.DeleteRecord(object value)
    {
        if (value is not T record)
            throw new ArgumentException($"Cannot delete record of type \"{value?.GetType().Name ?? "null"}\", expected \"{typeof(T).Name}\"", nameof(value));

        return DeleteRecord(record);
    }

    /// <summary>
    /// Deletes the specified modeled table <paramref name="record"/> from the database.
    /// </summary>
    /// <param name="record">Record to delete.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <returns>Number of rows affected.</returns>
    public Task<int> DeleteRecordAsync(T record, CancellationToken cancellationToken)
    {
        return DeleteRecordAsync(cancellationToken, GetPrimaryKeys(record));
    }

    Task<int> ITableOperations.DeleteRecordAsync(object value, CancellationToken cancellationToken)
    {
        if (value is not T record)
            throw new ArgumentException($"Cannot delete record of type \"{value?.GetType().Name ?? "null"}\", expected \"{typeof(T).Name}\"", nameof(value));

        return DeleteRecordAsync(record, cancellationToken);
    }

    /// <inheritdoc/>
    public int DeleteRecord(DataRow row)
    {
        return DeleteRecord(GetPrimaryKeys(row));
    }

    /// <inheritdoc/>
    public Task<int> DeleteRecordAsync(DataRow row, CancellationToken cancellationToken)
    {
        return DeleteRecordAsync(cancellationToken, GetPrimaryKeys(row));
    }

    /// <inheritdoc/>
    public int DeleteRecord(RecordRestriction? restriction, bool? applyRootQueryRestriction = null)
    {
        ArgumentNullException.ThrowIfNull(restriction);

        string? sqlExpression = null;

        try
        {
            if (RootQueryRestriction is not null && (applyRootQueryRestriction ?? ApplyRootQueryRestrictionToDeletes))
                restriction = (RootQueryRestriction + restriction)!;

            sqlExpression = $"{m_deleteWhereSql}{UpdateFieldNames(restriction.FilterExpression)}";
            int affectedRecords = Connection.ExecuteNonQuery(sqlExpression, restriction.Parameters);

            if (affectedRecords > 0)
                PrimaryKeyCache = null;

            return affectedRecords;
        }
        catch (Exception ex)
        {
            InvalidOperationException opex = new($"Exception during record delete for {typeof(T).Name} \"{sqlExpression ?? "undefined"}, {ValueList(restriction.Parameters)}\": {ex.Message}", ex);

            if (ExceptionHandler is null)
                throw opex;

            ExceptionHandler(opex);

            return 0;
        }
    }

    /// <inheritdoc/>
    public async Task<int> DeleteRecordAsync(RecordRestriction? restriction, CancellationToken cancellationToken, bool? applyRootQueryRestriction = null)
    {
        ArgumentNullException.ThrowIfNull(restriction);

        string? sqlExpression = null;

        try
        {
            if (RootQueryRestriction is not null && (applyRootQueryRestriction ?? ApplyRootQueryRestrictionToDeletes))
                restriction = (RootQueryRestriction + restriction)!;

            sqlExpression = $"{m_deleteWhereSql}{UpdateFieldNames(restriction.FilterExpression)}";
            int affectedRecords = await Connection.ExecuteNonQueryAsync(sqlExpression, cancellationToken, restriction.Parameters).ConfigureAwait(false);

            if (affectedRecords > 0)
                PrimaryKeyCache = null;

            return affectedRecords;
        }
        catch (Exception ex)
        {
            InvalidOperationException opex = new($"Exception during record delete for {typeof(T).Name} \"{sqlExpression ?? "undefined"}, {ValueList(restriction.Parameters)}\": {ex.Message}", ex);

            if (ExceptionHandler is null)
                throw opex;

            ExceptionHandler(opex);

            return 0;
        }
    }

    /// <inheritdoc/>
    public int DeleteRecordWhere(string filterExpression, params object?[] parameters)
    {
        return DeleteRecord(new RecordRestriction(filterExpression, parameters));
    }

    /// <inheritdoc/>
    public Task<int> DeleteRecordWhereAsync(string filterExpression, CancellationToken cancellationToken, params object?[] parameters)
    {
        return DeleteRecordAsync(new RecordRestriction(filterExpression, parameters), cancellationToken);
    }

    /// <summary>
    /// Updates the database with the specified modeled table <paramref name="record"/>,
    /// any model properties marked with <see cref="UpdateValueExpressionAttribute"/> will
    /// be evaluated and applied before the record is provided to the data source.
    /// </summary>
    /// <param name="record">Record to update.</param>
    /// <param name="restriction">Record restriction to apply, if any.</param>
    /// <param name="applyRootQueryRestriction">
    /// Flag that determines if any existing <see cref="RootQueryRestriction"/> should be applied. Defaults to
    /// <see cref="ApplyRootQueryRestrictionToUpdates"/> setting.
    /// </param>
    /// <returns>Number of rows affected.</returns>
    /// <remarks>
    /// <para>
    /// Record restriction is only used for custom update expressions or in cases where modeled
    /// table has no defined primary keys.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// </remarks>
    public int UpdateRecord(T record, RecordRestriction? restriction = null, bool? applyRootQueryRestriction = null)
    {
        return UpdateRecordOperation(record, 0, (sqlFormat, _, parameters) => Connection.ExecuteNonQuery(sqlFormat, parameters), CancellationToken.None, restriction, applyRootQueryRestriction);
    }

    int ITableOperations.UpdateRecord(object value, RecordRestriction? restriction, bool? applyRootQueryRestriction)
    {
        if (value is not T record)
            throw new ArgumentException($"Cannot update record of type \"{value?.GetType().Name ?? "null"}\", expected \"{typeof(T).Name}\"", nameof(value));

        return UpdateRecord(record, restriction, applyRootQueryRestriction);
    }

    /// <summary>
    /// Updates the database with the specified modeled table <paramref name="record"/>,
    /// any model properties marked with <see cref="UpdateValueExpressionAttribute"/> will
    /// be evaluated and applied before the record is provided to the data source.
    /// </summary>
    /// <param name="record">Record to update.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="restriction">Record restriction to apply, if any.</param>
    /// <param name="applyRootQueryRestriction">
    /// Flag that determines if any existing <see cref="RootQueryRestriction"/> should be applied. Defaults to
    /// <see cref="ApplyRootQueryRestrictionToUpdates"/> setting.
    /// </param>
    /// <returns>Number of rows affected.</returns>
    /// <remarks>
    /// <para>
    /// Record restriction is only used for custom update expressions or in cases where modeled
    /// table has no defined primary keys.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// </remarks>
    public Task<int> UpdateRecordAsync(T record, CancellationToken cancellationToken, RecordRestriction? restriction = null, bool? applyRootQueryRestriction = null)
    {
        return UpdateRecordOperation(record, Task.FromResult(0), Connection.ExecuteNonQueryAsync, cancellationToken, restriction, applyRootQueryRestriction);
    }

    Task<int> ITableOperations.UpdateRecordAsync(object value, CancellationToken cancellationToken, RecordRestriction? restriction, bool? applyRootQueryRestriction)
    {
        if (value is not T record)
            throw new ArgumentException($"Cannot update record of type \"{value?.GetType().Name ?? "null"}\", expected \"{typeof(T).Name}\"", nameof(value));

        return UpdateRecordAsync(record, cancellationToken, restriction, applyRootQueryRestriction);
    }

    private TReturn UpdateRecordOperation<TReturn>(T record, TReturn zeroReturn, Func<string, CancellationToken, object?[], TReturn> updateAction, CancellationToken cancellationToken, RecordRestriction? restriction, bool? applyRootQueryRestriction)
    {
        List<object?> values = [];

        try
        {
            s_updateRecordInstance(new CurrentScope { Instance = record, TableOperations = this, Connection = Connection });

            if (RootQueryRestriction is not null && (applyRootQueryRestriction ?? ApplyRootQueryRestrictionToUpdates))
                restriction = (RootQueryRestriction + restriction)!;
        }
        catch (Exception ex)
        {
            if (ExceptionHandler is null)
                throw;

            ExceptionHandler(ex);

            return zeroReturn;
        }

        if (restriction is null)
        {
            try
            {
                foreach (PropertyInfo property in s_updateProperties)
                    values.Add(GetInterpretedPropertyValue(property, record));

                foreach (PropertyInfo property in s_primaryKeyProperties)
                    values.Add(GetInterpretedPropertyValue(property, record));

                return updateAction(m_updateSql, cancellationToken, values.ToArray());
            }
            catch (Exception ex)
            {
                InvalidOperationException opex = new($"Exception during record update for {typeof(T).Name} \"{m_updateSql}, {ValueList(values)}\": {ex.Message}", ex);

                if (ExceptionHandler is null)
                    throw opex;

                ExceptionHandler(opex);

                return zeroReturn;
            }
        }

        string? sqlExpression = null;

        try
        {
            foreach (PropertyInfo property in s_updateProperties)
                values.Add(GetInterpretedPropertyValue(property, record));

            values.AddRange(restriction.Parameters);

            List<object> updateWhereOffsets = [];
            int updateFieldIndex = s_updateProperties.Length;

            for (int i = 0; i < restriction.Parameters.Length; i++)
                updateWhereOffsets.Add($"{{{updateFieldIndex + i}}}");

            sqlExpression = $"{m_updateWhereSql}{string.Format(UpdateFieldNames(restriction.FilterExpression)!, updateWhereOffsets.ToArray())}";

            return updateAction(sqlExpression, cancellationToken, values.ToArray());
        }
        catch (Exception ex)
        {
            InvalidOperationException opex = new($"Exception during record update for {typeof(T).Name} \"{sqlExpression}, {ValueList(values)}\": {ex.Message}", ex);

            if (ExceptionHandler is null)
                throw opex;

            ExceptionHandler(opex);

            return zeroReturn;
        }
    }

    /// <summary>
    /// Updates the database with the specified modeled table <paramref name="record"/>
    /// referenced by the specified SQL filter expression and parameters, any model properties
    /// marked with <see cref="UpdateValueExpressionAttribute"/> will be evaluated and applied
    /// before the record is provided to the data source.
    /// </summary>
    /// <param name="record">Record to update.</param>
    /// <param name="filterExpression">
    /// Filter SQL expression for restriction as a composite format string - does not include WHERE.
    /// When escaping is needed for field names, use standard ANSI quotes.
    /// </param>
    /// <param name="parameters">Restriction parameter values.</param>
    /// <returns>Number of rows affected.</returns>
    /// <remarks>
    /// <para>
    /// Record restriction is only used for custom update expressions or in cases where modeled
    /// table has no defined primary keys.
    /// </para>
    /// <para>
    /// Each indexed parameter, e.g., "{0}", in the composite format <paramref name="filterExpression"/>
    /// will be converted into query parameters where each of the corresponding values in the
    /// <paramref name="parameters"/> collection will be applied as <see cref="IDbDataParameter"/>
    /// values to an executed <see cref="IDbCommand"/> query.
    /// </para>
    /// <para>
    /// If any of the specified <paramref name="parameters"/> reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// <para>
    /// If needed, field names that are escaped with standard ANSI quotes in the filter expression
    /// will be updated to reflect what is defined in the user model.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="UpdateRecord(T, RecordRestriction, bool?)"/>.
    /// </para>
    /// </remarks>
    public int UpdateRecordWhere(T record, string filterExpression, params object?[] parameters)
    {
        return UpdateRecord(record, new RecordRestriction(filterExpression, parameters));
    }

    int ITableOperations.UpdateRecordWhere(object value, string filterExpression, params object?[] parameters)
    {
        if (value is not T record)
            throw new ArgumentException($"Cannot update record of type \"{value?.GetType().Name ?? "null"}\", expected \"{typeof(T).Name}\"", nameof(value));

        return UpdateRecordWhere(record, filterExpression, parameters);
    }

    /// <summary>
    /// Updates the database with the specified modeled table <paramref name="record"/>
    /// referenced by the specified SQL filter expression and parameters, any model properties
    /// marked with <see cref="UpdateValueExpressionAttribute"/> will be evaluated and applied
    /// before the record is provided to the data source.
    /// </summary>
    /// <param name="record">Record to update.</param>
    /// <param name="filterExpression">
    /// Filter SQL expression for restriction as a composite format string - does not include WHERE.
    /// When escaping is needed for field names, use standard ANSI quotes.
    /// </param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">Restriction parameter values.</param>
    /// <returns>Number of rows affected.</returns>
    /// <remarks>
    /// <para>
    /// Record restriction is only used for custom update expressions or in cases where modeled
    /// table has no defined primary keys.
    /// </para>
    /// <para>
    /// Each indexed parameter, e.g., "{0}", in the composite format <paramref name="filterExpression"/>
    /// will be converted into query parameters where each of the corresponding values in the
    /// <paramref name="parameters"/> collection will be applied as <see cref="IDbDataParameter"/>
    /// values to an executed <see cref="IDbCommand"/> query.
    /// </para>
    /// <para>
    /// If any of the specified <paramref name="parameters"/> reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// <para>
    /// If needed, field names that are escaped with standard ANSI quotes in the filter expression
    /// will be updated to reflect what is defined in the user model.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="UpdateRecord(T, RecordRestriction, bool?)"/>.
    /// </para>
    /// </remarks>
    public Task<int> UpdateRecordWhereAsync(T record, string filterExpression, CancellationToken cancellationToken, params object?[] parameters)
    {
        return UpdateRecordAsync(record, cancellationToken, new RecordRestriction(filterExpression, parameters));
    }

    Task<int> ITableOperations.UpdateRecordWhereAsync(object value, string filterExpression, CancellationToken cancellationToken, params object?[] parameters)
    {
        if (value is not T record)
            throw new ArgumentException($"Cannot update record of type \"{value?.GetType().Name ?? "null"}\", expected \"{typeof(T).Name}\"", nameof(value));

        return UpdateRecordWhereAsync(record, filterExpression, cancellationToken, parameters);
    }

    /// <inheritdoc/>
    public int UpdateRecord(DataRow row, RecordRestriction? restriction = null)
    {
        return UpdateRecord(LoadRecord(row)!, restriction);
    }

    /// <inheritdoc/>
    public Task<int> UpdateRecordAsync(DataRow row, CancellationToken cancellationToken, RecordRestriction? restriction = null)
    {
        return UpdateRecordAsync(LoadRecord(row)!, cancellationToken, restriction);
    }

    /// <inheritdoc/>
    public int UpdateRecordWhere(DataRow row, string filterExpression, params object?[] parameters)
    {
        return UpdateRecord(row, new RecordRestriction(filterExpression, parameters));
    }

    /// <inheritdoc/>
    public Task<int> UpdateRecordWhereAsync(DataRow row, string filterExpression, CancellationToken cancellationToken, params object?[] parameters)
    {
        return UpdateRecordAsync(row, cancellationToken, new RecordRestriction(filterExpression, parameters));
    }

    /// <summary>
    /// Adds the specified modeled table <paramref name="record"/> to the database.
    /// </summary>
    /// <param name="record">Record to add.</param>
    /// <returns>Number of rows affected.</returns>
    public int AddNewRecord(T record)
    {
        List<object?> values = [];

        try
        {
            foreach (PropertyInfo property in s_addNewProperties)
                values.Add(GetInterpretedPropertyValue(property, record));

            int affectedRecords = Connection.ExecuteNonQuery(m_addNewSql, values.ToArray());

            if (affectedRecords > 0)
                PrimaryKeyCache = null;

            return affectedRecords;
        }
        catch (Exception ex)
        {
            InvalidOperationException opex = new($"Exception during record insert for {typeof(T).Name} \"{m_addNewSql}, {ValueList(values)}\": {ex.Message}", ex);

            if (ExceptionHandler is null)
                throw opex;

            ExceptionHandler(opex);

            return 0;
        }
    }

    int ITableOperations.AddNewRecord(object value)
    {
        if (value is not T record)
            throw new ArgumentException($"Cannot add new record of type \"{value?.GetType().Name ?? "null"}\", expected \"{typeof(T).Name}\"", nameof(value));

        return AddNewRecord(record);
    }

    /// <summary>
    /// Adds the specified modeled table <paramref name="record"/> to the database.
    /// </summary>
    /// <param name="record">Record to add.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <returns>Number of rows affected.</returns>
    public async Task<int> AddNewRecordAsync(T record, CancellationToken cancellationToken)
    {
        List<object?> values = [];

        try
        {
            foreach (PropertyInfo property in s_addNewProperties)
                values.Add(GetInterpretedPropertyValue(property, record));

            int affectedRecords = await Connection.ExecuteNonQueryAsync(m_addNewSql, cancellationToken, values.ToArray()).ConfigureAwait(false);

            if (affectedRecords > 0)
                PrimaryKeyCache = null;

            return affectedRecords;
        }
        catch (Exception ex)
        {
            InvalidOperationException opex = new($"Exception during record insert for {typeof(T).Name} \"{m_addNewSql}, {ValueList(values)}\": {ex.Message}", ex);

            if (ExceptionHandler is null)
                throw opex;

            ExceptionHandler(opex);

            return 0;
        }
    }

    Task<int> ITableOperations.AddNewRecordAsync(object value, CancellationToken cancellationToken)
    {
        if (value is not T record)
            throw new ArgumentException($"Cannot add new record of type \"{value?.GetType().Name ?? "null"}\", expected \"{typeof(T).Name}\"", nameof(value));

        return AddNewRecordAsync(record, cancellationToken);
    }

    /// <inheritdoc/>
    public int AddNewRecord(DataRow row)
    {
        return AddNewRecord(LoadRecord(row)!);
    }

    /// <inheritdoc/>
    public Task<int> AddNewRecordAsync(DataRow row, CancellationToken cancellationToken)
    {
        return AddNewRecordAsync(LoadRecord(row)!, cancellationToken);
    }

    /// <summary>
    /// Adds the specified modeled table <paramref name="record"/> to the database if the
    /// record has not defined any of its primary key values; otherwise, the database will
    /// be updated with the specified modeled table <paramref name="record"/>.
    /// </summary>
    /// <param name="record">Record to add or update.</param>
    /// <returns>Number of rows affected.</returns>
    public int AddNewOrUpdateRecord(T record)
    {
        return s_primaryKeyProperties.All(property => Common.IsDefaultValue(property.GetValue(record)))
            ? AddNewRecord(record)
            : UpdateRecord(record);
    }

    int ITableOperations.AddNewOrUpdateRecord(object value)
    {
        if (value is not T record)
            throw new ArgumentException($"Cannot add new or update record of type \"{value?.GetType().Name ?? "null"}\", expected \"{typeof(T).Name}\"", nameof(value));

        return AddNewOrUpdateRecord(record);
    }

    /// <summary>
    /// Adds the specified modeled table <paramref name="record"/> to the database if the
    /// record has not defined any of its primary key values; otherwise, the database will
    /// be updated with the specified modeled table <paramref name="record"/>.
    /// </summary>
    /// <param name="record">Record to add or update.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <returns>Number of rows affected.</returns>
    public Task<int> AddNewOrUpdateRecordAsync(T record, CancellationToken cancellationToken)
    {
        return s_primaryKeyProperties.All(property => Common.IsDefaultValue(property.GetValue(record)))
            ? AddNewRecordAsync(record, cancellationToken)
            : UpdateRecordAsync(record, cancellationToken);
    }

    Task<int> ITableOperations.AddNewOrUpdateRecordAsync(object value, CancellationToken cancellationToken)
    {
        if (value is not T record)
            throw new ArgumentException($"Cannot add new or update record of type \"{value?.GetType().Name ?? "null"}\", expected \"{typeof(T).Name}\"", nameof(value));

        return AddNewOrUpdateRecordAsync(record, cancellationToken);
    }

    /// <summary>
    /// Gets the primary key values from the specified <paramref name="record"/>.
    /// </summary>
    /// <param name="record">Record of data to retrieve primary keys from.</param>
    /// <returns>Primary key values from the specified <paramref name="record"/>.</returns>
    public object[] GetPrimaryKeys(T record)
    {
        try
        {
            List<object> values = [];

            foreach (PropertyInfo property in s_primaryKeyProperties)
                values.Add(property.GetValue(record)!);

            return values.ToArray();
        }
        catch (Exception ex)
        {
            InvalidOperationException opex = new($"Exception loading primary key fields for {typeof(T).Name} \"{s_primaryKeyProperties.Select(property => property.Name).ToDelimitedString(", ")}\": {ex.Message}", ex);

            if (ExceptionHandler is null)
                throw opex;

            ExceptionHandler(opex);

            return [];
        }
    }

    object[] ITableOperations.GetPrimaryKeys(object value)
    {
        if (value is not T record)
            throw new ArgumentException($"Cannot get primary keys for record of type \"{value?.GetType().Name ?? "null"}\", expected \"{typeof(T).Name}\"", nameof(value));

        return GetPrimaryKeys(record);
    }

    /// <inheritdoc/>
    public object[] GetPrimaryKeys(DataRow row)
    {
        try
        {
            List<object> values = [];

            foreach (PropertyInfo property in s_primaryKeyProperties)
                values.Add(row[s_fieldNames[property.Name]]);

            return values.ToArray();
        }
        catch (Exception ex)
        {
            InvalidOperationException opex = new($"Exception loading primary key fields for {typeof(T).Name} \"{s_primaryKeyProperties.Select(property => property.Name).ToDelimitedString(", ")}\": {ex.Message}", ex);

            if (ExceptionHandler is null)
                throw opex;

            ExceptionHandler(opex);

            return [];
        }
    }

    /// <summary>
    /// Gets a record restriction based on the non-primary key values of the specified <paramref name="record"/>.
    /// </summary>
    /// <param name="record">Record to retrieve non-primary key field values from.</param>
    /// <returns>Record restriction based on the non-primary key values of the specified <paramref name="record"/>.</returns>
    /// <remarks>
    /// This will look up a newly added record when the primary key values are not yet defined searching all field values.
    /// If all fields do not represent a unique record, queries based on this restriction will return multiple records.
    /// Note that if the modeled table has fields that are known be unique, searching based on those fields is preferred.
    /// </remarks>
    public RecordRestriction GetNonPrimaryFieldRecordRestriction(T record)
    {
        string[] fieldNames = GetNonPrimaryFieldNames();

        return new RecordRestriction(
            fieldNames.Select((fieldName, index) => $"{fieldName} = {{{index}}}").ToDelimitedString(" AND "),
            fieldNames.Select(fieldName => GetFieldValue(record, fieldName)).ToArray());
    }

    RecordRestriction ITableOperations.GetNonPrimaryFieldRecordRestriction(object value)
    {
        if (value is not T record)
            throw new ArgumentException($"Cannot get non-primary key field restriction for record of type \"{value?.GetType().Name ?? "null"}\", expected \"{typeof(T).Name}\"", nameof(value));
        
        return GetNonPrimaryFieldRecordRestriction(record);
    }

    /// <inheritdoc/>
    public string[] GetFieldNames(bool escaped = true)
    {
        // Fields in the field names dictionary are stored in unescaped format
        return escaped ? 
            s_fieldNames.Values.Select(fieldName => GetEscapedFieldName(fieldName)).ToArray() : 
            s_fieldNames.Values.ToArray();
    }

    /// <inheritdoc/>
    public string[] GetNonPrimaryFieldNames(bool escaped = true)
    {
        HashSet<string> primaryKeyFields = new(GetPrimaryKeyFieldNames(escaped), UseCaseSensitiveFieldNames ? StringComparer.Ordinal : StringComparer.OrdinalIgnoreCase);

        // Fields in the field names dictionary are stored in unescaped format
        return escaped ?
            s_fieldNames.Values.Select(fieldName => GetEscapedFieldName(fieldName)).Where(fieldName => !primaryKeyFields.Contains(fieldName)).ToArray() :
            s_fieldNames.Values.Where(fieldName => !primaryKeyFields.Contains(fieldName)).ToArray();
    }

    /// <inheritdoc/>
    public string[] GetPrimaryKeyFieldNames(bool escaped = true)
    {
        return escaped ?
            s_primaryKeyFields.Split(',').Select(fieldName => GetEscapedFieldName(fieldName.Trim())).ToArray() : 
            s_primaryKeyFields.Split(',').Select(fieldName => GetUnescapedFieldName(fieldName.Trim())).ToArray();
    }

    /// <inheritdoc/>
    public bool TryGetFieldAttribute<TAttribute>(string fieldName, out TAttribute? attribute) where TAttribute : Attribute
    {
        if (s_propertyNames.TryGetValue(fieldName, out string? propertyName) && s_properties.TryGetValue(propertyName, out PropertyInfo? property) && property.TryGetAttribute(out attribute))
            return true;

        attribute = null!;

        return false;
    }

    /// <inheritdoc/>
    public bool TryGetFieldAttribute(string fieldName, Type attributeType, out Attribute? attribute)
    {
        if (!typeof(Attribute).IsAssignableFrom(attributeType))
            throw new ArgumentException($"The specified type \"{attributeType.Name}\" is not an Attribute.", nameof(attributeType));

        if (s_propertyNames.TryGetValue(fieldName, out string? propertyName) && s_properties.TryGetValue(propertyName, out PropertyInfo? property) && property.TryGetAttribute(attributeType, out attribute))
            return true;

        attribute = null;

        return false;
    }

    /// <inheritdoc/>
    public bool FieldHasAttribute<TAttribute>(string fieldName) where TAttribute : Attribute
    {
        return FieldHasAttribute(fieldName, typeof(TAttribute));
    }

    /// <inheritdoc/>
    public bool FieldHasAttribute(string fieldName, Type attributeType)
    {
        if (!attributeType.IsSubclassOf(typeof(Attribute)))
            throw new ArgumentException($"The specified type \"{attributeType.Name}\" is not an Attribute.", nameof(attributeType));

        if (s_propertyNames.TryGetValue(fieldName, out string? propertyName) && s_properties.TryGetValue(propertyName, out PropertyInfo? property) && s_attributes.TryGetValue(property, out HashSet<Type>? attributes))
            return attributes.Contains(attributeType);

        return false;
    }

    /// <summary>
    /// Gets the value for the specified field.
    /// </summary>
    /// <param name="record">Modeled table record.</param>
    /// <param name="fieldName">Field name to retrieve.</param>
    /// <returns>Field value or <c>null</c> if field is not found.</returns>
    public object? GetFieldValue(T? record, string fieldName)
    {
        if (s_propertyNames.TryGetValue(fieldName, out string? propertyName) && s_properties.TryGetValue(propertyName, out PropertyInfo? property))
            return property.GetValue(record);

        return typeof(T).GetProperty(fieldName)?.GetValue(record);
    }

    object? ITableOperations.GetFieldValue(object? value, string fieldName)
    {
        if (value is not T record)
            throw new ArgumentException($"Cannot get \"{fieldName}\" field value for record of type \"{value?.GetType().Name ?? "null"}\", expected \"{typeof(T).Name}\"", nameof(value));

        return GetFieldValue(record, fieldName);
    }

    /// <inheritdoc/>
    public object? GetInterpretedFieldValue(string fieldName, object? value)
    {
        if (s_fieldDataTypeTargets is null && s_encryptDataTargets is null)
            return value;

        if (s_propertyNames.TryGetValue(fieldName, out string? propertyName) && s_properties.TryGetValue(propertyName, out PropertyInfo? property))
            return GetInterpretedValue(property, value);

        return value;
    }

    /// <inheritdoc/>
    public Type? GetFieldType(string fieldName)
    {
        if (s_propertyNames.TryGetValue(fieldName, out string? propertyName) && s_properties.TryGetValue(propertyName, out PropertyInfo? property))
            return property.PropertyType;

        return null;
    }

    /// <inheritdoc/>
    public bool FieldExists(string fieldName)
    {
        return s_validFieldNames.Contains(fieldName);
    }

    /// <inheritdoc/>
    public RecordRestriction[]? GetSearchRestrictions(params IRecordFilter?[]? recordFilters)
    {
        return recordFilters?.Where(recordFilter => !string.IsNullOrWhiteSpace(recordFilter?.FieldName))
            .Select(recordFilter => recordFilter!.GenerateRestriction(this))
            .ToArray();
    }

    /// <inheritdoc/>
    public int GetPrimaryKeyCacheSize()
    {
        return PrimaryKeyCache?.Rows.Count ?? 0;
    }

    /// <inheritdoc/>
    public void ClearPrimaryKeyCache()
    {
        PrimaryKeyCache = null;
    }

    // Derive raw or encrypted field values or IDbCommandParameter values with specific DbType if
    // a primary key field data type has been targeted for specific database type
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private object?[] GetInterpretedPrimaryKeys(object?[] primaryKeys, bool skipEncryption = false)
    {
        if (s_fieldDataTypeTargets is null && s_encryptDataTargets is null)
            return primaryKeys;

        object?[] interpretedKeys = new object[s_primaryKeyProperties.Length];

        for (int i = 0; i < interpretedKeys.Length; i++)
            interpretedKeys[i] = GetInterpretedValue(s_primaryKeyProperties[i], primaryKeys[i], skipEncryption);

        return interpretedKeys;
    }

    // Derive raw or encrypted field values or IDbCommandParameter values with specific DbType if
    // a primary key field data type has been targeted for specific database type
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private object? GetInterpretedPropertyValue(PropertyInfo property, T record)
    {
        object? value = property.GetValue(record);

        if (value is char && Connection.DatabaseType == DatabaseType.SQLite)
            value = value.ToString();

        if (s_fieldDataTypeTargets is null && s_encryptDataTargets is null)
            return value;

        return GetInterpretedValue(property, value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private object? GetInterpretedValue(PropertyInfo property, object? value, bool skipEncryption = false)
    {
        if (!skipEncryption && s_encryptDataTargets is not null && value is not null && s_encryptDataTargets.TryGetValue(property, out string? keyReference))
            value = value.ToString()!.Encrypt(keyReference, CipherStrength.Aes256);

        if (s_fieldDataTypeTargets is not null && s_fieldDataTypeTargets.TryGetValue(property, out Dictionary<DatabaseType, DbType>? fieldDataTypeTargets) && fieldDataTypeTargets is not null && fieldDataTypeTargets.TryGetValue(Connection.DatabaseType, out DbType fieldDataType))
        {
            return new IntermediateParameter
            {
                Value = value, 
                DbType = fieldDataType
            };
        }

        return value;
    }

    // Derive table name, escaping it if requested by model
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string GetEscapedTableName()
    {
        if (s_escapedTableNameTargets is null)
            return s_tableName;

        if (s_escapedTableNameTargets.TryGetValue(Connection.DatabaseType, out bool useAnsiQuotes))
            return Connection.EscapeIdentifier(s_tableName, useAnsiQuotes);

        return s_tableName;
    }

    // Derive field name, escaping it if requested by model
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string GetEscapedFieldName(string fieldName, Dictionary<DatabaseType, bool>? escapedFieldNameTargets = null)
    {
        if (s_escapedFieldNameTargets is null)
            return fieldName;

        if (escapedFieldNameTargets is null && !s_escapedFieldNameTargets.TryGetValue(fieldName, out escapedFieldNameTargets) || escapedFieldNameTargets is null)
            return fieldName;

        if (escapedFieldNameTargets.TryGetValue(Connection.DatabaseType, out bool useAnsiQuotes))
            return Connection.EscapeIdentifier(fieldName, useAnsiQuotes);

        return fieldName;
    }

    // Derive field name, unescaping it if it was escaped by the model
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string GetUnescapedFieldName(string fieldName)
    {
        if (s_escapedFieldNameTargets is null)
            return fieldName;

        return s_escapedFieldNameTargets.TryGetValue(fieldName, out _) ? 
            fieldName.Substring(1, fieldName.Length - 2) : 
            fieldName;
    }

    // Update field names in expression, escaping or unescaping as needed as defined by model
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string? UpdateFieldNames(string? filterExpression)
    {
        if (filterExpression is null)
            return null;

        if (s_escapedFieldNameTargets is not null)
        {
            foreach (KeyValuePair<string, Dictionary<DatabaseType, bool>?> escapedFieldNameTarget in s_escapedFieldNameTargets)
            {
                string fieldName = escapedFieldNameTarget.Key;
                string derivedFieldName = GetEscapedFieldName(fieldName, escapedFieldNameTarget.Value);
                string ansiEscapedFieldName = $"\"{fieldName}\"";

                if (UseCaseSensitiveFieldNames)
                {
                    if (!derivedFieldName.Equals(ansiEscapedFieldName))
                        filterExpression = filterExpression.Replace(ansiEscapedFieldName, derivedFieldName);
                }
                else
                {
                    if (!derivedFieldName.Equals(ansiEscapedFieldName, StringComparison.OrdinalIgnoreCase))
                        filterExpression = filterExpression.ReplaceCaseInsensitive(ansiEscapedFieldName, derivedFieldName);
                }
            }
        }

        return filterExpression;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool FieldIsEncrypted(string fieldName)
    {
        return s_encryptDataTargets is not null &&
               s_propertyNames.TryGetValue(fieldName, out string? propertyName) &&
               s_properties.TryGetValue(propertyName, out PropertyInfo? property) &&
               s_encryptDataTargets.ContainsKey(property);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IEnumerable<T?> LocalOrderBy(IEnumerable<T?> queryResults, string sortField, bool ascending, StringComparer? comparer = null)
    {
        // Execute order-by locally on unencrypted data
        return ascending ?
            queryResults.OrderBy(record => GetFieldValue(record, sortField) as string, comparer ?? StringComparer.OrdinalIgnoreCase) :
            queryResults.OrderByDescending(record => GetFieldValue(record, sortField) as string, comparer ?? StringComparer.OrdinalIgnoreCase);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IAsyncEnumerable<T?> LocalOrderByAsync(IAsyncEnumerable<T?> queryResults, string sortField, bool ascending, StringComparer? comparer = null)
    {
        // Execute order-by locally on unencrypted data
        return ascending ?
            queryResults.OrderBy(record => GetFieldValue(record, sortField) as string, comparer ?? StringComparer.OrdinalIgnoreCase) :
            queryResults.OrderByDescending(record => GetFieldValue(record, sortField) as string, comparer ?? StringComparer.OrdinalIgnoreCase);
    }

    // Validates that an order by expression is either an explicitly allowed via the 'SortExtensionAttribute' or a field in the model 
    private string? ValidateOrderByExpression(string? orderByExpression)
    {
        if (string.IsNullOrWhiteSpace(orderByExpression))
            return orderByExpression;

        // Split the expression by commas to handle multiple fields
        string[] fieldExpressions = orderByExpression.Split(',');
        List<string> validatedExpressions = [];

        foreach (string fieldExpression in fieldExpressions)
        {
            string trimmedExpression = fieldExpression.Trim();

            if (string.IsNullOrWhiteSpace(trimmedExpression))
                continue;

            // Parse the field name and any optional direction (ASC/DESC)
            string fieldName, direction;

            // Check for ASC/DESC keywords
            if (trimmedExpression.EndsWith(" ASC", StringComparison.OrdinalIgnoreCase))
            {
                fieldName = trimmedExpression[..^4].Trim();
                direction = " ASC";
            }
            else if (trimmedExpression.EndsWith(" DESC", StringComparison.OrdinalIgnoreCase))
            {
                fieldName = trimmedExpression[..^5].Trim();
                direction = " DESC";
            }
            else
            {
                // No direction specified
                fieldName = trimmedExpression;
                direction = string.Empty;
            }

            // Check if the field matches any SortExtension pre-compiled regex patterns
            bool matchFound = false;

            foreach ((Regex fieldMatchExpression, Func<string, string> sortExtensionMethod) in s_sortExtensions)
            {
                if (!fieldMatchExpression.IsMatch(fieldName))
                    continue;

                // Call the delegate method to get the coded order by expression
                string expression = sortExtensionMethod(fieldName);
                validatedExpressions.Add(expression + direction);
                matchFound = true;

                // Exit after first match, possible duplicate field matches are ignored - models with wide
                // field match expression should ensure that the most specific expressions are listed first
                break;
            }

            if (matchFound)
                continue;

            // If no SortExtension method matches, check if it's a valid field in the model
            if (!FieldExists(fieldName))
                throw new InvalidExpressionException($"\"{fieldName}\" is not a valid field in the order by expression");

            validatedExpressions.Add(fieldName + direction);
        }

        // Combine all validated expressions back with commas
        return string.Join(", ", validatedExpressions);
    }

    #endregion

    #region [ Static ]

    // Static Fields
    private static readonly string s_tableName;
    private static readonly Dictionary<string, PropertyInfo> s_properties;
    private static readonly Dictionary<string, string> s_fieldNames;
    private static readonly Dictionary<string, string> s_propertyNames;
    private static readonly Dictionary<PropertyInfo, HashSet<Type>> s_attributes;
    private static readonly PropertyInfo[] s_addNewProperties;
    private static readonly PropertyInfo[] s_updateProperties;
    private static readonly PropertyInfo[] s_primaryKeyProperties;
    private static readonly Dictionary<PropertyInfo, Dictionary<DatabaseType, DbType>?>? s_fieldDataTypeTargets;
    private static readonly Dictionary<PropertyInfo, string>? s_encryptDataTargets;
    private static readonly Dictionary<DatabaseType, bool>? s_escapedTableNameTargets;
    private static readonly Dictionary<string, Dictionary<DatabaseType, bool>?>? s_escapedFieldNameTargets;
    private static readonly List<(DatabaseType, TargetExpression, StatementTypes, AffixPosition, string)>? s_expressionAmendments;
    private static readonly RootQueryRestrictionAttribute? s_rootQueryRestrictionAttribute;
    private static readonly string s_selectCountSql;
    private static readonly string s_selectSetSql;
    private static readonly string s_selectSetWhereSql;
    private static readonly string s_selectKeysSql;
    private static readonly string s_selectKeysWhereSql;
    private static readonly string s_selectRowSql;
    private static readonly string s_addNewSql;
    private static readonly string s_updateSql;
    private static readonly string s_updateWhereSql;
    private static readonly string s_deleteSql;
    private static readonly string s_deleteWhereSql;
    private static readonly string s_primaryKeyFields;
    private static readonly bool s_hasPrimaryKeyIdentityField;
    private static readonly Func<CurrentScope, T> s_createRecordInstance;
    private static readonly Action<CurrentScope> s_updateRecordInstance;
    private static readonly Action<CurrentScope> s_applyRecordDefaults;
    private static readonly DataTable s_tableSchema;
    private static readonly HashSet<string> s_validFieldNames;
    private static readonly HashSet<string> s_searchableFields;
    private static readonly (Regex, Func<IRecordFilter, RecordRestriction>)[] s_searchExtensions;
    private static readonly (Regex, Func<string, string>)[] s_sortExtensions;
    private static TypeRegistry? s_typeRegistry;

    // Static Constructor
    static TableOperations()
    {
        StringBuilder addNewFields = new();
        StringBuilder addNewFormat = new();
        StringBuilder updateFormat = new();
        StringBuilder whereFormat = new();
        StringBuilder allFields = new("*");
        StringBuilder primaryKeyFields = new();
        List<PropertyInfo> addNewProperties = [];
        List<PropertyInfo> updateProperties = [];
        List<PropertyInfo> primaryKeyProperties = [];
        int primaryKeyIndex = 0;
        int addNewFieldIndex = 0;
        int updateFieldIndex = 0;

        // Table name will default to class name of modeled table
        s_tableName = typeof(T).Name;

        // Check for overridden table name
        if (typeof(T).TryGetAttribute(out TableNameAttribute? tableNameAttribute) && !string.IsNullOrWhiteSpace(tableNameAttribute.TableName))
            s_tableName = tableNameAttribute.TableName;

        // Check for escaped table name targets
        if (typeof(T).TryGetAttributes(out UseEscapedNameAttribute[]? useEscapedNameAttributes))
            s_escapedTableNameTargets = DeriveEscapedNameTargets(useEscapedNameAttributes!);

        // Check for expression amendments
        if (typeof(T).TryGetAttributes(out AmendExpressionAttribute[]? amendExpressionAttributes))
            s_expressionAmendments = DeriveExpressionAmendments(amendExpressionAttributes!);

        // Check for root query restriction
        typeof(T).TryGetAttribute(out s_rootQueryRestrictionAttribute);

        s_properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(property => property is { CanRead: true, CanWrite: true })
            .Where(property => !property.AttributeExists<PropertyInfo, NonRecordFieldAttribute>())
            .ToDictionary(property => property.Name, StringComparer.OrdinalIgnoreCase);

        s_fieldNames = s_properties.ToDictionary(kvp => kvp.Key, kvp => GetFieldName(kvp.Value), StringComparer.OrdinalIgnoreCase);
        s_propertyNames = s_fieldNames.ToDictionary(kvp => kvp.Value, kvp => kvp.Key, StringComparer.OrdinalIgnoreCase);
        s_attributes = new Dictionary<PropertyInfo, HashSet<Type>>();
        s_hasPrimaryKeyIdentityField = false;

        foreach (PropertyInfo property in s_properties.Values)
        {
            string fieldName = s_fieldNames[property.Name];

            property.TryGetAttribute(out PrimaryKeyAttribute? primaryKeyAttribute);
                
            if (property.TryGetAttribute(out EncryptDataAttribute? encryptDataAttribute) && property.PropertyType == typeof(string))
            {
                s_encryptDataTargets ??= new Dictionary<PropertyInfo, string>();
                s_encryptDataTargets[property] = encryptDataAttribute.KeyReference;
            }

            if (property.TryGetAttributes(out FieldDataTypeAttribute[]? fieldDataTypeAttributes))
            {
                s_fieldDataTypeTargets ??= new Dictionary<PropertyInfo, Dictionary<DatabaseType, DbType>?>();
                s_fieldDataTypeTargets[property] = DeriveFieldDataTypeTargets(fieldDataTypeAttributes!);
            }

            if (property.TryGetAttributes(out useEscapedNameAttributes))
            {
                s_escapedFieldNameTargets ??= new Dictionary<string, Dictionary<DatabaseType, bool>?>(StringComparer.OrdinalIgnoreCase);
                s_escapedFieldNameTargets[fieldName] = DeriveEscapedNameTargets(useEscapedNameAttributes!);

                // If any database has been targeted for escaping the field name, pre-apply the standard ANSI escaped
                // field name in the static SQL expressions. This will provide a unique replaceable identifier should
                // the common database delimiters, or no delimiters, be applicable for an active database connection
                fieldName = $"\"{fieldName}\"";
            }

            if (primaryKeyAttribute is null)
            {
                addNewFields.Append($"{(addNewFields.Length > 0 ? ", " : "")}{fieldName}");
                addNewFormat.Append($"{(addNewFormat.Length > 0 ? ", " : "")}{{{addNewFieldIndex++}}}");
                updateFormat.Append($"{(updateFormat.Length > 0 ? ", " : "")}{fieldName}={{{updateFieldIndex++}}}");
                addNewProperties.Add(property);
                updateProperties.Add(property);
            }
            else
            {
                if (primaryKeyAttribute.IsIdentity)
                {
                    s_hasPrimaryKeyIdentityField = true;
                }
                else
                {
                    addNewFields.Append($"{(addNewFields.Length > 0 ? ", " : "")}{fieldName}");
                    addNewFormat.Append($"{(addNewFormat.Length > 0 ? ", " : "")}{{{addNewFieldIndex++}}}");
                    addNewProperties.Add(property);
                }

                whereFormat.Append($"{(whereFormat.Length > 0 ? " AND " : "")}{fieldName}={{{primaryKeyIndex++}}}");
                primaryKeyFields.Append($"{(primaryKeyFields.Length > 0 ? ", " : "")}{fieldName}");
                primaryKeyProperties.Add(property);
            }

            s_attributes.Add(property, [..property.CustomAttributes.Select(attributeData => attributeData.AttributeType)]);
        }

        // Have to assume all fields are primary when none are specified
        if (primaryKeyProperties.Count == 0)
        {
            foreach (PropertyInfo property in s_properties.Values)
            {
                string fieldName = s_fieldNames[property.Name];

                if (s_escapedFieldNameTargets?.ContainsKey(fieldName) ?? false)
                    fieldName = $"\"{fieldName}\"";

                whereFormat.Append($"{(whereFormat.Length > 0 ? " AND " : "")}{fieldName}={{{primaryKeyIndex++}}}");
                primaryKeyFields.Append($"{(primaryKeyFields.Length > 0 ? ", " : "")}{fieldName}");
                primaryKeyProperties.Add(property);
            }

            s_primaryKeyFields = primaryKeyFields.ToString();

            // Default to all
            primaryKeyFields.Clear();
            primaryKeyFields.Append('*');
        }
        else
        {
            s_primaryKeyFields = primaryKeyFields.ToString();
        }

        List<object> updateWhereOffsets = [];

        for (int i = 0; i < primaryKeyIndex; i++)
            updateWhereOffsets.Add($"{{{updateFieldIndex + i}}}");

        // If any database has been targeted for escaping the table name, pre-apply the standard ANSI escaped
        // table name in the static SQL expressions. This will provide a unique replaceable identifier should
        // the common database delimiters, or no delimiters, be applicable for an active database connection
        string tableName = s_tableName;

        if (s_escapedTableNameTargets is not null)
            tableName = $"\"{tableName}\"";

        if (s_expressionAmendments is not null)
        {
            // Add tokens to primary expressions for easy replacement
            tableName = $"{TableNamePrefixToken}{tableName}{TableNameSuffixToken}";
            allFields.Insert(0, FieldListPrefixToken);
            allFields.Append(FieldListSuffixToken);
            primaryKeyFields.Insert(0, FieldListPrefixToken);
            primaryKeyFields.Append(FieldListSuffixToken);
            addNewFields.Insert(0, FieldListPrefixToken);
            addNewFields.Append(FieldListSuffixToken);
            updateFormat.Insert(0, FieldListPrefixToken);
            updateFormat.Append(FieldListSuffixToken);
        }

        s_selectCountSql = string.Format(SelectCountSqlFormat, tableName);
        s_selectSetSql = string.Format(SelectSetSqlFormat, allFields, tableName);
        s_selectSetWhereSql = string.Format(SelectSetWhereSqlFormat, allFields, tableName);
        s_selectKeysSql = string.Format(SelectSetSqlFormat, primaryKeyFields, tableName);
        s_selectKeysWhereSql = string.Format(SelectSetWhereSqlFormat, primaryKeyFields, tableName);
        s_selectRowSql = string.Format(SelectRowSqlFormat, tableName, whereFormat);
        s_addNewSql = string.Format(AddNewSqlFormat, tableName, addNewFields, addNewFormat);
        s_updateSql = string.Format(UpdateSqlFormat, tableName, updateFormat, string.Format(whereFormat.ToString(), updateWhereOffsets.ToArray()));
        s_deleteSql = string.Format(DeleteSqlFormat, tableName, whereFormat);
        s_updateWhereSql = s_updateSql[..(s_updateSql.IndexOf(" WHERE ", StringComparison.Ordinal) + 7)];
        s_deleteWhereSql = s_deleteSql[..(s_deleteSql.IndexOf(" WHERE ", StringComparison.Ordinal) + 7)];

        s_addNewProperties = addNewProperties.ToArray();
        s_updateProperties = updateProperties.ToArray();
        s_primaryKeyProperties = primaryKeyProperties.ToArray();

        // Create an instance of modeled table to allow any static functionality to be initialized,
        // such as registering any custom types or symbols that may be useful for value expressions
        ValueExpressionParser<T>.InitializeType();

        // Generate compiled "create new" and "update" record functions for modeled table
        s_createRecordInstance = ValueExpressionParser<T>.CreateInstance<CurrentScope>(s_properties.Values, s_typeRegistry);
        s_updateRecordInstance = ValueExpressionParser<T>.UpdateInstance<CurrentScope>(s_properties.Values, s_typeRegistry);
        s_applyRecordDefaults = ValueExpressionParser<T>.ApplyDefaults<CurrentScope>(s_properties.Values, s_typeRegistry);

        // Generate a data table to be used for schema operations
        s_tableSchema = new DataTable(s_tableName);

        foreach (PropertyInfo property in s_properties.Values)
        {
            string fieldName = s_fieldNames[property.Name];
            Type propertyType = property.PropertyType;

            bool isNullable = false;
            Type columnType = propertyType;

            // Check if the property is a nullable value type
            if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                isNullable = true;
                columnType = Nullable.GetUnderlyingType(propertyType)!; // e.g., int for int?
            }
            else if (!propertyType.IsValueType)
            {
                // Reference types are nullable by default
                isNullable = true;
            }

            // Create the DataColumn with the non-nullable type
            DataColumn column = new(fieldName, columnType) { AllowDBNull = isNullable };

            s_tableSchema.Columns.Add(column);
        }

        // Create hash set of valid field names, escaped and unescaped
        s_validFieldNames = new HashSet<string>(s_fieldNames.Values, StringComparer.OrdinalIgnoreCase);
        s_validFieldNames.UnionWith(s_fieldNames.Values.Select(GetUnescapedFieldName));

        // Create hash set of searchable fields
        s_searchableFields = new HashSet<string>(typeof(T).GetCustomAttributes<SearchableAttribute>().SelectMany(attr => attr.FieldNames), StringComparer.OrdinalIgnoreCase);

        // Get all public static methods for the modeled table
        MethodInfo[] staticMethods = typeof(T).GetMethods(BindingFlags.Public | BindingFlags.Static);

        // Resolve extensions methods for the modeled table
        s_searchExtensions = ResolveExtensionAttributes<SearchExtensionAttribute, IRecordFilter, RecordRestriction>(staticMethods);
        s_sortExtensions = ResolveExtensionAttributes<SortExtensionAttribute, string, string>(staticMethods);
    }

    internal static bool IsSearchableField(string fieldName)
    {
        return s_searchableFields.Contains(fieldName);
    }

    private static (Regex, Func<TInput, TReturn>)[] ResolveExtensionAttributes<TAttribute, TInput, TReturn>(MethodInfo[] staticMethods) where TAttribute : ExtensionAttributeBase
    {
        List<(Regex, Func<TInput, TReturn>)> extensions = [];

        foreach (MethodInfo method in staticMethods)
        {
            TAttribute? attribute = method.GetCustomAttribute<TAttribute>();

            if (attribute is null)
                continue;

            // Validate method signature
            ParameterInfo[] parameters = method.GetParameters();

            if (method.ReturnType != typeof(TReturn) || parameters.Length != 1 || parameters[0].ParameterType != typeof(TInput))
            {
                throw new InvalidOperationException(
                    $"Method \"{method.Name}\" marked with \"{typeof(TAttribute).Name}\" in model \"{typeof(T).Name}\" has an invalid signature. " +
                    $"Expected: public static {typeof(TReturn).Name} Method({typeof(TInput)} input)");
            }

            // Pre-compile the regex pattern
            Regex regex = new(attribute.FieldMatch, RegexOptions.Compiled);

            // Create and cache the delegate
            Func<TInput, TReturn> staticExtensionMethod = (Func<TInput, TReturn>)Delegate.CreateDelegate(typeof(Func<TInput, TReturn>), method);
            extensions.Add((regex, staticExtensionMethod));
        }

        return extensions.ToArray();
    }

    internal static Func<IRecordFilter, RecordRestriction>? GetSearchExtensionMethod(string fieldName)
    {
        foreach ((Regex fieldMatchExpression, Func<IRecordFilter, RecordRestriction> searchExtensionMethod) in s_searchExtensions)
        {
            if (fieldMatchExpression.IsMatch(fieldName))
                return searchExtensionMethod;
        }

        return null;
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

    // Static Methods

    /// <summary>
    /// Gets a delegate for the <see cref="LoadRecord(DataRow)"/> function for specified type <typeparamref name="T"/>.
    /// </summary>
    /// <returns>Delegate for the <see cref="LoadRecord(DataRow)"/> function.</returns>
    /// <remarks>
    /// This method is useful to deserialize a <see cref="DataRow"/> into a type <typeparamref name="T"/> instance
    /// when no data connection is available, e.g., when using a deserialized <see cref="DataSet"/>.
    /// </remarks>
    public static Func<DataRow, T?> LoadRecordFunction()
    {
        using AdoDataConnection connection = new(null!, typeof(NullConnection));
        return new TableOperations<T>(connection).LoadRecord;
    }

    /// <summary>
    /// Gets a delegate for the <see cref="NewRecord"/> function for specified type <typeparamref name="T"/>.
    /// </summary>
    /// <returns>Delegate for the <see cref="NewRecord"/> function.</returns>
    /// <remarks>
    /// This method is useful to create a new type <typeparamref name="T"/> instance when no data connection
    /// is available, applying any modeled default values as specified by a <see cref="DefaultValueAttribute"/>
    /// or <see cref="DefaultValueExpressionAttribute"/> on the model properties.
    /// </remarks>
    public static Func<T?> NewRecordFunction()
    {
        using AdoDataConnection connection = new(null!, typeof(NullConnection));
        return new TableOperations<T>(connection).NewRecord;
    }

    /// <summary>
    /// Gets a delegate for the <see cref="ApplyRecordDefaults"/> method for specified type <typeparamref name="T"/>.
    /// </summary>
    /// <returns>Delegate for the <see cref="ApplyRecordDefaults"/> method.</returns>
    /// <remarks>
    /// This method is useful to apply defaults values to an existing type <typeparamref name="T"/> instance when no data
    /// connection is available, applying any modeled default values as specified by a <see cref="DefaultValueAttribute"/>
    /// or <see cref="DefaultValueExpressionAttribute"/> on the model properties.
    /// </remarks>
    public static Action<T> ApplyRecordDefaultsFunction()
    {
        using AdoDataConnection connection = new(null!, typeof(NullConnection));
        return new TableOperations<T>(connection).ApplyRecordDefaults;
    }

    /// <summary>
    /// Gets a delegate for the <see cref="ApplyRecordUpdates"/> method for specified type <typeparamref name="T"/>.
    /// </summary>
    /// <returns>Delegate for the <see cref="ApplyRecordUpdates"/> method.</returns>
    /// <remarks>
    /// This method is useful to apply update values to an existing type <typeparamref name="T"/> instance when no data
    /// connection is available, applying any modeled update values as specified by instances of the
    /// <see cref="UpdateValueExpressionAttribute"/> on the model properties.
    /// </remarks>
    public static Action<T> ApplyRecordUpdatesFunction()
    {
        using AdoDataConnection connection = new(null!, typeof(NullConnection));
        return new TableOperations<T>(connection).ApplyRecordUpdates;
    }

    private static string GetFieldName(PropertyInfo property)
    {
        if (property.TryGetAttribute(out FieldNameAttribute? fieldNameAttribute) && !string.IsNullOrEmpty(fieldNameAttribute.FieldName))
            return fieldNameAttribute.FieldName;

        return property.Name;
    }

    private static Dictionary<DatabaseType, DbType>? DeriveFieldDataTypeTargets(FieldDataTypeAttribute[]? fieldDataTypeAttributes)
    {
        if (fieldDataTypeAttributes is null || fieldDataTypeAttributes.Length == 0)
            return null;

        DatabaseType[] databaseTypes;
        DbType defaultFieldDataType;

        // If any attribute has no database target type specified, then all database types are assumed
        if (fieldDataTypeAttributes.Any(attribute => attribute.TargetDatabaseType is null))
        {
            databaseTypes = Enum.GetValues(typeof(DatabaseType)).Cast<DatabaseType>().ToArray();
            defaultFieldDataType = fieldDataTypeAttributes.First(attribute => attribute.TargetDatabaseType is null).FieldDataType;
        }
        else
        {
            databaseTypes = fieldDataTypeAttributes.Select(attribute => attribute.TargetDatabaseType.GetValueOrDefault()).Distinct().ToArray();
            defaultFieldDataType = DbType.String;
        }

        Dictionary<DatabaseType, DbType> fieldDataTypes = new(databaseTypes.Length);

        foreach (DatabaseType databaseType in databaseTypes)
        {
            FieldDataTypeAttribute? fieldDataTypeAttribute = fieldDataTypeAttributes.FirstOrDefault(attribute => attribute.TargetDatabaseType == databaseType);
            fieldDataTypes[databaseType] = fieldDataTypeAttribute?.FieldDataType ?? defaultFieldDataType;
        }

        return fieldDataTypes;
    }

    private static Dictionary<DatabaseType, bool>? DeriveEscapedNameTargets(UseEscapedNameAttribute[]? useEscapedNameAttributes)
    {
        if (useEscapedNameAttributes is null || useEscapedNameAttributes.Length == 0)
            return null;

        DatabaseType[] databaseTypes;
        bool allDatabasesTargeted = false;

        // If any attribute has no database target type specified, then all database types are assumed
        if (useEscapedNameAttributes.Any(attribute => attribute.TargetDatabaseType is null))
        {
            allDatabasesTargeted = true;
            databaseTypes = Enum.GetValues(typeof(DatabaseType)).Cast<DatabaseType>().ToArray();
        }
        else
        {
            databaseTypes = useEscapedNameAttributes.Select(attribute => attribute.TargetDatabaseType.GetValueOrDefault()).Distinct().ToArray();
        }

        Dictionary<DatabaseType, bool> escapedNameTargets = new(databaseTypes.Length);

        foreach (DatabaseType databaseType in databaseTypes)
        {
            UseEscapedNameAttribute? useEscapedNameAttribute = useEscapedNameAttributes.FirstOrDefault(attribute => attribute.TargetDatabaseType == databaseType);
            bool useAnsiQuotes = useEscapedNameAttribute is { UseAnsiQuotes: true } || allDatabasesTargeted && databaseType != DatabaseType.MySQL;
            escapedNameTargets[databaseType] = useAnsiQuotes;
        }

        return escapedNameTargets;
    }

    private static List<(DatabaseType, TargetExpression, StatementTypes, AffixPosition, string)>? DeriveExpressionAmendments(AmendExpressionAttribute?[]? amendExpressionAttributes)
    {
        if (amendExpressionAttributes is null || amendExpressionAttributes.Length == 0)
            return null;

        List<(DatabaseType, TargetExpression, StatementTypes, AffixPosition, string)> typedExpressionAmendments = [];
        List<(DatabaseType, TargetExpression, StatementTypes, AffixPosition, string)> untypedExpressionAmendments = [];
        List<(DatabaseType, TargetExpression, StatementTypes, AffixPosition, string)> expressionAmendments;

        foreach (AmendExpressionAttribute? attribute in amendExpressionAttributes)
        {
            if (attribute is null)
                continue;

            DatabaseType[] databaseTypes;

            // If any attribute has no database target type specified, then all database types are assumed
            if (attribute.TargetDatabaseType is null)
            {
                databaseTypes = Enum.GetValues(typeof(DatabaseType)).Cast<DatabaseType>().ToArray();
                expressionAmendments = untypedExpressionAmendments;
            }
            else
            {
                databaseTypes = [attribute.TargetDatabaseType.Value];
                expressionAmendments = typedExpressionAmendments;
            }

            foreach (DatabaseType databaseType in databaseTypes)
            {
                string amendmentText = attribute.AmendmentText.Trim();
                amendmentText = attribute.AffixPosition == AffixPosition.Prefix ? $"{amendmentText} " : $" {amendmentText}";
                expressionAmendments.Add((databaseType, attribute.TargetExpression, attribute.StatementTypes, attribute.AffixPosition, amendmentText));
            }
        }

        // Sort expression amendments with a specified database type higher in the execution order to allow for database specific overrides
        expressionAmendments = [..typedExpressionAmendments];
        expressionAmendments.AddRange(untypedExpressionAmendments);

        return expressionAmendments.Count > 0 ? expressionAmendments : null; //-V3022
    }

    private static string ValueList(IReadOnlyList<object?>? values)
    {
        if (values is null)
            return string.Empty;

        StringBuilder delimitedString = new();

        for (int i = 0; i < values.Count; i++)
        {
            if (delimitedString.Length > 0)
                delimitedString.Append(", ");

            delimitedString.Append($"{i}:{values[i]?.ToString() ?? "null"}");
        }

        return delimitedString.ToString();
    }

    #endregion
}
