//******************************************************************************************************
//  DataExtensions.cs - Gbtc
//
//  Copyright © 2012, Grid Protection Alliance.  All Rights Reserved.
//
//  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements. See
//  the NOTICE file distributed with this work for additional information regarding copyright ownership.
//  The GPA licenses this file to you under the MIT License (MIT), the "License"; you may
//  not use this file except in compliance with the License. You may obtain a copy of the License at:
//
//      http://www.opensource.org/licenses/MIT
//
//  Unless agreed to in writing, the subject software distributed under the License is distributed on an
//  "AS-IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. Refer to the
//  License for the specific language governing permissions and limitations.
//
//  Code Modification History:
//  ----------------------------------------------------------------------------------------------------
//  02/05/2003 - J. Ritchie Carroll
//       Generated original version of source code.
//  05/25/2004 - J. Ritchie Carroll
//       Added "with parameters" overloads to all basic query functions.
//  12/10/2004 - Tim M Shults
//       Added several new WithParameters overloads that allow a programmer to send just the
//       parameter values instead of creating a series of parameter objects and then sending
//       them through. Easy way to cut down on the amount of code.
//       This code is just for calls to Stored Procedures and will not work for in-line SQL.
//  03/28/2006 - Pinal C. Patel
//       Migrated 2.0 version of source code from 1.1 source (GSF.Database.Common).
//  08/21/2007 - Darrell Zuercher
//       Edited code comments.
//  09/15/2008 - J. Ritchie Carroll
//       Converted to C# extensions.
//  09/29/2008 - Pinal C. Patel
//       Reviewed code comments.
//  09/09/2009 - J. Ritchie Carroll
//       Added extensions for ODBC providers.
//  09/14/2009 - Stephen C. Wills
//       Added new header and license agreement.
//  12/02/2009 - Stephen C. Wills
//       Added disposal of database command objects.
//  09/28/2010 - J. Ritchie Carroll
//       Added Stephen's CreateParameterizedCommand connection extension.
//  04/07/2011 - J. Ritchie Carroll
//       Added Mehul's AddParameterWithValue command extension. Added overloads for all
//       PopulateParameters() command extensions so that they could take a "params" style
//       array of values after initial value for ease-of-use. Added "params" style array
//       to all templated IDbConnection that will use the CreateParameterizedCommand
//       connection extension with optional parameters.
//  06/16/2011 - Pinal C. Patel
//       Modified AddParameterWithValue() to be backwards compatible.
//  07/18/2011 - Stephen C. Wills
//       Added DataRow extension functions to automatically convert from types that
//       implement the IConvertible interface.
//  08/12/2011 - Pinal C. Patel
//       Modified AddParameterWithValue() to correctly implement backwards compatible.
//  09/19/2011 - Stephen C. Wills
//       Modified AddParametersWithValues() to parse parameters prefixed
//       with a colon for Oracle database compatibility.
//  09/21/2011 - J. Ritchie Carroll
//       Added Mono implementation exception regions.
//  12/14/2012 - Starlynn Danyelle Gilliam
//       Modified Header.
//  12/13/2019 - J. Ritchie Carroll
//      Migrated to Gemstone libraries.
//  08/01/2024 - J. Ritchie Carroll
//      Migrated code to use System.Data.Common abstract classes instead of IDb interfaces
//      which includes async support.
//
//******************************************************************************************************
// ReSharper disable InconsistentNaming
// ReSharper disable UnusedVariable

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Gemstone.Collections.IAsyncEnumerableExtensions;
using Microsoft.Data.SqlClient;

namespace Gemstone.Data.DataExtensions;

/// <summary>
/// Defines extension functions related to database and SQL interaction.
/// </summary>
public static class DataExtensions
{
    /// <summary>
    /// The default timeout duration used for executing SQL statements when timeout duration is not specified.
    /// </summary>
    public const int DefaultTimeoutDuration = 30;

    // Defines a list of keywords used to identify PL/SQL blocks.
    private static readonly string[] s_plsqlIdentifiers = ["CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", "CREATE PROCEDURE", "CREATE OR REPLACE PROCEDURE", "CREATE PACKAGE", "CREATE OR REPLACE PACKAGE", "DECLARE", "BEGIN"];

    private static readonly Regex s_sqlParameterRegex = new(@"^[:@][a-zA-Z]\w*$", RegexOptions.Compiled);
    private static readonly Regex s_sqlCommentRegex = new(@"/\*.*\*/|--.*(?=\n)", RegexOptions.Multiline);
    private static readonly Regex s_sqlIdentifierRegex = new(@"^(\S+|(\S+|\[.+\])(\.\S+|\.\[.+\])*|(\S+|\`.+\`)(\.\S+|\.\`.+\`)*|(\S+|\"".+\"")(\.\S+|\.\"".+\"")*)$", RegexOptions.Compiled | RegexOptions.Singleline | RegexOptions.ExplicitCapture);

    #region [ SQL Encoding String Extension ]

    /// <summary>
    /// Performs SQL encoding on given SQL string.
    /// </summary>
    /// <param name="sql">The string on which SQL encoding is to be performed.</param>
    /// <param name="databaseType">Database type for the SQL encoding.</param>
    /// <returns>The SQL encoded string.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string SQLEncode(this string sql, DatabaseType databaseType = DatabaseType.Other)
    {
        if (databaseType == DatabaseType.MySQL)
            return sql.Replace("\\", "\\\\").Replace("\'", "\\\'");

        return sql.Replace("\'", "\'\'"); //.Replace("/*", "").Replace("--", "");
    }

    #endregion

    #region [ ExecuteNonQuery Overloaded Extension ]

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and returns the number of rows affected.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>The number of rows affected.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ExecuteNonQuery(this DbConnection connection, string sql, params object[] parameters)
    {
        return connection.ExecuteNonQuery(DefaultTimeoutDuration, sql, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and returns the number of rows affected.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>The number of rows affected.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ExecuteNonQuery(this DbConnection connection, int timeout, string sql, params object[] parameters)
    {
        using DbCommand command = connection.CreateParameterizedCommand(sql, parameters);

        command.CommandTimeout = timeout;

        return command.ExecuteNonQuery();
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and returns the number of rows affected.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>The number of rows affected.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<int> ExecuteNonQueryAsync(this DbConnection connection, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        return connection.ExecuteNonQueryAsync(DefaultTimeoutDuration, sql, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and returns the number of rows affected.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>The number of rows affected.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static async Task<int> ExecuteNonQueryAsync(this DbConnection connection, int timeout, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        await using DbCommand command = connection.CreateParameterizedCommand(sql, parameters);

        command.CommandTimeout = timeout;

        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and returns the number of rows affected.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>The number of rows affected.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ExecuteNonQuery(this DbCommand command, string sql, params object[] parameters)
    {
        return command.ExecuteNonQuery(DefaultTimeoutDuration, sql, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and returns the number of rows affected.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>The number of rows affected.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ExecuteNonQuery(this DbCommand command, int timeout, string sql, params object[] parameters)
    {
        command.CommandTimeout = timeout;
        command.Parameters.Clear();
        command.AddParametersWithValues(sql, parameters);

        return command.ExecuteNonQuery();
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and returns the number of rows affected.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>The number of rows affected.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<int> ExecuteNonQueryAsync(this DbCommand command, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        return command.ExecuteNonQueryAsync(DefaultTimeoutDuration, sql, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and returns the number of rows affected.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>The number of rows affected.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<int> ExecuteNonQueryAsync(this DbCommand command, int timeout, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        command.CommandTimeout = timeout;
        command.Parameters.Clear();
        command.AddParametersWithValues(sql, parameters);

        return command.ExecuteNonQueryAsync(cancellationToken);
    }

    #endregion

    #region [ ExecuteReader Overloaded Extensions ]

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and builds a <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DbDataReader"/> object and its associated command.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (DbDataReader, DbCommand) ExecuteReader(this DbConnection connection, string sql, params object[] parameters)
    {
        return connection.ExecuteReader(DefaultTimeoutDuration, sql, CommandBehavior.Default, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and builds a <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DbDataReader"/> object and its associated command.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (DbDataReader, DbCommand) ExecuteReader(this DbConnection connection, int timeout, string sql, params object[] parameters)
    {
        return connection.ExecuteReader(timeout, sql, CommandBehavior.Default, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and builds a <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="behavior">One of the <see cref="CommandBehavior"/> values.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DbDataReader"/> object and its associated command.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (DbDataReader, DbCommand) ExecuteReader(this DbConnection connection, int timeout, string sql, CommandBehavior behavior, params object[] parameters)
    {
        DbCommand? command = null;
        DbDataReader? reader = null;

        try
        {
            command = connection.CreateParameterizedCommand(sql, parameters);
            command.CommandTimeout = timeout;
            return (command.ExecuteReader(behavior), command);
        }
        catch
        {
            reader?.Dispose();
            command?.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and builds a <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DbDataReader"/> object and its associated command.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (Task<DbDataReader>, DbCommand) ExecuteReaderAsync(this DbConnection connection, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        return connection.ExecuteReaderAsync(DefaultTimeoutDuration, sql, CommandBehavior.Default, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and builds a <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DbDataReader"/> object and its associated command.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (Task<DbDataReader>, DbCommand) ExecuteReaderAsync(this DbConnection connection, int timeout, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        return connection.ExecuteReaderAsync(timeout, sql, CommandBehavior.Default, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and builds a <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="behavior">One of the <see cref="CommandBehavior"/> values.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DbDataReader"/> object and its associated command.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (Task<DbDataReader>, DbCommand) ExecuteReaderAsync(this DbConnection connection, int timeout, string sql, CommandBehavior behavior, CancellationToken cancellationToken, params object[] parameters)
    {
        DbCommand? command = null;
        DbDataReader? reader = null;

        try
        {
            command = connection.CreateParameterizedCommand(sql, parameters);
            command.CommandTimeout = timeout;
            return (command.ExecuteReaderAsync(behavior, cancellationToken), command);
        }
        catch
        {
            reader?.Dispose();
            command?.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and builds a <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DbDataReader"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DbDataReader ExecuteReader(this DbCommand command, string sql, params object[] parameters)
    {
        return command.ExecuteReader(DefaultTimeoutDuration, sql, CommandBehavior.Default, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and builds a <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DbDataReader"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DbDataReader ExecuteReader(this DbCommand command, int timeout, string sql, params object[] parameters)
    {
        return command.ExecuteReader(timeout, sql, CommandBehavior.Default, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and builds a <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="behavior">One of the <see cref="CommandBehavior"/> values.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DbDataReader"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DbDataReader ExecuteReader(this DbCommand command, int timeout, string sql, CommandBehavior behavior, params object[] parameters)
    {
        command.CommandTimeout = timeout;
        command.Parameters.Clear();
        command.AddParametersWithValues(sql, parameters);

        return command.ExecuteReader(behavior);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and builds a <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DbDataReader"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<DbDataReader> ExecuteReaderAsync(this DbCommand command, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        return command.ExecuteReaderAsync(DefaultTimeoutDuration, sql, CommandBehavior.Default, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and builds a <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DbDataReader"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<DbDataReader> ExecuteReaderAsync(this DbCommand command, int timeout, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        return command.ExecuteReaderAsync(timeout, sql, CommandBehavior.Default, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and builds a <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="behavior">One of the <see cref="CommandBehavior"/> values.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DbDataReader"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<DbDataReader> ExecuteReaderAsync(this DbCommand command, int timeout, string sql, CommandBehavior behavior, CancellationToken cancellationToken, params object[] parameters)
    {
        command.CommandTimeout = timeout;
        command.Parameters.Clear();
        command.AddParametersWithValues(sql, parameters);

        return command.ExecuteReaderAsync(behavior, cancellationToken);
    }

    #endregion

    #region [ ExecuteScalar Overloaded Extensions ]

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and returns the value in the first column 
    /// of the first row in the result set.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static object? ExecuteScalar(this DbConnection connection, string sql, params object[] parameters)
    {
        return connection.ExecuteScalar(DefaultTimeoutDuration, sql, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and returns the value in the first column 
    /// of the first row in the result set.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static object? ExecuteScalar(this DbConnection connection, int timeout, string sql, params object[] parameters)
    {
        using DbCommand command = connection.CreateParameterizedCommand(sql, parameters);

        command.CommandTimeout = timeout;

        return command.ExecuteScalar();
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and returns the value in the first column 
    /// of the first row in the result set.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<object?> ExecuteScalarAsync(this DbConnection connection, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        return connection.ExecuteScalarAsync(DefaultTimeoutDuration, sql, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and returns the value in the first column 
    /// of the first row in the result set.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static async Task<object?> ExecuteScalarAsync(this DbConnection connection, int timeout, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        await using DbCommand command = connection.CreateParameterizedCommand(sql, parameters);

        command.CommandTimeout = timeout;

        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and returns the value in the first column 
    /// of the first row in the result set.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static object? ExecuteScalar(this DbCommand command, string sql, params object[] parameters)
    {
        return command.ExecuteScalar(DefaultTimeoutDuration, sql, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and returns the value in the first column 
    /// of the first row in the result set.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static object? ExecuteScalar(this DbCommand command, int timeout, string sql, params object[] parameters)
    {
        command.CommandTimeout = timeout;
        command.Parameters.Clear();
        command.AddParametersWithValues(sql, parameters);

        return command.ExecuteScalar();
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and returns the value in the first column 
    /// of the first row in the result set.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<object?> ExecuteScalarAsync(this DbCommand command, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        return command.ExecuteScalarAsync(DefaultTimeoutDuration, sql, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and returns the value in the first column 
    /// of the first row in the result set.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<object?> ExecuteScalarAsync(this DbCommand command, int timeout, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        command.CommandTimeout = timeout;
        command.Parameters.Clear();
        command.AddParametersWithValues(sql, parameters);

        return command.ExecuteScalarAsync(cancellationToken);
    }

    #endregion

    #region [ ExecuteScript Overloaded Extensions ]

    /// <summary>
    /// Executes the statements defined in the given TSQL script.
    /// </summary>
    /// <param name="connection">The connection used to execute SQL statements.</param>
    /// <param name="scriptPath">The path to the SQL script.</param>
    public static void ExecuteTSQLScript(this DbConnection connection, string scriptPath)
    {
        using TextReader scriptReader = File.OpenText(scriptPath);

        ExecuteTSQLScript(connection, scriptReader);
    }

    /// <summary>
    /// Executes the statements defined in the given TSQL script.
    /// </summary>
    /// <param name="connection">The connection used to execute SQL statements.</param>
    /// <param name="scriptReader">The reader used to extract statements from the SQL script.</param>
    public static void ExecuteTSQLScript(this DbConnection connection, TextReader scriptReader)
    {
        string? line = scriptReader.ReadLine();

        using DbCommand command = connection.CreateCommand();

        StringBuilder statementBuilder = new();

        while (line is not null)
        {
            string trimLine = line.Trim();

            if (trimLine == "GO")
            {
                // Remove comments and execute the statement.
                string statement = statementBuilder.ToString();
                command.CommandText = s_sqlCommentRegex.Replace(statement, " ").Trim();
                command.ExecuteNonQuery();
                statementBuilder.Clear();
            }
            else
            {
                // Append this line to the statement
                statementBuilder.Append(line);
                statementBuilder.Append('\n');
            }

            // Read the next line from the file.
            line = scriptReader.ReadLine();
        }
    }

    /// <summary>
    /// Executes the statements defined in the given MySQL script.
    /// </summary>
    /// <param name="connection">The connection used to execute SQL statements.</param>
    /// <param name="scriptPath">The path to the SQL script.</param>
    public static void ExecuteMySQLScript(this DbConnection connection, string scriptPath)
    {
        using TextReader scriptReader = File.OpenText(scriptPath);

        ExecuteMySQLScript(connection, scriptReader);
    }

    /// <summary>
    /// Executes the statements defined in the given MySQL script.
    /// </summary>
    /// <param name="connection">The connection used to execute SQL statements.</param>
    /// <param name="scriptReader">The reader used to extract statements from the SQL script.</param>
    public static void ExecuteMySQLScript(this DbConnection connection, TextReader scriptReader)
    {
        string? line = scriptReader.ReadLine();
        string delimiter = ";";

        using DbCommand command = connection.CreateCommand();

        StringBuilder statementBuilder = new();

        while (line is not null)
        {
            if (line.StartsWith("DELIMITER ", StringComparison.OrdinalIgnoreCase))
            {
                delimiter = line.Split(' ')[1].Trim();
            }
            else
            {
                statementBuilder.Append(line);
                statementBuilder.Append('\n');

                string statement = statementBuilder.ToString();
                statement = s_sqlCommentRegex.Replace(statement, " ").Trim();

                if (statement.EndsWith(delimiter, StringComparison.Ordinal))
                {
                    // Remove trailing delimiter.
                    statement = statement.Remove(statement.Length - delimiter.Length);

                    // Remove comments and execute the statement.
                    command.CommandText = statement;
                    command.ExecuteNonQuery();
                    statementBuilder.Clear();
                }
            }

            // Read the next line from the file.
            line = scriptReader.ReadLine();
        }
    }

    /// <summary>
    /// Executes the statements defined in the given Oracle database script.
    /// </summary>
    /// <param name="connection">The connection used to execute SQL statements.</param>
    /// <param name="scriptPath">The path to the SQL script.</param>
    public static void ExecuteOracleScript(this DbConnection connection, string scriptPath)
    {
        using TextReader scriptReader = File.OpenText(scriptPath);

        ExecuteOracleScript(connection, scriptReader);
    }

    /// <summary>
    /// Executes the statements defined in the given Oracle database script.
    /// </summary>
    /// <param name="connection">The connection used to execute SQL statements.</param>
    /// <param name="scriptReader">The reader used to extract statements from the SQL script.</param>
    public static void ExecuteOracleScript(this DbConnection connection, TextReader scriptReader)
    {
        string? line = scriptReader.ReadLine();

        using DbCommand command = connection.CreateCommand();

        StringBuilder statementBuilder = new();

        while (line is not null)
        {
            string trimLine = line.Trim();

            statementBuilder.Append(line);
            statementBuilder.Append('\n');

            string statement = statementBuilder.ToString();
            statement = s_sqlCommentRegex.Replace(statement, " ").Trim();

            // Determine whether the statement is a PL/SQL block.
            // If the statement is a PL/SQL block, the delimiter
            // is a forward slash. Otherwise, it is a semicolon.
            bool isPlsqlBlock = s_plsqlIdentifiers.Any(ident => statement.IndexOf(ident, StringComparison.CurrentCultureIgnoreCase) >= 0);

            // If the statement is a PL/SQL block and the current line is a forward slash,
            // or if the statement is not a PL/SQL block and the statement in a semicolon,
            // then execute and flush the statement so that the next statement can be executed.
            if (isPlsqlBlock && trimLine == "/" || !isPlsqlBlock && statement.EndsWith(";", StringComparison.Ordinal))
            {
                // Remove trailing delimiter and newlines.
                statement = statement.Remove(statement.Length - 1);

                // Remove comments and execute the statement.
                command.CommandText = statement;
                command.ExecuteNonQuery();
                statementBuilder.Clear();
            }

            // Read the next line from the file.
            line = scriptReader.ReadLine();
        }
    }

    #endregion

    #region [ RetrieveRow Overloaded Extensions ]

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and returns the first <see cref="DataRow"/> in the result set.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>The first <see cref="DataRow"/> in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataRow RetrieveRow(this DbConnection connection, string sql, params object[] parameters)
    {
        return connection.RetrieveRow(DefaultTimeoutDuration, sql, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and returns the first <see cref="DataRow"/> in the result set.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>The first <see cref="DataRow"/> in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataRow RetrieveRow(this DbConnection connection, int timeout, string sql, params object[] parameters)
    {
        DataTable dataTable = connection.RetrieveData(timeout, sql, parameters);

        if (dataTable.Rows.Count == 0)
            dataTable.Rows.Add(dataTable.NewRow());

        return dataTable.Rows[0];
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and returns the first <see cref="DataRow"/> in the result set.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>The first <see cref="DataRow"/> in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<DataRow> RetrieveRowAsync(this DbConnection connection, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        return connection.RetrieveRowAsync(DefaultTimeoutDuration, sql, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and returns the first <see cref="DataRow"/> in the result set.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>The first <see cref="DataRow"/> in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static async Task<DataRow> RetrieveRowAsync(this DbConnection connection, int timeout, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        DataTable dataTable = await connection.RetrieveDataAsync(timeout, sql, cancellationToken, parameters).ConfigureAwait(false);

        if (dataTable.Rows.Count == 0)
            dataTable.Rows.Add(dataTable.NewRow());

        return dataTable.Rows[0];
    }

    /// <summary>
    /// Tries to retrieve the first <see cref="DataRow"/> in the result set of the SQL statement using <see cref="DbConnection"/>.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="row">The first <see cref="DataRow"/> in the result set, or <c>null</c>.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>The first <see cref="DataRow"/> in the result set.</returns>
    public static bool TryRetrieveRow(this DbConnection connection, string sql, out DataRow? row, params object[] parameters)
    {
        return connection.TryRetrieveRow(sql, DefaultTimeoutDuration, out row, parameters);
    }

    /// <summary>
    /// Tries to retrieve the first <see cref="DataRow"/> in the result set of the SQL statement using <see cref="DbConnection"/>.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="row">The first <see cref="DataRow"/> in the result set, or <c>null</c>.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>The first <see cref="DataRow"/> in the result set.</returns>
    public static bool TryRetrieveRow(this DbConnection connection, string sql, int timeout, out DataRow? row, params object[] parameters)
    {
        DataTable dataTable = connection.RetrieveData(sql, timeout, parameters);

        if (dataTable.Rows.Count == 0)
        {
            row = null;
            return false;
        }

        row = dataTable.Rows[0];
        return true;
    }

    /// <summary>
    /// Tries to retrieve the first <see cref="DataRow"/> in the result set of the SQL statement using <see cref="DbConnection"/>.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>Tuple of first <see cref="DataRow"/> in the result set (can be <c>null</c>) and a flag that determines if retrieve was successful.</returns>
    public static Task<(DataRow? row, bool)> TryRetrieveRowAsync(this DbConnection connection, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        return connection.TryRetrieveRowAsync(sql, DefaultTimeoutDuration, cancellationToken, parameters);
    }

    /// <summary>
    /// Tries to retrieve the first <see cref="DataRow"/> in the result set of the SQL statement using <see cref="DbConnection"/>.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>Tuple of first <see cref="DataRow"/> in the result set (can be <c>null</c>) and a flag that determines if retrieve was successful.</returns>
    public static async Task<(DataRow? row, bool)> TryRetrieveRowAsync(this DbConnection connection, string sql, int timeout, CancellationToken cancellationToken, params object[] parameters)
    {
        DataTable dataTable = await connection.RetrieveDataAsync(timeout, sql, cancellationToken, parameters).ConfigureAwait(false);
        return dataTable.Rows.Count == 0 ? (null, false) : (dataTable.Rows[0], true);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and returns the first <see cref="DataRow"/> in the result set.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>The first <see cref="DataRow"/> in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataRow RetrieveRow(this DbCommand command, string sql, params object[] parameters)
    {
        return command.RetrieveRow(DefaultTimeoutDuration, sql, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and returns the first <see cref="DataRow"/> in the result set.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>The first <see cref="DataRow"/> in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataRow RetrieveRow(this DbCommand command, int timeout, string sql, params object[] parameters)
    {
        DataTable dataTable = command.RetrieveData(timeout, sql, parameters);

        if (dataTable.Rows.Count == 0)
            dataTable.Rows.Add(dataTable.NewRow());

        return dataTable.Rows[0];
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and returns the first <see cref="DataRow"/> in the result set.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>The first <see cref="DataRow"/> in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<DataRow> RetrieveRowAsync(this DbCommand command, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        return command.RetrieveRowAsync(DefaultTimeoutDuration, sql, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and returns the first <see cref="DataRow"/> in the result set.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>The first <see cref="DataRow"/> in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static async Task<DataRow> RetrieveRowAsync(this DbCommand command, int timeout, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        DataTable dataTable = await command.RetrieveDataAsync(timeout, sql, cancellationToken, parameters).ConfigureAwait(false);

        if (dataTable.Rows.Count == 0)
            dataTable.Rows.Add(dataTable.NewRow());

        return dataTable.Rows[0];
    }

    /// <summary>
    /// Tries to retrieve the first <see cref="DataRow"/> in the result set of the SQL statement using <see cref="DbCommand"/>.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="row">The first <see cref="DataRow"/> in the result set, or <c>null</c>.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>The first <see cref="DataRow"/> in the result set.</returns>
    public static bool TryRetrieveRow(this DbCommand command, string sql, out DataRow? row, params object[] parameters)
    {
        return command.TryRetrieveRow(sql, DefaultTimeoutDuration, out row, parameters);
    }

    /// <summary>
    /// Tries to retrieve the first <see cref="DataRow"/> in the result set of the SQL statement using <see cref="DbCommand"/>.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="row">The first <see cref="DataRow"/> in the result set, or <c>null</c>.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>The first <see cref="DataRow"/> in the result set.</returns>
    public static bool TryRetrieveRow(this DbCommand command, string sql, int timeout, out DataRow? row, params object[] parameters)
    {
        DataTable dataTable = command.RetrieveData(sql, timeout, parameters);

        if (dataTable.Rows.Count == 0)
        {
            row = null;
            return false;
        }

        row = dataTable.Rows[0];
        return true;
    }

    /// <summary>
    /// Tries to retrieve the first <see cref="DataRow"/> in the result set of the SQL statement using <see cref="DbCommand"/>.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>Tuple of first <see cref="DataRow"/> in the result set (can be <c>null</c>) and a flag that determines if retrieve was successful.</returns>
    public static Task<(DataRow? row, bool)> TryRetrieveRowAsync(this DbCommand command, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        return command.TryRetrieveRowAsync(sql, DefaultTimeoutDuration, cancellationToken, parameters);
    }

    /// <summary>
    /// Tries to retrieve the first <see cref="DataRow"/> in the result set of the SQL statement using <see cref="DbCommand"/>.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>Tuple of first <see cref="DataRow"/> in the result set (can be <c>null</c>) and a flag that determines if retrieve was successful.</returns>
    public static async Task<(DataRow? row, bool)> TryRetrieveRowAsync(this DbCommand command, string sql, int timeout, CancellationToken cancellationToken, params object[] parameters)
    {
        DataTable dataTable = await command.RetrieveDataAsync(timeout, sql, cancellationToken, parameters).ConfigureAwait(false);
        return dataTable.Rows.Count == 0 ? (null, false) : (dataTable.Rows[0], true);
    }

    #endregion

    #region [ RetrieveData Overloaded Extensions ]

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and returns the first <see cref="DataTable"/> 
    /// of result set, if the result set contains multiple tables.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DataTable"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataTable RetrieveData(this DbConnection connection, string sql, params object[] parameters)
    {
        return connection.RetrieveData(DefaultTimeoutDuration, sql, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and returns the first <see cref="DataTable"/> 
    /// of result set, if the result set contains multiple tables.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DataTable"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataTable RetrieveData(this DbConnection connection, int timeout, string sql, params object[] parameters)
    {
        return connection.RetrieveDataSet(timeout, sql, parameters).Tables[0];
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and returns the first <see cref="DataTable"/> 
    /// of result set, if the result set contains multiple tables.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DataTable"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<DataTable> RetrieveDataAsync(this DbConnection connection, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        return connection.RetrieveDataAsync(DefaultTimeoutDuration, sql, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and returns the first <see cref="DataTable"/> 
    /// of result set, if the result set contains multiple tables.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DataTable"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static async Task<DataTable> RetrieveDataAsync(this DbConnection connection, int timeout, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        return (await connection.RetrieveDataSetAsync(timeout, sql, cancellationToken, parameters).ConfigureAwait(false)).Tables[0];
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and returns the first <see cref="DataTable"/> 
    /// of result set, if the result set contains multiple tables.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DataTable"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataTable RetrieveData(this DbCommand command, string sql, params object[] parameters)
    {
        return command.RetrieveData(DefaultTimeoutDuration, sql, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and returns the first <see cref="DataTable"/> 
    /// of result set, if the result set contains multiple tables.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DataTable"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataTable RetrieveData(this DbCommand command, int timeout, string sql, params object[] parameters)
    {
        return command.RetrieveDataSet(timeout, sql, parameters).Tables[0];
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and returns the first <see cref="DataTable"/> 
    /// of result set, if the result set contains multiple tables.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DataTable"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<DataTable> RetrieveDataAsync(this DbCommand command, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        return command.RetrieveDataAsync(DefaultTimeoutDuration, sql, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and returns the first <see cref="DataTable"/> 
    /// of result set, if the result set contains multiple tables.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DataTable"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static async Task<DataTable> RetrieveDataAsync(this DbCommand command, int timeout, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        return (await command.RetrieveDataSetAsync(timeout, sql, cancellationToken, parameters).ConfigureAwait(false)).Tables[0];
    }

    #endregion

    #region [ RetrieveDataSet Overloaded Extensions ]

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and returns the <see cref="DataSet"/> that 
    /// may contain multiple tables, depending on the SQL statement.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DataSet"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataSet RetrieveDataSet(this DbConnection connection, string sql, params object[] parameters)
    {
        return connection.RetrieveDataSet(DefaultTimeoutDuration, sql, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and returns the <see cref="DataSet"/> that 
    /// may contain multiple tables, depending on the SQL statement.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DataSet"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataSet RetrieveDataSet(this DbConnection connection, int timeout, string sql, params object[] parameters)
    {
        using DbCommand command = connection.CreateParameterizedCommand(sql, parameters);

        command.CommandTimeout = timeout;

        using DbDataReader reader = command.ExecuteReader();
        DataSet data = new("Temp");
        int tableIndex = 0;

        do
        {
            string tableName = tableIndex == 0 ? "Table" : $"Table{tableIndex}";
            data.Load(reader, LoadOption.PreserveChanges, tableName);
            tableIndex++;
        }
        while (!reader.IsClosed);

        return data;
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and returns the <see cref="DataSet"/> that 
    /// may contain multiple tables, depending on the SQL statement.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DataSet"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<DataSet> RetrieveDataSetAsync(this DbConnection connection, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        return connection.RetrieveDataSetAsync(DefaultTimeoutDuration, sql, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbConnection"/>, and returns the <see cref="DataSet"/> that 
    /// may contain multiple tables, depending on the SQL statement.
    /// </summary>
    /// <param name="connection">The <see cref="DbConnection"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DataSet"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static async Task<DataSet> RetrieveDataSetAsync(this DbConnection connection, int timeout, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        await using DbCommand command = connection.CreateParameterizedCommand(sql, parameters);

        command.CommandTimeout = timeout;

        await using DbDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        DataSet data = new("Temp");
        int tableIndex = 0;

        do
        {
            cancellationToken.ThrowIfCancellationRequested();
            string tableName = tableIndex == 0 ? "Table" : $"Table{tableIndex}";
            data.Load(reader, LoadOption.PreserveChanges, tableName);
            tableIndex++;
        }
        while (!reader.IsClosed);

        return data;
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and returns the <see cref="DataSet"/> that 
    /// may contain multiple tables, depending on the SQL statement.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DataSet"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataSet RetrieveDataSet(this DbCommand command, string sql, params object[] parameters)
    {
        return command.RetrieveDataSet(DefaultTimeoutDuration, sql, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and returns the <see cref="DataSet"/> that 
    /// may contain multiple tables, depending on the SQL statement.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DataSet"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DataSet RetrieveDataSet(this DbCommand command, int timeout, string sql, params object[] parameters)
    {
        command.CommandTimeout = timeout;
        command.Parameters.Clear();
        command.AddParametersWithValues(sql, parameters);

        using DbDataReader reader = command.ExecuteReader();
        DataSet data = new("Temp");
        int tableIndex = 0;

        do
        {
            string tableName = tableIndex == 0 ? "Table" : $"Table{tableIndex}";
            data.Load(reader, LoadOption.PreserveChanges, tableName);
            tableIndex++;
        }
        while (!reader.IsClosed);

        return data;
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and returns the <see cref="DataSet"/> that 
    /// may contain multiple tables, depending on the SQL statement.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DataSet"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<DataSet> RetrieveDataSetAsync(this DbCommand command, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        return command.RetrieveDataSetAsync(DefaultTimeoutDuration, sql, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="DbCommand"/>, and returns the <see cref="DataSet"/> that 
    /// may contain multiple tables, depending on the SQL statement.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> to use for executing the SQL statement.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sql">The SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters identified by '@' prefix in <paramref name="sql"/> expression.</param>
    /// <returns>A <see cref="DataSet"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static async Task<DataSet> RetrieveDataSetAsync(this DbCommand command, int timeout, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        command.CommandTimeout = timeout;
        command.Parameters.Clear();
        command.AddParametersWithValues(sql, parameters);

        await using DbDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        DataSet data = new("Temp");
        int tableIndex = 0;

        do
        {
            cancellationToken.ThrowIfCancellationRequested();
            string tableName = tableIndex == 0 ? "Table" : $"Table{tableIndex}";
            data.Load(reader, LoadOption.PreserveChanges, tableName);
            tableIndex++;
        }
        while (!reader.IsClosed);

        return data;
    }

    #endregion

    #region [ DataRow Extensions ]

    /// <summary>
    /// Provides strongly-typed access to each of the column values in the specified row.
    /// Automatically applies type conversion to the column values.
    /// </summary>
    /// <typeparam name="T">A generic parameter that specifies the return type of the column.</typeparam>
    /// <param name="row">The input <see cref="DataRow"/>, which acts as the instance for the extension method.</param>
    /// <param name="field">The name of the column to return the value of.</param>
    /// <returns>The value, of type T, of the <see cref="DataColumn"/> specified by <paramref name="field"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T ConvertField<T>(this DataRow row, string field)
    {
        return ConvertField(row, field, default(T)!);
    }

    /// <summary>
    /// Provides strongly-typed access to each of the column values in the specified row.
    /// Automatically applies type conversion to the column values.
    /// </summary>
    /// <typeparam name="T">A generic parameter that specifies the return type of the column.</typeparam>
    /// <param name="row">The input <see cref="DataRow"/>, which acts as the instance for the extension method.</param>
    /// <param name="field">The name of the column to return the value of.</param>
    /// <param name="defaultValue">The value to be substituted if <see cref="DBNull.Value"/> is retrieved.</param>
    /// <returns>The value, of type T, of the <see cref="DataColumn"/> specified by <paramref name="field"/>.</returns>
    public static T ConvertField<T>(this DataRow row, string field, T defaultValue)
    {
        object? value = row.Field<object>(field);

        if (value is null || value == DBNull.Value)
            return defaultValue;

        // If the value is an instance of the given type,
        // no type conversion is necessary
        if (value is T typeValue)
            return typeValue;

        Type type = typeof(T);

        // Nullable types cannot be used in type conversion, but we can use Nullable.GetUnderlyingType()
        // to determine whether the type is nullable and convert to the underlying type instead
        Type underlyingType = Nullable.GetUnderlyingType(type) ?? type;

        // Handle Guids as a special case since they do not implement IConvertible
        if (underlyingType == typeof(Guid))
            return (T)(object)Guid.Parse(value.ToString() ?? "");

        // Handle enums as a special case since they do not implement IConvertible
        if (underlyingType.IsEnum)
            return (T)Enum.Parse(underlyingType, value.ToString() ?? "");

        return (T)Convert.ChangeType(value, underlyingType);
    }

    /// <summary>
    /// Automatically applies type conversion to column values when only a type is available.
    /// </summary>
    /// <param name="row">The input <see cref="DataRow"/>, which acts as the instance for the extension method.</param>
    /// <param name="field">The name of the column to return the value of.</param>
    /// <param name="type">Type of the column.</param>
    /// <returns>The value of the <see cref="DataColumn"/> specified by <paramref name="field"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static object? ConvertField(this DataRow row, string field, Type type)
    {
        return ConvertField(row, field, type, null!);
    }

    /// <summary>
    /// Automatically applies type conversion to column values when only a type is available.
    /// </summary>
    /// <param name="row">The input <see cref="DataRow"/>, which acts as the instance for the extension method.</param>
    /// <param name="field">The name of the column to return the value of.</param>
    /// <param name="type">Type of the column.</param>
    /// <param name="defaultValue">The value to be substituted if <see cref="DBNull.Value"/> is retrieved.</param>
    /// <returns>The value of the <see cref="DataColumn"/> specified by <paramref name="field"/>.</returns>
    public static object? ConvertField(this DataRow row, string field, Type type, object? defaultValue)
    {
        object? value = row.Field<object>(field);

        if (value is null || value == DBNull.Value)
            return defaultValue ?? (type.IsValueType ? Activator.CreateInstance(type) : null);

        // If the value is an instance of the given type,
        // no type conversion is necessary
        if (type.IsInstanceOfType(value))
            return value;

        // Nullable types cannot be used in type conversion, but we can use Nullable.GetUnderlyingType()
        // to determine whether the type is nullable and convert to the underlying type instead
        Type underlyingType = Nullable.GetUnderlyingType(type) ?? type;

        // Handle Guids as a special case since they do not implement IConvertible
        if (underlyingType == typeof(Guid))
            return Guid.Parse(value.ToString() ?? "");

        // Handle enums as a special case since they do not implement IConvertible
        if (underlyingType.IsEnum)
            return Enum.Parse(underlyingType, value.ToString() ?? "");

        return Convert.ChangeType(value, underlyingType);
    }

    /// <summary>
    /// Provides strongly-typed access to each of the column values in the specified row.
    /// Automatically applies type conversion to the column values.
    /// </summary>
    /// <typeparam name="T">A generic parameter that specifies the return type of the column.</typeparam>
    /// <param name="row">The input <see cref="DataRow"/>, which acts as the instance for the extension method.</param>
    /// <param name="field">The name of the column to return the value of.</param>
    /// <returns>The value, of type T, of the <see cref="DataColumn"/> specified by <paramref name="field"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T? ConvertNullableField<T>(this DataRow row, string field) where T : struct
    {
        object? value = row.Field<object>(field);

        if (value is null)
            return null;

        return (T)Convert.ChangeType(value, typeof(T));
    }

    /// <summary>
    /// Parses a Guid from a database field that is a Guid type or a string representing a Guid.
    /// </summary>
    /// <param name="row">The input <see cref="DataRow"/>, which acts as the instance for the extension method.</param>
    /// <param name="field">The name of the column to return the value of.</param>
    /// <param name="defaultValue">The value to be substituted if <see cref="DBNull.Value"/> is retrieved; defaults to <see cref="Guid.Empty"/>.</param>
    /// <returns>The <see cref="Guid"/> value of the <see cref="DataColumn"/> specified by <paramref name="field"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Guid ConvertGuidField(this DataRow row, string field, Guid? defaultValue = null)
    {
        object? value = row.Field<object>(field);

        if (value is null || value == DBNull.Value)
            return defaultValue ?? Guid.Empty;

        if (value is Guid guidValue)
            return guidValue;

        return Guid.Parse(value.ToString() ?? "");
    }

    #endregion

    #region [ DataTable Extensions ]

    /// <summary>
    /// Returns an <see cref="IAsyncEnumerable{T}"/> of <see cref="DataRow"/> values from the <see cref="DataTable"/>.
    /// </summary>
    /// <param name="source">The source <see cref="DataTable" /> to make async enumerable.</param>
    /// <exception cref="ArgumentNullException">The source <see cref="DataTable" /> is <c>null</c>>.</exception>
    /// <returns>An <see cref="IAsyncEnumerable{T}"/> of <see cref="DataRow"/> values from the <see cref="DataTable"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IAsyncEnumerable<DataRow> AsAsyncEnumerable(this DataTable source)
    {
        return source.AsEnumerable().ToAsyncEnumerable();
    }

    /// <summary>
    /// Returns a cancellable <see cref="IAsyncEnumerable{T}"/> of <see cref="DataRow"/> values from the <see cref="DataTable"/>
    /// and configures how awaits on the tasks returned from an async iteration will be performed.
    /// </summary>
    /// <param name="source">The source <see cref="DataTable" /> to make async enumerable.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="continueOnCapturedContext">Whether to capture and marshal back to the current context.</param>
    /// <exception cref="ArgumentNullException">The source <see cref="DataTable" /> is <c>null</c>>.</exception>
    /// <returns>An <see cref="IAsyncEnumerable{T}"/> of <see cref="DataRow"/> values from the <see cref="DataTable"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ConfiguredCancelableAsyncEnumerable<DataRow> AsAwaitConfiguredCancelableAsyncEnumerable(this DataTable source, CancellationToken cancellationToken, bool continueOnCapturedContext = false)
    {
        return source.AsAsyncEnumerable().WithAwaitConfiguredCancellation(cancellationToken, continueOnCapturedContext);
    }
        
    #endregion

    #region [ UpdateData Overloaded Functions ]

    /// <summary>
    /// Updates the underlying data of the <see cref="DataTable"/> using <see cref="SqlConnection"/>, and
    /// returns the number of rows successfully updated.
    /// </summary>
    /// <param name="sourceData">The <see cref="DataTable"/> used to update the underlying data source.</param>
    /// <param name="sourceSql">The SQL statement used initially to populate the <see cref="DataTable"/>.</param>
    /// <param name="connection">The <see cref="SqlConnection"/> to use for updating the underlying data source.</param>
    /// <returns>The number of rows successfully updated from the <see cref="DataTable"/>.</returns>
    [SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
    public static int UpdateData(this SqlConnection connection, DataTable sourceData, string sourceSql)
    {
        SqlDataAdapter dataAdapter = new(sourceSql, connection);
        SqlCommandBuilder _ = new(dataAdapter);

        return dataAdapter.Update(sourceData);
    }

    #endregion

    #region [ Command Parameter Population Functions ]

    /// <summary>
    /// Takes the <see cref="SqlCommand"/> object and populates it with the given parameters.
    /// </summary>
    /// <param name="command">The <see cref="SqlCommand"/> whose parameters are to be populated.</param>
    /// <param name="parameter1">The first parameter value to populate the <see cref="SqlCommand"/> parameters with.</param>
    /// <param name="parameters">The remaining parameter values to populate the <see cref="SqlCommand"/> parameters with.</param>
    public static void PopulateParameters(this SqlCommand command, object? parameter1, params object[] parameters)
    {
        command.PopulateParameters(new[] { parameter1 }.Concat(parameters).NullAsDBNull());
    }

    /// <summary>
    ///  Takes the <see cref="SqlCommand"/> object and populates it with the given parameters.
    /// </summary>
    /// <param name="command">The <see cref="SqlCommand"/> whose parameters are to be populated.</param>
    /// <param name="parameters">The parameter values to populate the <see cref="SqlCommand"/> parameters with.</param>
    public static void PopulateParameters(this SqlCommand command, object[] parameters)
    {
        command.PopulateParameters(SqlCommandBuilder.DeriveParameters, parameters);
    }

    /// <summary>
    /// Takes the <see cref="DbCommand"/> object and populates it with the given parameters.
    /// </summary>
    /// <param name="command">The <see cref="DbCommand"/> whose parameters are to be populated.</param>
    /// <param name="deriveParameters">The DeriveParameters() implementation of the <paramref name="command"/> to use to populate parameters.</param>
    /// <param name="values">The parameter values to populate the <see cref="DbCommand"/> parameters with.</param>
    /// <typeparam name="TDbCommand">Then <see cref="DbCommand"/> type to be used.</typeparam>
    /// <exception cref="ArgumentException">
    /// Number of <see cref="DbParameter"/> arguments in <see cref="DbCommand.CommandText"/> of this <paramref name="command"/>, identified by '@', do not match number of supplied parameter <paramref name="values"/> -or-
    /// You have supplied more <paramref name="values"/> than parameters listed for the stored procedure.
    /// </exception>
    public static void PopulateParameters<TDbCommand>(this TDbCommand command, Action<TDbCommand> deriveParameters, object[]? values) where TDbCommand : DbCommand
    {
        if (values is null)
            return;

        string commandText = command.CommandText;

        if (string.IsNullOrEmpty(commandText))
            throw new ArgumentNullException(nameof(command), "command.CommandText is null");

        // Add parameters for standard SQL expressions (i.e., non stored procedure expressions)
        if (!IsStoredProcedure(commandText))
        {
            command.AddParametersWithValues(commandText, values);
            return;
        }

        command.CommandType = CommandType.StoredProcedure;

        // Makes quick query to db to find the parameters for the StoredProc, and then creates them for
        // the command. The DeriveParameters() is only for commands with CommandType of StoredProcedure.
        deriveParameters(command);

        // Removes the ReturnValue Parameter.
        command.Parameters.RemoveAt(0);

        // Checks to see if the Parameters found match the Values provided.
        if (command.Parameters.Count != values.Length)
        {
            // If there are more values than parameters, throws an error.
            if (values.Length > command.Parameters.Count)
                throw new ArgumentException("You have supplied more values than parameters listed for the stored procedure");

            // Otherwise, assume that the missing values are for Parameters that have default values,
            // and the code uses the default. To do this fill the extended ParamValue as Nothing/Null.
            Array.Resize(ref values, command.Parameters.Count); // Makes the Values array match the Parameters of the Stored Proc.
        }

        // Assigns the values to the Parameters.
        for (int i = 0; i < command.Parameters.Count; i++)
            command.Parameters[i].Value = values[i];
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsStoredProcedure(string sql)
    {
        // No lock required for this use case: https://docs.microsoft.com/en-us/dotnet/standard/base-types/thread-safety-in-regular-expressions
        return s_sqlIdentifierRegex.IsMatch(sql.Trim());
    }

    /// <summary>
    /// Creates and adds an <see cref="DbParameter"/> to the <see cref="DbCommand"/> object with the specified <paramref name="value"/>.
    /// </summary>
    /// <param name="command"><see cref="DbCommand"/> to which parameter needs to be added.</param>
    /// <param name="name">Name of the <see cref="DbParameter"/> to be added.</param>
    /// <param name="value">Value of the <see cref="DbParameter"/> to be added.</param>
    /// <param name="direction"><see cref="ParameterDirection"/> for <see cref="DbParameter"/>.</param>
    public static void AddParameterWithValue(this DbCommand command, string name, object value, ParameterDirection direction = ParameterDirection.Input)
    {
        if (value is DbParameter)
        {
            // Value is already a parameter.
            command.Parameters.Add(value);
        }
        else
        {
            // Create a parameter for the value.
            DbParameter parameter = command.CreateParameter();

            parameter.ParameterName = name;
            parameter.Value = value;
            parameter.Direction = direction;

            command.Parameters.Add(parameter);
        }
    }

    /// <summary>
    /// Creates and adds a new <see cref="DbParameter"/> for each of the specified <paramref name="values"/> to the <see cref="DbCommand"/> object.
    /// </summary>
    /// <param name="command"><see cref="DbCommand"/> to which parameters need to be added.</param>
    /// <param name="sql">The SQL statement.</param>
    /// <param name="values">The values for the parameters of the <see cref="DbCommand"/> in the order that they appear in the SQL statement.</param>
    /// <remarks>
    /// <para>
    /// This method does very rudimentary parsing of the SQL statement so parameter names should start with the '@'
    /// character and should be surrounded by either spaces, parentheses, or commas.
    /// </para>
    /// <para>
    /// Do not use the same parameter name twice in the expression so that each parameter, identified by '@', will
    /// have a corresponding value.
    /// </para>
    /// </remarks>
    /// <returns>The fully populated parameterized command.</returns>
    /// <exception cref="ArgumentException">Number of <see cref="DbParameter"/> arguments in <paramref name="sql"/> expression, identified by '@', do not match number of supplied parameter <paramref name="values"/>.</exception>
    public static void AddParametersWithValues(this DbCommand command, string sql, params object[] values)
    {
        if (values.FirstOrDefault(value => value is DbParameter) is not null)
        {
            // Values are already parameters
            foreach (object param in values)
                command.Parameters.Add(param);
        }
        else
        {
            // Pick up all parameters that start with @ or : but skip keywords such as @@IDENTITY
            string[] tokens = sql.Split(' ', '(', ')', ',', '=')
                .Where(token => token.StartsWith(":", StringComparison.Ordinal) || token.StartsWith("@", StringComparison.Ordinal) && !token.StartsWith("@@", StringComparison.Ordinal))
                .Distinct()
                .Where(IsValidParameter)
                .ToArray();

            int i = 0;

            if (tokens.Length != values.Length)
                throw new ArgumentException("Number of parameter arguments in SQL expression do not match number of supplied values", nameof(values));

            foreach (string token in tokens)
            {
                if (!command.Parameters.Contains(token))
                    command.AddParameterWithValue(token, values[i++]);
            }
        }

        command.CommandText = sql;
    }

    private static bool IsValidParameter(string token)
    {
        // No lock required for this use case: https://docs.microsoft.com/en-us/dotnet/standard/base-types/thread-safety-in-regular-expressions
        return s_sqlParameterRegex.IsMatch(token);
    }

    /// <summary>
    /// Creates and returns a parameterized <see cref="DbCommand"/>. Parameter names are embedded in the SQL statement
    /// passed as a parameter to this method.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="sql">The SQL statement.</param>
    /// <param name="values">The values for the parameters of the <see cref="DbCommand"/> in the order that they appear in the SQL statement.</param>
    /// <remarks>
    /// <para>
    /// This method does very rudimentary parsing of the SQL statement so parameter names should start with the '@'
    /// character and should be surrounded by either spaces, parentheses, or commas.
    /// </para>
    /// <para>
    /// Do not use the same parameter name twice in the expression so that each parameter, identified by '@', will
    /// have a corresponding value.
    /// </para>
    /// </remarks>
    /// <returns>The fully populated parameterized command.</returns>
    /// <exception cref="ArgumentException">Number of <see cref="DbParameter"/> arguments in <paramref name="sql"/> expression, identified by '@', do not match number of supplied parameter <paramref name="values"/>.</exception>
    public static DbCommand CreateParameterizedCommand(this DbConnection connection, string sql, params object[] values)
    {
        DbCommand command = connection.CreateCommand();

        command.AddParametersWithValues(sql, values);

        if (IsStoredProcedure(sql))
        {
            command.CommandType = CommandType.StoredProcedure;

            // Force parameters for stored procedures to have no name - cannot determine proper
            // name in a database abstract way. As a result, callers must specify proper number
            // of parameters for stored procedure, in order.
            foreach (DbParameter parameter in command.Parameters)
                parameter.ParameterName = null;
        }

        return command;
    }

    /// <summary>
    /// Gets any <c>null</c> parameter values as <see cref="DBNull"/>.
    /// </summary>
    /// <param name="values">Source parameter values.</param>
    /// <returns>Parameter values with <c>null</c> replaced with <see cref="DBNull"/>.</returns>
    public static object[] NullAsDBNull(this IEnumerable<object?> values) => values.Select(value => value ?? DBNull.Value).ToArray();

    #endregion

    #region [ CSV / DataTable Conversion Functions ]

    /// <summary>
    /// Converts a delimited string into a <see cref="DataTable"/>.
    /// </summary>
    /// <param name="delimitedData">The delimited text to be converted to <see cref="DataTable"/>.</param>
    /// <param name="delimiter">The character(s) used for delimiting the text.</param>
    /// <param name="header">true, if the delimited text contains header information; otherwise, false.</param>
    /// <returns>A <see cref="DataTable"/> object.</returns>
    public static DataTable ToDataTable(this string delimitedData, string delimiter, bool header)
    {
        DataTable table = new();

        string pattern =
            // Regex pattern that will be used to split the delimited data.
            $"{Regex.Escape(delimiter)}(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))";

        // Remove any leading and trailing whitespace, carriage returns or line feeds.
        delimitedData = delimitedData.Trim().Trim('\r', '\n').Replace("\n", "");

        string[] lines = delimitedData.Split('\r'); //Splits delimited data into lines.

        int cursor = 0;

        // Assumes that the first line has header information.
        string[] headers = Regex.Split(lines[cursor], pattern);

        // Creates columns.
        if (header)
        {
            // Uses the first row as header row.
            foreach (string columnName in headers)
                table.Columns.Add(new DataColumn(columnName.Trim('\"'))); //Remove any leading and trailing quotes from the column name.

            cursor++;
        }
        else
        {
            for (int i = 0; i < headers.Length; i++)
                table.Columns.Add(new DataColumn());
        }

        // Populates the data table with CSV data.
        for (; cursor < lines.Length; cursor++)
        {
            // Creates new row.
            DataRow row = table.NewRow();

            // Populates the new row.
            string[] fields = Regex.Split(lines[cursor], pattern);

            // Removes any leading and trailing quotes from the data.
            for (int i = 0; i < fields.Length; i++)
                row[i] = fields[i].Trim('\"');

            // Adds the new row.
            table.Rows.Add(row);
        }

        return table;
    }

    /// <summary>
    /// Converts the <see cref="DataTable"/> to a multi-line delimited string (e.g., CSV export).
    /// </summary>
    /// <param name="table">The <see cref="DataTable"/> whose data is to be converted to delimited text.</param>
    /// <param name="delimiter">The character(s) to be used for delimiting the text.</param>
    /// <param name="quoted">true, if text is to be surrounded by quotes; otherwise, false.</param>
    /// <param name="header">true, if the delimited text should have header information.</param>
    /// <returns>A string of delimited text.</returns>
    public static string ToDelimitedString(this DataTable table, string delimiter, bool quoted, bool header)
    {
        StringBuilder data = new();

        // Uses the column names as the headers if headers are requested.
        if (header)
        {
            for (int i = 0; i < table.Columns.Count; i++)
            {
                data.Append($"{(quoted ? "\"" : "")}{table.Columns[i].ColumnName}{(quoted ? "\"" : "")}");

                if (i < table.Columns.Count - 1)
                    data.Append(delimiter);
            }

            data.Append(Environment.NewLine);
        }

        for (int i = 0; i < table.Rows.Count; i++)
        {
            // Converts data table's data to delimited data.
            for (int j = 0; j < table.Columns.Count; j++)
            {
                data.Append($"{(quoted ? "\"" : "")}{table.Rows[i][j]}{(quoted ? "\"" : "")}");

                if (j < table.Columns.Count - 1)
                    data.Append(delimiter);
            }

            data.Append(Environment.NewLine);
        }

        // Returns the delimited data.
        return data.ToString();
    }

    #endregion
}
