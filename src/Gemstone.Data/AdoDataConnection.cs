//  AdoDataConnection.cs - Gbtc
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
//  04/07/2011 - J. Ritchie Carroll
//       Generated original version of source code.
//  09/19/2011 - Stephen C. Wills
//       Added database awareness and Oracle database compatibility.
//  10/18/2011 - J. Ritchie Carroll
//       Modified ADO database class to allow directly instantiated instances, as well as configured.
//  12/14/2012 - Starlynn Danyelle Gilliam
//       Modified Header.
//  12/13/2019 - J. Ritchie Carroll
//      Migrated to Gemstone libraries.
//
//******************************************************************************************************
// ReSharper disable CoVariantArrayConversion
// ReSharper disable InconsistentNaming

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Gemstone.Configuration;
using Gemstone.Data.DataExtensions;
using Gemstone.StringExtensions;

namespace Gemstone.Data;

/// <summary>
/// Specifies the database type underlying an <see cref="AdoDataConnection"/>.
/// </summary>
public enum DatabaseType
{
    /// <summary>
    /// Underlying ADO database type is Microsoft Access.
    /// </summary>
    Access,

    /// <summary>
    /// Underlying ADO database type is SQL Server.
    /// </summary>
    SQLServer,

    /// <summary>
    /// Underlying ADO database type is MySQL.
    /// </summary>
    MySQL,

    /// <summary>
    /// Underlying ADO database type is Oracle.
    /// </summary>
    Oracle,

    /// <summary>
    /// Underlying ADO database type is SQLite.
    /// </summary>
    SQLite,

    /// <summary>
    /// Underlying ADO database type is PostgreSQL.
    /// </summary>
    PostgreSQL,

    /// <summary>
    /// Underlying ADO database type is unknown.
    /// </summary>
    Other
}

/// <summary>
/// Creates a new <see cref="DbConnection"/> from any specified or configured ADO.NET data source.
/// </summary>
/// <remarks>
/// Example connection and data provider strings:
/// <list type="table">
///     <listheader>
///         <term>Database type</term>
///         <description>Example connection string / data provider string</description>
///     </listheader>
///     <item>
///         <term>SQL Server</term>
///         <description>
///         ConnectionString = "Data Source=serverName; Initial Catalog=databaseName; User ID=userName; Password=password"<br/>
///         DataProviderString = "AssemblyName=Microsoft.Data.SqlClient; ConnectionType=Microsoft.Data.SqlClient.SqlConnection"
///         </description>
///     </item>
///     <item>
///         <term>SQLite</term>
///         <description>
///         ConnectionString = "Data Source=databaseName.db; Version=3; Foreign Keys=True; FailIfMissing=True"<br/>
///         DataProviderString = "AssemblyName=Microsoft.Data.Sqlite; ConnectionType=Microsoft.Data.Sqlite.SqliteConnection"
///         </description>
///     </item>
///     <item>
///         <term>PostgreSQL</term>
///         <description>
///         ConnectionString = "Host=localhost; Database=DatabaseName; Username=postgres; Password=password"<br/>
///         DataProviderString = "AssemblyName=Npgsql; ConnectionType=Npgsql.NpgsqlConnection"
///         </description>
///     </item>
///     <item>
///         <term>MySQL</term>
///         <description>
///         ConnectionString = "Server=serverName; Database=databaseName; Uid=root; Pwd=password; allow user variables = true"<br/>
///         DataProviderString = "AssemblyName=MySql.Data; ConnectionType=MySql.Data.MySqlClient.MySqlConnection"
///         </description>
///     </item>
///     <item>
///         <term>Oracle</term>
///         <description>
///         ConnectionString = "Data Source=tnsName; User ID=schemaUserName; Password=schemaPassword"<br/>
///         DataProviderString = "AssemblyName=Oracle.ManagedDataAccess; ConnectionType=Oracle.ManagedDataAccess.Client.OracleConnection"
///         </description>
///     </item>
/// </list>
/// Example configuration file that defines connection settings:
/// <code>
/// <![CDATA[
/// <?xml version="1.0" encoding="utf-8"?>
/// <configuration>
///   <configSections>
///     <section name="categorizedSettings" type="GSF.Configuration.CategorizedSettingsSection, GSF.Core" />
///   </configSections>
///   <categorizedSettings>
///     <systemSettings>
///       <add name="ConnectionString" value="Data Source=localhost\SQLEXPRESS; Initial Catalog=MyDatabase; Integrated Security=SSPI" description="ADO database connection string" encrypted="false" />
///       <add name="DataProviderString" value="AssemblyName={System.Data, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089}; ConnectionType=System.Data.SqlClient.SqlConnection" description="ADO database provider string" encrypted="false" />
///     </systemSettings>
///   </categorizedSettings>
///   <startup>
///     <supportedRuntime version="v4.0" sku=".NETFramework,Version=v4.5" />
///   </startup>
/// </configuration>
/// ]]>
/// </code>
/// </remarks>
public class AdoDataConnection : IDisposable
{
    #region [ Members ]

    // Fields
    private readonly bool m_disposeConnection;
    private bool m_disposed;

    #endregion

    #region [ Constructors ]

    /// <summary>
    /// Creates and opens a new <see cref="AdoDataConnection"/> from specified <paramref name="settings"/>.
    /// </summary>
    /// <param name="settings">Settings instance.</param>
    public AdoDataConnection(Settings settings)
        : this(settings["System"]["ConnectionString"]!.ToString(), settings["System"]["DataProviderString"]!.ToString())
    {
    }

    /// <summary>
    /// Creates and opens a new <see cref="AdoDataConnection"/> from specified <paramref name="connectionString"/> and <paramref name="dataProviderString"/>.
    /// </summary>
    /// <param name="connectionString">Database specific ADO connection string.</param>
    /// <param name="dataProviderString">Key/value pairs that define which ADO assembly and types to load.</param>
    public AdoDataConnection(string? connectionString, string? dataProviderString)
        : this(connectionString, dataProviderString, true)
    {
    }

    /// <summary>
    /// Creates and opens a new <see cref="AdoDataConnection"/> from specified <paramref name="connectionString"/>
    /// and <paramref name="connectionType"/>.
    /// </summary>
    /// <param name="connectionString">Database specific ADO connection string.</param>
    /// <param name="connectionType">The ADO type used to establish the database connection.</param>
    public AdoDataConnection(string connectionString, Type connectionType)
    {
        if (!typeof(DbConnection).IsAssignableFrom(connectionType))
            throw new ArgumentException($"Connection type must derived from the {nameof(DbConnection)} class", nameof(connectionType));

        DatabaseType = GetDatabaseType(connectionType);
        m_disposeConnection = true;

        try
        {
            // Open ADO.NET provider connection
            Connection = (DbConnection)Activator.CreateInstance(connectionType)!;
            Connection.ConnectionString = connectionString;
            Connection.Open();
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to open ADO data connection, verify \"ConnectionString\": {ex.Message}", ex);
        }
    }

    /// <summary>
    /// Creates a new <see cref="AdoDataConnection"/> from specified <paramref name="connection"/>.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="disposeConnection">True if the database connection should be closed when the <see cref="AdoDataConnection"/> is disposed; false otherwise.</param>
    public AdoDataConnection(DbConnection connection, bool disposeConnection)
    {
        Connection = connection;
        DatabaseType = GetDatabaseType(connection.GetType());
        m_disposeConnection = disposeConnection;
    }

    // Creates a new AdoDataConnection, optionally opening connection.
    private AdoDataConnection(string? connectionString, string? dataProviderString, bool openConnection)
    {
        Type? connectionType;

        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentNullException(nameof(connectionString), "Parameter cannot be null or empty");

        if (string.IsNullOrWhiteSpace(dataProviderString))
            throw new ArgumentNullException(nameof(dataProviderString), "Parameter cannot be null or empty");

        try
        {
            // Attempt to load configuration from an ADO.NET database connection
            Dictionary<string, string> settings = dataProviderString.ParseKeyValuePairs();
            string assemblyName = settings["AssemblyName"].ToNonNullString();
            string connectionTypeName = settings["ConnectionType"].ToNonNullString();

            if (string.IsNullOrEmpty(connectionTypeName))
                throw new NullReferenceException("ADO database connection type was undefined.");

            Assembly assembly = Assembly.Load(new AssemblyName(assemblyName));
            connectionType = assembly.GetType(connectionTypeName);

            if (!typeof(DbConnection).IsAssignableFrom(connectionType))
                throw new ArgumentException($"Connection type must derived from the {nameof(DbConnection)} class", nameof(dataProviderString));

            DatabaseType = GetDatabaseType(connectionType);
            m_disposeConnection = true;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to load ADO data provider, verify \"DataProviderString\": {ex.Message}", ex);
        }

        if (!openConnection)
            return;

        try
        {
            // Open ADO.NET provider connection
            Connection = (DbConnection)Activator.CreateInstance(connectionType)!;
            Connection.ConnectionString = connectionString;
            Connection.Open();
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to open ADO data connection, verify \"ConnectionString\": {ex.Message}", ex);
        }
    }

    /// <summary>
    /// Releases the unmanaged resources before the <see cref="AdoDataConnection"/> object is reclaimed by <see cref="GC"/>.
    /// </summary>
    ~AdoDataConnection() => Dispose(false);

    #endregion

    #region [ Properties ]

    /// <summary>
    /// Gets an open <see cref="DbConnection"/> to configured ADO.NET data source.
    /// </summary>
    public DbConnection Connection { get; } = default!;

    /// <summary>
    /// Gets or sets the type of the database underlying the <see cref="AdoDataConnection"/>.
    /// </summary>
    /// <remarks>
    /// This value is automatically assigned based on the connection type specified in the data provider string, however,
    /// if the database type cannot be determined it will be set to <see cref="Data.DatabaseType.Other"/>. In this
    /// case, if you know the behavior of your custom ADO database connection matches that of another defined database
    /// type, you can manually assign the database type to allow for database interaction interoperability.
    /// </remarks>
    public DatabaseType DatabaseType { get; set; }

    /// <summary>
    /// Gets or sets default timeout for <see cref="AdoDataConnection"/> data operations.
    /// </summary>
    public int DefaultTimeout { get; set; } = DataExtensions.DataExtensions.DefaultTimeoutDuration;

    /// <summary>
    /// Gets current UTC date-time in an implementation that is proper for the connected <see cref="AdoDataConnection"/> database type.
    /// </summary>
    public object UtcNow
    {
        get
        {
            if (IsJetEngine)
                return DateTime.UtcNow.ToOADate();

            if (IsSqlite)
                return new DateTime(DateTime.UtcNow.Ticks, DateTimeKind.Unspecified);

            return DateTime.UtcNow;
        }
    }

    /// <summary>
    /// Gets the default <see cref="IsolationLevel"/> for the connected <see cref=" AdoDataConnection"/> database type.
    /// </summary>
    public IsolationLevel DefaultIsolationLevel => IsSQLServer ? IsolationLevel.ReadUncommitted : IsolationLevel.Unspecified;

    /// <summary>
    /// Gets a value to indicate whether source database is Microsoft Access.
    /// </summary>
    public bool IsJetEngine => DatabaseType == DatabaseType.Access;

    /// <summary>
    /// Gets a value to indicate whether source database is Microsoft SQL Server.
    /// </summary>
    public bool IsSQLServer => DatabaseType == DatabaseType.SQLServer;

    /// <summary>
    /// Gets a value to indicate whether source database is MySQL.
    /// </summary>
    public bool IsMySQL => DatabaseType == DatabaseType.MySQL;

    /// <summary>
    /// Gets a value to indicate whether source database is Oracle.
    /// </summary>
    public bool IsOracle => DatabaseType == DatabaseType.Oracle;

    /// <summary>
    /// Gets a value to indicate whether source database is SQLite.
    /// </summary>
    public bool IsSqlite => DatabaseType == DatabaseType.SQLite;

    /// <summary>
    /// Gets a value to indicate whether source database is PostgreSQL.
    /// </summary>
    public bool IsPostgreSQL => DatabaseType == DatabaseType.PostgreSQL;

    #endregion

    #region [ Methods ]

    /// <summary>
    /// Executes the given SQL script using <see cref="Connection"/>.
    /// </summary>
    /// <param name="scriptPath">The path to the SQL script.</param>
    public void ExecuteScript(string scriptPath)
    {
        using TextReader scriptReader = File.OpenText(scriptPath);
        ExecuteScript(scriptReader);
    }

    /// <summary>
    /// Executes the given SQL script using <see cref="Connection"/>.
    /// </summary>
    /// <param name="scriptReader">The reader used to extract SQL statements to be executed.</param>
    public void ExecuteScript(TextReader scriptReader)
    {
        switch (DatabaseType)
        {
            case DatabaseType.SQLServer:
                // For the standard way to execute a TSQL script, refer to the following.
                //
                // http://stackoverflow.com/questions/650098/how-to-execute-an-sql-script-file-using-c-sharp
                //
                // This solution was not used here because useLegacyV2RuntimeActivationPolicy
                // needs to be added to the app.config file for it to work. This is not
                // ideal for a core library since it has no control over an application's
                // config, and it can make no assertion about the flag's suitability to the
                // application. For more information, refer to the following.
                //
                // http://web.archive.org/web/20130128072944/http://www.marklio.com/marklio/PermaLink,guid,ecc34c3c-be44-4422-86b7-900900e451f9.aspx
                Connection.ExecuteTSQLScript(scriptReader);

                break;

            case DatabaseType.Oracle:
                Connection.ExecuteOracleScript(scriptReader);

                break;

            case DatabaseType.MySQL:
                Type? mySqlScriptType = Connection.GetType().Assembly.GetType("MySql.Data.MySqlClient.MySqlScript");

                if (mySqlScriptType is not null)
                {
                    object executor = Activator.CreateInstance(mySqlScriptType, Connection, scriptReader.ReadToEnd())!;
                    MethodInfo executeMethod = executor.GetType().GetMethod("Execute")!;
                    executeMethod.Invoke(executor, null);
                }
                else
                {
                    Connection.ExecuteMySQLScript(scriptReader);
                }

                break;

            default:
                Connection.ExecuteNonQuery(scriptReader.ReadToEnd());

                break;
        }
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the number of rows affected.
    /// </summary>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>The number of rows affected.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int ExecuteNonQuery(string sqlFormat, params object?[] parameters)
    {
        return ExecuteNonQuery(DefaultTimeout, sqlFormat, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the number of rows affected.
    /// </summary>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>The number of rows affected.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> ExecuteNonQueryAsync(string sqlFormat, CancellationToken cancellationToken, params object?[] parameters)
    {
        return ExecuteNonQueryAsync(DefaultTimeout, sqlFormat, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the number of rows affected.
    /// </summary>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>The number of rows affected.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int ExecuteNonQuery(int timeout, string sqlFormat, params object?[] parameters)
    {
        string sql = GenericParameterizedQueryString(sqlFormat, parameters);
        return Connection.ExecuteNonQuery(timeout, sql, ResolveParameters(parameters));
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the number of rows affected.
    /// </summary>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>The number of rows affected.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<int> ExecuteNonQueryAsync(int timeout, string sqlFormat, CancellationToken cancellationToken, params object?[] parameters)
    {
        string sql = GenericParameterizedQueryString(sqlFormat, parameters);
        return Connection.ExecuteNonQueryAsync(timeout, sql, cancellationToken, ResolveParameters(parameters));
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and builds a <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>A <see cref="DbDataReader"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public DbDataReader ExecuteReader(string sqlFormat, params object?[] parameters)
    {
        return ExecuteReader(DefaultTimeout, sqlFormat, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and builds a <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>A <see cref="DbDataReader"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<DbDataReader> ExecuteReaderAsync(string sqlFormat, CancellationToken cancellationToken, params object?[] parameters)
    {
        return ExecuteReaderAsync(DefaultTimeout, sqlFormat, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and builds a <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>A <see cref="DbDataReader"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public DbDataReader ExecuteReader(int timeout, string sqlFormat, params object?[] parameters)
    {
        return ExecuteReader(CommandBehavior.Default, timeout, sqlFormat, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and builds a <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>A <see cref="DbDataReader"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<DbDataReader> ExecuteReaderAsync(int timeout, string sqlFormat, CancellationToken cancellationToken, params object?[] parameters)
    {
        return ExecuteReaderAsync(CommandBehavior.Default, timeout, sqlFormat, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and builds a <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="behavior">One of the <see cref="CommandBehavior"/> values.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>A <see cref="DbDataReader"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public DbDataReader ExecuteReader(CommandBehavior behavior, int timeout, string sqlFormat, params object?[] parameters)
    {
        string sql = GenericParameterizedQueryString(sqlFormat, parameters);
        return Connection.ExecuteReader(timeout, sql, behavior, ResolveParameters(parameters));
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and builds a <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="behavior">One of the <see cref="CommandBehavior"/> values.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>A <see cref="DbDataReader"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<DbDataReader> ExecuteReaderAsync(CommandBehavior behavior, int timeout, string sqlFormat, CancellationToken cancellationToken, params object?[] parameters)
    {
        string sql = GenericParameterizedQueryString(sqlFormat, parameters);
        return Connection.ExecuteReaderAsync(timeout, sql, behavior, cancellationToken, ResolveParameters(parameters));
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the value in the first column 
    /// of the first row in the result set as type <typeparamref name="T"/>.
    /// </summary>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public T ExecuteScalar<T>(string sqlFormat, params object?[] parameters)
    {
        return ExecuteScalar<T>(DefaultTimeout, sqlFormat, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the value in the first column 
    /// of the first row in the result set as type <typeparamref name="T"/>.
    /// </summary>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<T> ExecuteScalarAsync<T>(string sqlFormat, CancellationToken cancellationToken, params object?[] parameters)
    {
        return ExecuteScalarAsync<T>(DefaultTimeout, sqlFormat, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the value in the first column 
    /// of the first row in the result set as type <typeparamref name="T"/>.
    /// </summary>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public T ExecuteScalar<T>(int timeout, string sqlFormat, params object?[] parameters)
    {
        return ExecuteScalar(default(T)!, timeout, sqlFormat, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the value in the first column 
    /// of the first row in the result set as type <typeparamref name="T"/>.
    /// </summary>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<T> ExecuteScalarAsync<T>(int timeout, string sqlFormat, CancellationToken cancellationToken, params object?[] parameters)
    {
        return ExecuteScalarAsync(default(T)!, timeout, sqlFormat, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the value in the first column 
    /// of the first row in the result set as type <typeparamref name="T"/>, substituting <paramref name="defaultValue"/>
    /// if <see cref="DBNull.Value"/> is retrieved.
    /// </summary>
    /// <param name="defaultValue">The value to be substituted if <see cref="DBNull.Value"/> is retrieved.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public T ExecuteScalar<T>(T defaultValue, string sqlFormat, params object?[] parameters)
    {
        return ExecuteScalar(defaultValue, DefaultTimeout, sqlFormat, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the value in the first column 
    /// of the first row in the result set as type <typeparamref name="T"/>, substituting <paramref name="defaultValue"/>
    /// if <see cref="DBNull.Value"/> is retrieved.
    /// </summary>
    /// <param name="defaultValue">The value to be substituted if <see cref="DBNull.Value"/> is retrieved.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<T> ExecuteScalarAsync<T>(T defaultValue, string sqlFormat, CancellationToken cancellationToken, params object?[] parameters)
    {
        return ExecuteScalarAsync(defaultValue, DefaultTimeout, sqlFormat, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the value in the first column 
    /// of the first row in the result set as type <typeparamref name="T"/>, substituting <paramref name="defaultValue"/>
    /// if <see cref="DBNull.Value"/> is retrieved.
    /// </summary>
    /// <param name="defaultValue">The value to be substituted if <see cref="DBNull.Value"/> is retrieved.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public T ExecuteScalar<T>(T defaultValue, int timeout, string sqlFormat, params object?[] parameters)
    {
        return (T)ExecuteScalar(typeof(T), defaultValue, timeout, sqlFormat, parameters)!;
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the value in the first column 
    /// of the first row in the result set as type <typeparamref name="T"/>, substituting <paramref name="defaultValue"/>
    /// if <see cref="DBNull.Value"/> is retrieved.
    /// </summary>
    /// <param name="defaultValue">The value to be substituted if <see cref="DBNull.Value"/> is retrieved.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public async Task<T> ExecuteScalarAsync<T>(T defaultValue, int timeout, string sqlFormat, CancellationToken cancellationToken, params object?[] parameters)
    {
        return (T)(await ExecuteScalarAsync(typeof(T), defaultValue, timeout, sqlFormat, cancellationToken, parameters).ConfigureAwait(false))!;
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the value in the first column 
    /// of the first row in the result set as type <paramref name="returnType"/>.
    /// </summary>
    /// <param name="returnType">The type to which the result of the query should be converted.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public object? ExecuteScalar(Type returnType, string sqlFormat, params object?[] parameters)
    {
        return ExecuteScalar(returnType, DefaultTimeout, sqlFormat, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the value in the first column 
    /// of the first row in the result set as type <paramref name="returnType"/>.
    /// </summary>
    /// <param name="returnType">The type to which the result of the query should be converted.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<object?> ExecuteScalarAsync(Type returnType, string sqlFormat, CancellationToken cancellationToken, params object?[] parameters)
    {
        return ExecuteScalarAsync(returnType, DefaultTimeout, sqlFormat, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the value in the first column 
    /// of the first row in the result set as type <paramref name="returnType"/>.
    /// </summary>
    /// <param name="returnType">The type to which the result of the query should be converted.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public object? ExecuteScalar(Type returnType, int timeout, string sqlFormat, params object?[] parameters)
    {
        return returnType.IsValueType ? 
            ExecuteScalar(returnType, Activator.CreateInstance(returnType), timeout, sqlFormat, parameters) : 
            ExecuteScalar(returnType, (object)default!, timeout, sqlFormat, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the value in the first column 
    /// of the first row in the result set as type <paramref name="returnType"/>.
    /// </summary>
    /// <param name="returnType">The type to which the result of the query should be converted.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<object?> ExecuteScalarAsync(Type returnType, int timeout, string sqlFormat, CancellationToken cancellationToken, params object?[] parameters)
    {
        return returnType.IsValueType ?
            ExecuteScalarAsync(returnType, Activator.CreateInstance(returnType), timeout, sqlFormat, cancellationToken, parameters) :
            ExecuteScalarAsync(returnType, default!, timeout, sqlFormat, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the value in the first column 
    /// of the first row in the result set as type <paramref name="returnType"/>, substituting <paramref name="defaultValue"/>
    /// if <see cref="DBNull.Value"/> is retrieved.
    /// </summary>
    /// <param name="returnType">The type to which the result of the query should be converted.</param>
    /// <param name="defaultValue">The value to be substituted if <see cref="DBNull.Value"/> is retrieved.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public object? ExecuteScalar(Type returnType, object? defaultValue, string sqlFormat, params object?[] parameters)
    {
        return ExecuteScalar(returnType, defaultValue, DefaultTimeout, sqlFormat, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the value in the first column 
    /// of the first row in the result set as type <paramref name="returnType"/>, substituting <paramref name="defaultValue"/>
    /// if <see cref="DBNull.Value"/> is retrieved.
    /// </summary>
    /// <param name="returnType">The type to which the result of the query should be converted.</param>
    /// <param name="defaultValue">The value to be substituted if <see cref="DBNull.Value"/> is retrieved.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<object?> ExecuteScalarAsync(Type returnType, object? defaultValue, string sqlFormat, CancellationToken cancellationToken, params object?[] parameters)
    {
        return ExecuteScalarAsync(returnType, defaultValue, DefaultTimeout, sqlFormat, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the value in the first column 
    /// of the first row in the result set as type <paramref name="returnType"/>, substituting <paramref name="defaultValue"/>
    /// if <see cref="DBNull.Value"/> is retrieved.
    /// </summary>
    /// <param name="returnType">The type to which the result of the query should be converted.</param>
    /// <param name="defaultValue">The value to be substituted if <see cref="DBNull.Value"/> is retrieved.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public object? ExecuteScalar(Type returnType, object? defaultValue, int timeout, string sqlFormat, params object?[] parameters)
    {
        object? value = ExecuteScalar(timeout, sqlFormat, parameters);
        return ProcessTypedScalar(returnType, defaultValue, value);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the value in the first column 
    /// of the first row in the result set as type <paramref name="returnType"/>, substituting <paramref name="defaultValue"/>
    /// if <see cref="DBNull.Value"/> is retrieved.
    /// </summary>
    /// <param name="returnType">The type to which the result of the query should be converted.</param>
    /// <param name="defaultValue">The value to be substituted if <see cref="DBNull.Value"/> is retrieved.</param>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public async Task<object?> ExecuteScalarAsync(Type returnType, object? defaultValue, int timeout, string sqlFormat, CancellationToken cancellationToken, params object?[] parameters)
    {
        object? value = await ExecuteScalarAsync(timeout, sqlFormat, cancellationToken, parameters).ConfigureAwait(false);
        return ProcessTypedScalar(returnType, defaultValue, value);
    }

    private static object? ProcessTypedScalar(Type returnType, object? defaultValue, object? value)
    {
        // It's important that we do not validate the default value to determine
        // whether it is assignable to the return type because this method is
        // sometimes used to return null for value types in default value
        // expressions where nullable types are not supported
        if (value is null || value == DBNull.Value)
            return defaultValue;

        // Nullable types cannot be used in type conversion, but we can use Nullable.GetUnderlyingType()
        // to determine whether the type is nullable and convert to the underlying type instead
        Type type = Nullable.GetUnderlyingType(returnType) ?? returnType;

        // Handle Guids
        if (type == typeof(Guid))
            return System.Guid.Parse(value.ToString()!);

        // Handle string types that may have a converter function (e.g., Enums)
        if (value is string)
            return value.ToString().ConvertToType(type);

        // Handle native types
        if (value is IConvertible)
            return Convert.ChangeType(value, type);

        return value;
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the value in the first column 
    /// of the first row in the result set.
    /// </summary>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public object? ExecuteScalar(string sqlFormat, params object?[] parameters)
    {
        return ExecuteScalar(DefaultTimeout, sqlFormat, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the value in the first column 
    /// of the first row in the result set.
    /// </summary>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<object?> ExecuteScalarAsync(string sqlFormat, CancellationToken cancellationToken, params object?[] parameters)
    {
        return ExecuteScalarAsync(DefaultTimeout, sqlFormat, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the value in the first column 
    /// of the first row in the result set.
    /// </summary>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public object? ExecuteScalar(int timeout, string sqlFormat, params object?[] parameters)
    {
        string sql = GenericParameterizedQueryString(sqlFormat, parameters);
        return Connection.ExecuteScalar(timeout, sql, ResolveParameters(parameters));
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the value in the first column 
    /// of the first row in the result set.
    /// </summary>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>Value in the first column of the first row in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<object?> ExecuteScalarAsync(int timeout, string sqlFormat, CancellationToken cancellationToken, params object?[] parameters)
    {
        string sql = GenericParameterizedQueryString(sqlFormat, parameters);
        return Connection.ExecuteScalarAsync(timeout, sql, cancellationToken, ResolveParameters(parameters));
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the first <see cref="DataRow"/> in the result set.
    /// </summary>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>The first <see cref="DataRow"/> in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public DataRow RetrieveRow(string sqlFormat, params object?[] parameters)
    {
        return RetrieveRow(DefaultTimeout, sqlFormat, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the first <see cref="DataRow"/> in the result set.
    /// </summary>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>The first <see cref="DataRow"/> in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<DataRow> RetrieveRowAsync(string sqlFormat, CancellationToken cancellationToken, params object?[] parameters)
    {
        return RetrieveRowAsync(DefaultTimeout, sqlFormat, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the first <see cref="DataRow"/> in the result set.
    /// </summary>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>The first <see cref="DataRow"/> in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public DataRow RetrieveRow(int timeout, string sqlFormat, params object?[] parameters)
    {
        string sql = GenericParameterizedQueryString(sqlFormat, parameters);
        return Connection.RetrieveRow(timeout, sql, ResolveParameters(parameters));
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the first <see cref="DataRow"/> in the result set.
    /// </summary>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>The first <see cref="DataRow"/> in the result set.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<DataRow> RetrieveRowAsync(int timeout, string sqlFormat, CancellationToken cancellationToken, params object?[] parameters)
    {
        string sql = GenericParameterizedQueryString(sqlFormat, parameters);
        return Connection.RetrieveRowAsync(timeout, sql, cancellationToken, ResolveParameters(parameters));
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the first <see cref="DataTable"/> 
    /// of result set, if the result set contains at least one table.
    /// </summary>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>A <see cref="DataTable"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public DataTable RetrieveData(string sqlFormat, params object?[] parameters)
    {
        return RetrieveData(DefaultTimeout, sqlFormat, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the first <see cref="DataTable"/> 
    /// of result set, if the result set contains at least one table.
    /// </summary>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>A <see cref="DataTable"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<DataTable> RetrieveDataAsync(string sqlFormat, CancellationToken cancellationToken, params object?[] parameters)
    {
        return RetrieveDataAsync(DefaultTimeout, sqlFormat, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the first <see cref="DataTable"/>
    /// of result set, if the result set contains at least one table, as an asynchronous enumerable.
    /// </summary>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>An asynchronous enumerable of <see cref="DataRow"/> objects.</returns>
    public IAsyncEnumerable<DataRow> RetrieveDataAsAsyncEnumerable(string sqlFormat, CancellationToken cancellationToken, params object?[] parameters)
    {
        return RetrieveDataAsAsyncEnumerable(DefaultTimeout, sqlFormat, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the first <see cref="DataTable"/> 
    /// of result set, if the result set contains at least one table.
    /// </summary>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>A <see cref="DataTable"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public DataTable RetrieveData(int timeout, string sqlFormat, params object?[] parameters)
    {
        string sql = GenericParameterizedQueryString(sqlFormat, parameters);
        return Connection.RetrieveData(timeout, sql, ResolveParameters(parameters));
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the first <see cref="DataTable"/> 
    /// of result set, if the result set contains at least one table.
    /// </summary>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>A <see cref="DataTable"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<DataTable> RetrieveDataAsync(int timeout, string sqlFormat, CancellationToken cancellationToken, params object?[] parameters)
    {
        string sql = GenericParameterizedQueryString(sqlFormat, parameters);
        return Connection.RetrieveDataAsync(timeout, sql, cancellationToken, ResolveParameters(parameters));
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the first <see cref="DataTable"/>
    /// of result set, if the result set contains at least one table, as an asynchronous enumerable.
    /// </summary>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>An asynchronous enumerable of <see cref="DataRow"/> objects.</returns>
    public async IAsyncEnumerable<DataRow> RetrieveDataAsAsyncEnumerable(int timeout, string sqlFormat, [EnumeratorCancellation] CancellationToken cancellationToken, params object?[] parameters)
    {
        await foreach (DataRow row in (await RetrieveDataAsync(timeout, sqlFormat, cancellationToken, parameters).ConfigureAwait(false)).AsAwaitConfiguredCancelableAsyncEnumerable(cancellationToken))
            yield return row;
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the <see cref="DataSet"/> that 
    /// may contain multiple tables, depending on the SQL statement.
    /// </summary>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>A <see cref="DataSet"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public DataSet RetrieveDataSet(string sqlFormat, params object?[] parameters)
    {
        return RetrieveDataSet(DefaultTimeout, sqlFormat, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the <see cref="DataSet"/> that 
    /// may contain multiple tables, depending on the SQL statement.
    /// </summary>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>A <see cref="DataSet"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<DataSet> RetrieveDataSetAsync(string sqlFormat, CancellationToken cancellationToken, params object?[] parameters)
    {
        return RetrieveDataSetAsync(DefaultTimeout, sqlFormat, cancellationToken, parameters);
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the <see cref="DataSet"/> that 
    /// may contain multiple tables, depending on the SQL statement.
    /// </summary>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>A <see cref="DataSet"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public DataSet RetrieveDataSet(int timeout, string sqlFormat, params object?[] parameters)
    {
        string sql = GenericParameterizedQueryString(sqlFormat, parameters);
        return Connection.RetrieveDataSet(timeout, sql, ResolveParameters(parameters));
    }

    /// <summary>
    /// Executes the SQL statement using <see cref="Connection"/>, and returns the <see cref="DataSet"/> that 
    /// may contain multiple tables, depending on the SQL statement.
    /// </summary>
    /// <param name="timeout">The time in seconds to wait for the SQL statement to execute.</param>
    /// <param name="sqlFormat">Format string for the SQL statement to be executed.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">The parameter values to be used to fill in <see cref="DbParameter"/> parameters.</param>
    /// <returns>A <see cref="DataSet"/> object.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<DataSet> RetrieveDataSetAsync(int timeout, string sqlFormat, CancellationToken cancellationToken, params object?[] parameters)
    {
        string sql = GenericParameterizedQueryString(sqlFormat, parameters);
        return Connection.RetrieveDataSetAsync(timeout, sql, cancellationToken, ResolveParameters(parameters));
    }

    /// <summary>
    /// Releases all the resources used by the <see cref="AdoDataConnection"/> object.
    /// </summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Releases the unmanaged resources used by the <see cref="AdoDataConnection"/> object and optionally releases the managed resources.
    /// </summary>
    /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (m_disposed)
            return;

        try
        {
            if (!disposing)
                return;

            if (m_disposeConnection)
                Connection.Dispose();
        }
        finally
        {
            m_disposed = true; // Prevent duplicate dispose.
        }
    }

    /// <summary>
    /// Escapes an identifier, e.g., a table or field name, using the common delimiter for the connected <see cref="AdoDataConnection"/>
    /// database type or the standard ANSI escaping delimiter, i.e., double-quotes, based on the <paramref name="useAnsiQuotes"/> flag.
    /// </summary>
    /// <param name="identifier">Field name to escape.</param>
    /// <param name="useAnsiQuotes">Force use of double-quote for identifier delimiter, per SQL-99 standard, regardless of database type.</param>
    /// <returns>Escaped identifier name.</returns>
    /// <exception cref="ArgumentException"><paramref name="identifier"/> value cannot be null, empty or whitespace.</exception>
    public string EscapeIdentifier(string identifier, bool useAnsiQuotes = false)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new ArgumentException("Value cannot be null, empty or whitespace.", nameof(identifier));

        identifier = identifier.Trim();

        if (useAnsiQuotes)
            return $"\"{identifier}\"";

        return DatabaseType switch
        {
            DatabaseType.SQLServer or DatabaseType.Access => $"[{identifier}]",
            DatabaseType.MySQL => $"`{identifier}`",
            _ => $"\"{identifier}\""
        };
    }

    /// <summary>
    /// Returns proper <see cref="System.Boolean"/> implementation for connected <see cref="AdoDataConnection"/> database type.
    /// </summary>
    /// <param name="value"><see cref="System.Boolean"/> to format per database type.</param>
    /// <returns>Proper <see cref="System.Boolean"/> implementation for connected <see cref="AdoDataConnection"/> database type.</returns>
    public object Bool(bool value)
    {
        if (IsOracle || IsPostgreSQL)
            return value ? 1 : 0;

        return value;
    }

    /// <summary>
    /// Returns proper <see cref="System.Guid"/> implementation for connected <see cref="AdoDataConnection"/> database type.
    /// </summary>
    /// <param name="value"><see cref="System.Guid"/> to format per database type.</param>
    /// <returns>Proper <see cref="System.Guid"/> implementation for connected <see cref="AdoDataConnection"/> database type.</returns>
    public object Guid(Guid value)
    {
        if (IsJetEngine)
            return $"{{{value}}}";

        if (IsSqlite || IsOracle || IsPostgreSQL)
            return value.ToString().ToLower();

        //return "P" + guid.ToString();

        return value;
    }

    /// <summary>
    /// Retrieves <see cref="System.Guid"/> from a <see cref="DataRow"/> field based on database type.
    /// </summary>
    /// <param name="row"><see cref="DataRow"/> from which value needs to be retrieved.</param>
    /// <param name="fieldName">Name of the field which contains <see cref="System.Guid"/>.</param>
    /// <returns><see cref="System.Guid"/>.</returns>
    public Guid Guid(DataRow row, string fieldName)
    {
        if (IsSQLServer)
            return row.Field<Guid>(fieldName);

        return System.Guid.Parse(row.Field<object>(fieldName)?.ToString() ?? System.Guid.Empty.ToString());
    }

    /// <summary>
    /// Creates a parameterized query string for the underlying database type based on the given format string and parameter names.
    /// </summary>
    /// <param name="format">A composite format string.</param>
    /// <param name="parameterNames">A string array that contains zero or more parameter names to format.</param>
    /// <returns>A parameterized query string based on the given format and parameter names.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public string ParameterizedQueryString(string format, params string[] parameterNames)
    {
        char paramChar = IsOracle ? ':' : '@';
        object[] parameters = parameterNames.Select(name => paramChar + name).Cast<object>().ToArray();

        return string.Format(format, parameters);
    }

    private DatabaseType GetDatabaseType(Type connectionType)
    {
        DatabaseType type = DatabaseType.Other;

        switch (connectionType.Name.ToLowerInvariant())
        {
            case "sqlconnection":
                type = DatabaseType.SQLServer;

                break;
            case "mysqlconnection":
                type = DatabaseType.MySQL;

                break;
            case "oracleconnection":
                type = DatabaseType.Oracle;

                break;
            case "sqliteconnection":
                type = DatabaseType.SQLite;

                break;
            case "npgsqlconnection":
                type = DatabaseType.PostgreSQL;

                break;
            case "oledbconnection":
                if (Connection.ConnectionString.ToLowerInvariant().Contains("microsoft.jet.oledb"))
                    type = DatabaseType.Access;

                break;
        }

        return type;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string GenericParameterizedQueryString(string sqlFormat, object?[] parameters)
    {
        string[] parameterNames = parameters.Select((_, index) => $"p{index}").ToArray();
        return ParameterizedQueryString(sqlFormat, parameterNames);
    }

    private object[] ResolveParameters(object?[] parameters)
    {
        DbParameter[] dataParameters = new DbParameter[parameters.Length];

        if (parameters.Length <= 0)
            return dataParameters;

        using DbCommand command = Connection.CreateCommand();

        for (int i = 0; i < parameters.Length; i++)
        {
            object? value = parameters[i];
            DbType? type = null;

            if (value is DbParameter dataParameter)
            {
                type = dataParameter.DbType;
                value = dataParameter.Value;
            }

            value = value switch
            {
                null => DBNull.Value,
                bool boolVal => Bool(boolVal),
                Guid guidVal => Guid(guidVal),
                _ => value
            };

            DbParameter parameter = command.CreateParameter();

            if (type.HasValue)
                parameter.DbType = type.Value;

            parameter.ParameterName = $"@p{i}";
            parameter.Value = value;
            dataParameters[i] = parameter;
        }

        // ReSharper disable once CoVariantArrayConversion
        return dataParameters;
    }

    #endregion

    #region [ Static ]

    /// <summary>
    /// Generates a data provider string for the given connection type.
    /// </summary>
    /// <param name="connectionType">The type used to establish a connection to the database.</param>
    /// <returns>The data provider string for the given connection type.</returns>
    public static string ToDataProviderString(Type connectionType)
    {
        if (!typeof(DbConnection).IsAssignableFrom(connectionType))
            throw new ArgumentException($"Connection type must derived from the {nameof(DbConnection)} class", nameof(connectionType));

        Dictionary<string, string> settings = new()
        {
            ["AssemblyName"] = connectionType.Assembly.FullName!,
            ["ConnectionType"] = connectionType.FullName!
        };

        return settings.JoinKeyValuePairs();

    }

    /// <inheritdoc cref="IDefineSettings.DefineSettings" />
    public static void DefineSettings(Settings settings, string settingsCategory = Settings.SystemSettingsCategory)
    {
        dynamic section = settings[settingsCategory];

        section.ConnectionString = ("Data Source=.\\SQLEXPRESS; Initial Catalog=DatabaseName; Integrated Security=SSPI; Connect Timeout=5", "Configuration database connection string");
        section.DataProviderString = ("AssemblyName=Microsoft.Data.SqlClient; ConnectionType=Microsoft.Data.SqlClient.SqlConnection", "Configuration database ADO.NET data provider assembly type creation string");

        // Define example connection settings
        section = settings["ExampleConnectionSettings"];

        section.SqlServer_ConnectionString = ("Data Source=.\\SQLEXPRESS; Initial Catalog=DatabaseName; Integrated Security=SSPI; Connect Timeout=5", "Example SQL Server database connection string");
        section.SqlServer_DataProviderString = ("AssemblyName=Microsoft.Data.SqlClient; ConnectionType=Microsoft.Data.SqlClient.SqlConnection", "Example SQL Server database ADO.NET provider string");
        section.SQLite_ConnectionString = ("Data Source=Example.db; Version=3; FailIfMissing=True; Foreign Keys=True", "Example SQLite database connection string");
        section.SQLite_DataProviderString = ("AssemblyName=Microsoft.Data.Sqlite; ConnectionType=Microsoft.Data.Sqlite.SqliteConnection", "Example SQLite database ADO.NET provider string");
        section.MySQL_ConnectionString = ("Server=localhost; Database=DatabaseName; Uid=root; Pwd=password", "Example MySQL database connection string");
        section.MySQL_DataProviderString = ("AssemblyName=MySql.Data; ConnectionType=MySql.Data.MySqlClient.MySqlConnection", "Example MySQL database ADO.NET provider string");
        section.Oracle_ConnectionString = ("Data Source=//localhost:1521/DatabaseName; User Id=Username; Password=password", "Example Oracle database connection string");
        section.Oracle_DataProviderString = ("AssemblyName=Oracle.ManagedDataAccess; ConnectionType=Oracle.ManagedDataAccess.Client.OracleConnection", "Example Oracle database ADO.NET provider string");
        section.PostgreSQL_ConnectionString = ("Host=localhost; Database=DatabaseName; Username=postgres; Password=password", "Example PostgreSQL database connection string");
        section.PostgreSQL_DataProviderString = ("AssemblyName=Npgsql; ConnectionType=Npgsql.NpgsqlConnection", "Example PostgreSQL database ADO.NET provider string");
        section.OleDB_ConnectionString = ("Provider=Microsoft.Jet.OLEDB.4.0; Data Source=Example.mdb", "Example OleDB database connection string");
        section.OleDB_DataProviderString = ("AssemblyName=System.Data; ConnectionType=System.Data.OleDb.OleDbConnection", "Example OleDB database ADO.NET provider string");
        section.ODBC_ConnectionString = ("Driver={SQL Server}; Server=.\\SQLEXPRESS; Database=DatabaseName; Trusted_Connection=yes", "Example ODBC database connection string");
        section.ODBC_DataProviderString = ("AssemblyName=System.Data; ConnectionType=System.Data.Odbc.OdbcConnection", "Example ODBC database ADO.NET provider string");
    }

    #endregion
}

