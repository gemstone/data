//******************************************************************************************************
//  Settings.cs - Gbtc
//
//  Copyright © 2020, Grid Protection Alliance.  All Rights Reserved.
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
//  10/10/2020 - J. Ritchie Carroll
//       Generated original version of source code.
//
//******************************************************************************************************

namespace Gemstone.Data;

///// <summary>
///// Defines data specific system settings for an application.
///// </summary>
//public class Settings : Configuration.Settings
//{
//    /// <summary>
//    /// Default value for <see cref="ConnectionString"/> property.
//    /// </summary>
//    public const string DefaultConnectionString = "Data Source=localhost; Initial Catalog=gemstone; Integrated Security=SSPI";

//    /// <summary>
//    /// Default value for <see cref="DataProviderString"/> property.
//    /// </summary>
//    public const string DefaultDataProviderString = "AssemblyName=Microsoft.Data.SqlClient; ConnectionType=Microsoft.Data.SqlClient.SqlConnection";

//    /// <summary>
//    /// Gets or sets the connection string used to connect to the data source.
//    /// </summary>
//    public string ConnectionString { get; set; } = DefaultConnectionString;

//    /// <summary>
//    /// Gets or sets the data provider string used to connect to the data source.
//    /// </summary>
//    public string DataProviderString { get; set; } = DefaultDataProviderString;

//    /// <inheritdoc/>
//    public override void Initialize(IConfiguration configuration)
//    {
//        base.Initialize(configuration);

//        IConfigurationSection settings = Configuration.GetSection(SystemSettings);

//        ConnectionString = settings[nameof(ConnectionString)].ToNonNullNorWhiteSpace(DefaultConnectionString);
//        DataProviderString = settings[nameof(DataProviderString)].ToNonNullNorWhiteSpace(DefaultDataProviderString);
//    }

//    /// <summary>
//    /// Configures the <see cref="IAppSettingsBuilder"/> for <see cref="Settings"/>.
//    /// </summary>
//    /// <param name="builder">Builder used to configure settings.</param>
//    public virtual void ConfigureAppSettings(IAppSettingsBuilder builder)
//    {
//        builder.Add($"{SystemSettings}:{nameof(ConnectionString)}", DefaultConnectionString, "Defines the connection string used to connect to the data source.");
//        builder.Add($"{SystemSettings}:{nameof(DataProviderString)}", DefaultDataProviderString, "Defines the data provider string used to connect to the data source.");

//        SwitchMappings[$"--{nameof(ConnectionString)}"] = $"{SystemSettings}:{nameof(ConnectionString)}";
//        SwitchMappings[$"--{nameof(DataProviderString)}"] = $"{SystemSettings}:{nameof(DataProviderString)}";
//        SwitchMappings["-c"] = $"{SystemSettings}:{nameof(ConnectionString)}";
//        SwitchMappings["-p"] = $"{SystemSettings}:{nameof(DataProviderString)}";
//    }

//    /// <summary>
//    /// Gets the default instance of <see cref="Settings"/>.
//    /// </summary>
//    public new static Settings Instance => (Settings)Gemstone.Configuration.Settings.Instance;
//}
