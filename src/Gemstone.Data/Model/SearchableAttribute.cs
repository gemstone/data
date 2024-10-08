﻿//******************************************************************************************************
//  SearchableAttribute.cs - Gbtc
//
//  Copyright © 2016, Grid Protection Alliance.  All Rights Reserved.
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
//  08/20/2016 - J. Ritchie Carroll
//       Generated original version of source code.
//  12/13/2019 - J. Ritchie Carroll
//       Migrated to Gemstone libraries.
//
//******************************************************************************************************

using System;

namespace Gemstone.Data.Model;

/// <summary>
/// Defines an attribute that will mark additional fields in the database as searchable field.
/// </summary>
/// <remarks>
/// All modeled fields are automatically searchable so this only applies to fields that are not modeled.
/// </remarks>
[AttributeUsage(AttributeTargets.Class)]
public sealed class SearchableAttribute(params string[] fields) : Attribute
{
    /// <summary>
    /// The field names that are searchable.
    /// </summary>
    public string[] FieldNames { get; } = fields;
}
