﻿//******************************************************************************************************
//  SearchExtensionAttribute.cs - Gbtc
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
//  03/05/2024 - C. Lackner
//       Generated original version of source code.
//
//******************************************************************************************************

using System;

namespace Gemstone.Data.Model
{
    /// <summary>
    /// Defines an attribute that marks methods that are used to transform <see cref="IRecordFilter"/> into <see cref="RecordRestriction"/>.
    /// </summary>
    [AttributeUsage(AttributeTargets.Method)]
    public sealed class SearchExtensionAttribute(string fieldMatch) : Attribute
    {
        /// <summary>
        /// The string used to match FieldNames this applies to.
        /// </summary>
        public string FieldMatch { get; } = fieldMatch;
    }
}
