//******************************************************************************************************
//  ValueLabel.cs - Gbtc
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
//  10/30/2024 - C. Lackner
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
/// Defines a common model to hold a Label and associated Value.
/// </summary>
public class ValueLabel
{
    #region [ Properties ]

    /// <summary>
    /// gets or sets the Value Property.
    /// </summary>
    public string Value { get; set; } = string.Empty;

    /// <summary>
    /// gets or sets the Label Property.
    /// </summary>
    public string Label { get; set; } = string.Empty;

    #endregion

}
