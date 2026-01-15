//******************************************************************************************************
//  ClaimQueryRestrictionAttribute.cs - Gbtc
//
//  Copyright © 2026, Grid Protection Alliance.  All Rights Reserved.
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
//  01/09/2026 - J. Ritchie Carroll
//       Generated original version of source code.
//
//******************************************************************************************************

using System;
using System.Data;

namespace Gemstone.Data.Model;

/// <summary>
/// Defines an attribute that will mark a modeled table static function as a method to create a <see cref="RecordRestriction"/>
/// to secure query functions for a modeled <see cref="SecureTableOperations{T}"/>.
/// </summary>
/// <remarks>The static function should be a part of the modeled class, and have the footprint 
/// <see cref="RecordRestriction"/> MethodName(<see langword="params"/> <see cref="object"/>[])</remarks>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class ClaimRestrictionAttribute : Attribute
{

    /// <summary>
    /// Defines claims which will be checked for parameter values
    /// </summary>
    public readonly string[] Claims;

    /// <summary>
    /// Creates a new parameterized <see cref="ClaimRestrictionAttribute"/> with the specified claims.
    /// </summary>
    /// <param name="claims">Claims to use for parameter values. The order of parameters is in the same order as claims, grabbing as many values as each claim provides</param>
    public ClaimRestrictionAttribute(params string[] claims)
    {
        Claims = claims ?? throw new ArgumentNullException(nameof(claims));
    }
}
