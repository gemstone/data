//******************************************************************************************************
//  EventTypeAttribute.cs - Gbtc
//
//  Copyright © 2025, Grid Protection Alliance.  All Rights Reserved.
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
//  10/18/2025 - C. Lackner
//       Generated original version of source code.
//
//******************************************************************************************************

using System;

namespace Gemstone.Data.Model;

/// <summary>
/// Defines an attribute that will mark a class as a JSON construct in <see cref="EventDetails"/>
/// the property name.
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public sealed class EventTypeAttribute : Attribute
{
    /// <summary>
    /// Gets the EventType name to use in the database.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Creates a new <see cref="EventTypeAttribute"/>.
    /// </summary>
    /// <param name="name">Name to use for database entry.</param>
    public EventTypeAttribute(string name)
    {
        Name = name;
    }
}
