﻿//******************************************************************************************************
//  EncryptDataAttribute.cs - Gbtc
//
//  Copyright © 2018, Grid Protection Alliance.  All Rights Reserved.
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
//  07/30/2018 - J. Ritchie Carroll
//       Generated original version of source code.
//  12/13/2019 - J. Ritchie Carroll
//      Migrated to Gemstone libraries.
//
//******************************************************************************************************

using System;

namespace Gemstone.Data.Model
{
    /// <summary>
    /// Defines an attribute that provides encryption of text field contents for a modeled table.
    /// </summary>
    /// <remarks>
    /// Application of attribute is only valid on <see cref="string"/> properties; attribute will be
    /// ignored if applied to properties of other types.
    /// </remarks>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class EncryptDataAttribute : Attribute
    {
        /// <summary>
        /// Default key reference value.
        /// </summary>
        public const string DefaultKeyReference = "DefaultTableOperationsKey";

        /// <summary>
        /// Gets reference name used to lookup encryption key and initialization vector;
        /// new keys will be created for reference if it does not exist.
        /// </summary>
        public string KeyReference { get; }

        /// <summary>
        /// Creates a new <see cref="EncryptDataAttribute"/>.
        /// </summary>
        public EncryptDataAttribute()
        {
            KeyReference = DefaultKeyReference;
        }

        /// <summary>
        /// Creates a new <see cref="EncryptDataAttribute"/> with a specified <paramref name="keyReference"/> value.
        /// </summary>
        /// <param name="keyReference">Reference name used to lookup encryption key and initialization vector.</param>
        public EncryptDataAttribute(string keyReference)
        {
            if (string.IsNullOrWhiteSpace(keyReference))
                throw new ArgumentNullException(nameof(keyReference), "Key reference cannot be null, empty or whitespace.");

            KeyReference = keyReference;
        }
    }
}