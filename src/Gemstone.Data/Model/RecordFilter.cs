﻿//******************************************************************************************************
//  RecordFilter.cs - Gbtc
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
//  03/04/2024 - C. Lackner
//       Generated original version of source code.
//
//******************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using Gemstone.ArrayExtensions;
using Gemstone.Collections.CollectionExtensions;
using Gemstone.Reflection.MemberInfoExtensions;
using Gemstone.StringExtensions;
using Gemstone.Units;

namespace Gemstone.Data.Model
{
    /// <summary>
    /// Defines a filter that can be applied to queries.
    /// </summary>
    /// <remarks>
    /// For Backend Restrictions <see cref="RecordRestriction"/> should be used.
    /// This is inteded to be used for user initiated seraches and filters in the User Interface.
    /// </remarks>
    public class RecordFilter<T>: IRecordFilter where T : class, new ()
    {
        #region [ Members ]

        // Fields

        /// <summary>
        /// 
        /// </summary>
        private string m_operator;

        /// <summary>
        /// Gets or sets the Name of the field to be searched.
        /// </summary>
        public string FieldName { get; set; }

        /// <summary>
        /// Gets or sets the value to be searched for.
        /// </summary>
        public object? SearchParameter { get; set; }

        #endregion

        #region [ Constructors ]

        

        #endregion

        #region [ Properties ]

        /// <summary>
        /// Gets or sets the Operator to be used for the Search.
        /// </summary>
        /// <remarks>
        /// <para>The list of supported operators includes:</para>
        ///
        /// <list type="bullet">
        ///   <item>=</item>
        ///   <item><![CDATA[<>]]></item>
        ///   <item><![CDATA[<]]></item>
        ///   <item><![CDATA[>]]></item>
        ///   <item>IN</item>
        ///   <item>NOT IN</item>
        ///   <item>LIKE</item>
        ///   <item>NOT LIKE</item>
        ///   <item><![CDATA[<=]]></item>
        ///   <item><![CDATA[>=]]></item>
        /// </list>
        /// </remarks>
        /// <exception cref="NotSupportedException">Attempted to assign an operator that is not supported.</exception>
        public string Operator
        {
            get => m_operator;
            set
            {
                if (s_validOperators.Contains(value, StringComparer.OrdinalIgnoreCase))
                    m_operator = value;
                else
                    throw new NotSupportedException($"{value} is not a valid operator");
            }
        }

        #endregion

        #region [ Methods ]

        /// <summary>
        /// Generates a <see cref="RecordRestriction"/> that corresponds to this <see cref="RecordFilter"/>.
        /// </summary>
        public RecordRestriction GenerateRestriction()
        {

            // FieldName has to be in the C# Model
            IEnumerable<string> fields = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Where(property => property is { CanRead: true, CanWrite: true })
                    .Select(property => property.Name).ToArray();

            //property.TryGetAttribute(out SearchableAttribute? searchableAttribute);

            if (!fields.Contains(FieldName))
                throw new ArgumentException($"{FieldName} is not a valid field for {typeof(T).Name}");

            if (s_groupOperators.Contains(m_operator, StringComparer.OrdinalIgnoreCase))
            {
                if (SearchParameter is not Array)
                {
                    SearchParameter = new object[] { SearchParameter };
                }

                int nParameters = ((Array)SearchParameter).Length;

                string[] parameters = new string[nParameters];
                for (int i =0; i < nParameters; i++)
                {
                    parameters[i] = $"{{{i}}}";
                }
                return new RecordRestriction($"{FieldName} {m_operator} ({string.Join(',', parameters)})", SearchParameter);

            }

            return new RecordRestriction($"{FieldName} {m_operator} {{0}}", SearchParameter);
        }


        #endregion

        #region [ Static ]

        // Static Methods

        private static readonly string[] s_validOperators = { "=", "<>", "<", ">", "IN", "NOT IN", "LIKE", "NOT LIKE", "<=", ">=" };
        private static readonly string[] s_groupOperators = { "IN", "NOT IN" };
        #endregion
    }
}
