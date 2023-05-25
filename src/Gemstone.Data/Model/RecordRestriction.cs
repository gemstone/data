//******************************************************************************************************
//  RecordRestriction.cs - Gbtc
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
//  03/13/2016 - J. Ritchie Carroll
//       Generated original version of source code.
//  12/13/2019 - J. Ritchie Carroll
//      Migrated to Gemstone libraries.
//
//******************************************************************************************************

using System;
using System.Data;
using System.Linq;
using System.Runtime.CompilerServices;
using Gemstone.ArrayExtensions;
using Gemstone.Collections.CollectionExtensions;

namespace Gemstone.Data.Model
{
    /// <summary>
    /// Defines a parameterized record restriction that can be applied to queries.
    /// </summary>
    /// <remarks>
    /// For versatility, values in the <see cref="Parameters"/> array are mutable, however, this makes the
    /// array vulnerable to unintended updates for long-lived instances. Consequently, the normal use-case
    /// of record restriction instances should be considered temporal. If an instance needs to be cached,
    /// consider use of the <see cref="Clone()"/> function to reduce risk of unintended array updates.
    /// </remarks>
    public class RecordRestriction : IEquatable<RecordRestriction>
    {
        #region [ Members ]

        // Fields

        /// <summary>
        /// Defines filter SQL expression for restriction as a composite format string - does not
        /// include WHERE. When escaping is needed for field names, use standard ANSI quotes.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Each indexed parameter, e.g., "{0}", in the composite format string will be converted into
        /// query parameters where each of the corresponding values in the <see cref="Parameters"/>
        /// collection will be applied as <see cref="IDbDataParameter"/> values to an executed
        /// <see cref="IDbCommand"/> query.
        /// </para>
        /// <para>
        /// If needed, field names that are escaped with standard ANSI quotes in the filter expression
        /// will be updated to reflect what is defined in the user model.
        /// </para>
        /// </remarks>
        public readonly string? FilterExpression;

        /// <summary>
        /// Defines restriction parameter values.
        /// </summary>
        /// <remarks>
        /// If any of the <see cref="Parameters"/> reference a table field that is modeled with either an
        /// <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
        /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the
        /// target parameter with the returned value so that the field value will be properly set prior
        /// to executing any database function.
        /// </remarks>
        public readonly object?[] Parameters;

        #endregion

        #region [ Constructors ]

        /// <summary>
        /// Creates a new parameterized <see cref="RecordRestriction"/> with the specified SQL filter
        /// expression and parameters.
        /// </summary>
        /// <param name="filterExpression">
        /// Filter SQL expression for restriction as a composite format string - does not include WHERE.
        /// When escaping is needed for field names, use standard ANSI quotes.
        /// </param>
        /// <param name="parameters">Restriction parameter values.</param>
        /// <remarks>
        /// <para>
        /// Each indexed parameter, e.g., "{0}", in the composite format <paramref name="filterExpression"/>
        /// will be converted into query parameters where each of the corresponding values in the
        /// <paramref name="parameters"/> collection will be applied as <see cref="IDbDataParameter"/>
        /// values to an executed <see cref="IDbCommand"/> query.
        /// </para>
        /// <para>
        /// If any of the <paramref name="parameters"/> reference a table field that is modeled with either
        /// an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
        /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the
        /// target parameter with the returned value so that the field value will be properly set prior to
        /// executing any database function.
        /// </para>
        /// <para>
        /// If needed, field names that are escaped with standard ANSI quotes in the filter expression
        /// will be updated to reflect what is defined in the user model.
        /// </para>
        /// </remarks>
        public RecordRestriction(string? filterExpression, params object?[]? parameters)
        {
            FilterExpression = filterExpression;
            Parameters = parameters ?? Array.Empty<object>();
        }

        #endregion

        #region [ Properties ]

        /// <summary>
        /// Gets or sets <see cref="Parameters"/> field value for the specified <paramref name="index"/>.
        /// </summary>
        /// <param name="index">Index into <see cref="Parameters"/> field array.</param>
        /// <returns><see cref="Parameters"/> field value for the specified <paramref name="index"/>.</returns>
        public object? this[int index]
        {
            get => Parameters[index];
            set => Parameters[index] = value;
        }

        #endregion

        #region [ Methods ]

        /// <summary>
        /// Creates a deep copy of this record restriction.
        /// </summary>
        /// <returns>Deep copy of this record restriction.</returns>
        public RecordRestriction Clone() => Clone(this)!;

        /// <summary>
        /// Determines whether the specified object is equal to the current object.
        /// </summary>
        /// <param name="obj">An object to compare with this object.</param>
        /// <returns><c>true</c> if the current object is equal to the <paramref name="obj" /> parameter; otherwise, <c>false</c>.</returns>
        public override bool Equals(object? obj) => obj is RecordRestriction other && Equals(other);

        /// <summary>
        /// Indicates whether the current object is equal to another object of the same type.
        /// </summary>
        /// <param name="other">An object to compare with this object.</param>
        /// <returns><c>true</c> if the current object is equal to the <paramref name="other" /> parameter; otherwise, <c>false</c>.</returns>
        public bool Equals(RecordRestriction? other)
        {
            if (other is null)
                return false;

            if (ReferenceEquals(this, other))
                return true;

            if (string.Equals(FilterExpression, other.FilterExpression, StringComparison.Ordinal))
                return Parameters.CompareTo(other.Parameters) == 0;

            return false;
        }

        /// <summary>
        /// Serves as the default hash function.
        /// </summary>
        /// <returns>A hash code for the current <see cref="RecordRestriction"/>.</returns>
        public override int GetHashCode()
        {
            unchecked
            {
                return ((FilterExpression?.GetHashCode() ?? 0) * 397) ^ (Parameters?.GetHashCode() ?? 0);
            }
        }

        #endregion

        #region [ Operators ]

        /// <summary>
        /// Implicitly converts a <see cref="string"/> based filter expression into a <see cref="RecordRestriction"/>.
        /// </summary>
        /// <param name="value">Operand.</param>
        /// <returns>Record operation representing the specified filter expression.</returns>
        public static implicit operator RecordRestriction(string? value) => new(value);

        /// <summary>
        /// Compares to record restrictions for equality.
        /// </summary>
        /// <param name="left"><see cref="RecordRestriction"/> left operand.</param>
        /// <param name="right"><see cref="RecordRestriction"/> right operand.</param>
        /// <returns><c>true</c> if record restrictions are equal; otherwise, <c>false</c>.</returns>
        public static bool operator ==(RecordRestriction? left, RecordRestriction? right)
        {
            if (left is null && right is null)
                return true;

            return left?.Equals(right) ?? false;
        }

        /// <summary>
        /// Compares to record restrictions for inequality.
        /// </summary>
        /// <param name="left"><see cref="RecordRestriction"/> left operand.</param>
        /// <param name="right"><see cref="RecordRestriction"/> right operand.</param>
        /// <returns><c>true</c> if record restrictions are not equal; otherwise, <c>false</c>.</returns>
        public static bool operator !=(RecordRestriction? left, RecordRestriction? right)
        {
            if (left is null && right is null)
                return false;

            return !left?.Equals(right) ?? true;
        }

        /// <summary>
        /// Combines two record restrictions with an AND condition.
        /// </summary>
        /// <param name="left"><see cref="RecordRestriction"/> left operand.</param>
        /// <param name="right"><see cref="RecordRestriction"/> right operand.</param>
        /// <returns>New combined record restriction.</returns>
        /// <remarks>
        /// If both parameters are <c>null</c>, result will be <c>null</c>. If one parameter is <c>null</c>
        /// and the other parameter is not, the non-null parameter will be returned. Equally, if the
        /// <see cref="FilterExpression"/> of both parameters are <c>null</c>, empty or only whitespace,
        /// then the result will be <c>null</c>. If one parameter has a filter expression that is
        /// <c>null</c>, empty or whitespace and the other parameter's filter expression is defined, then
        /// the parameter that has a filter expression will be returned.
        /// </remarks>
        public static RecordRestriction? operator +(RecordRestriction? left, RecordRestriction? right) => CombineAnd(left, right);

        /// <summary>
        /// Combines two record restrictions with an AND condition.
        /// </summary>
        /// <param name="left"><see cref="RecordRestriction"/> left operand.</param>
        /// <param name="right"><see cref="RecordRestriction"/> right operand.</param>
        /// <returns>New combined record restriction.</returns>
        /// <remarks>
        /// If both parameters are <c>null</c>, result will be <c>null</c>. If one parameter is <c>null</c>
        /// and the other parameter is not, the non-null parameter will be returned. Equally, if the
        /// <see cref="FilterExpression"/> of both parameters are <c>null</c>, empty or only whitespace,
        /// then the result will be <c>null</c>. If one parameter has a filter expression that is
        /// <c>null</c>, empty or whitespace and the other parameter's filter expression is defined, then
        /// the parameter that has a filter expression will be returned.
        /// </remarks>
        public static RecordRestriction? operator &(RecordRestriction? left, RecordRestriction? right) => CombineAnd(left, right);

        /// <summary>
        /// Combines two record restrictions with an OR condition.
        /// </summary>
        /// <param name="left"><see cref="RecordRestriction"/> left operand.</param>
        /// <param name="right"><see cref="RecordRestriction"/> right operand.</param>
        /// <returns>New combined record restriction.</returns>
        /// <remarks>
        /// If both parameters are <c>null</c>, result will be <c>null</c>. If one parameter is <c>null</c>
        /// and the other parameter is not, the non-null parameter will be returned. Equally, if the
        /// <see cref="FilterExpression"/> of both parameters are <c>null</c>, empty or only whitespace,
        /// then the result will be <c>null</c>. If one parameter has a filter expression that is
        /// <c>null</c>, empty or whitespace and the other parameter's filter expression is defined, then
        /// the parameter that has a filter expression will be returned.
        /// </remarks>
        public static RecordRestriction? operator |(RecordRestriction? left, RecordRestriction? right) => CombineOr(left, right);

        #endregion

        #region [ Static ]

        // Static Methods

        /// <summary>
        /// Creates a deep copy of the <paramref name="source"/> record restriction.
        /// </summary>
        /// <param name="source">Record restriction to clone.</param>
        /// <returns>Deep copy of the <paramref name="source"/> record restriction.</returns>
        public static RecordRestriction? Clone(RecordRestriction? source)
        {
            if (source is null)
                return default;

            object?[] parameters = source.Parameters;

            if (parameters.Length > 0)
            {
                parameters = new object[source.Parameters.Length];
                Array.Copy(source.Parameters, parameters, source.Parameters.Length);
            }

            return new RecordRestriction(source.FilterExpression, parameters);
        }

        /// <summary>
        /// Combines two record restrictions with an AND condition.
        /// </summary>
        /// <param name="left">Left operand.</param>
        /// <param name="right">Right operand.</param>
        /// <returns>New combined record restriction.</returns>
        /// <remarks>
        /// If both parameters are <c>null</c>, result will be <c>null</c>. If one parameter is <c>null</c>
        /// and the other parameter is not, the non-null parameter will be returned. Equally, if the
        /// <see cref="FilterExpression"/> of both parameters are <c>null</c>, empty or only whitespace,
        /// then the result will be <c>null</c>. If one parameter has a filter expression that is
        /// <c>null</c>, empty or whitespace and the other parameter's filter expression is defined, then
        /// the parameter that has a filter expression will be returned.
        /// </remarks>
        public static RecordRestriction? CombineAnd(RecordRestriction? left, RecordRestriction? right) => Combine(left, right, "AND");

        /// <summary>
        /// Combines two record restrictions with an OR condition.
        /// </summary>
        /// <param name="left">Left operand.</param>
        /// <param name="right">Right operand.</param>
        /// <returns>New combined record restriction.</returns>
        /// <remarks>
        /// If both parameters are <c>null</c>, result will be <c>null</c>. If one parameter is <c>null</c>
        /// and the other parameter is not, the non-null parameter will be returned. Equally, if the
        /// <see cref="FilterExpression"/> of both parameters are <c>null</c>, empty or only whitespace,
        /// then the result will be <c>null</c>. If one parameter has a filter expression that is
        /// <c>null</c>, empty or whitespace and the other parameter's filter expression is defined, then
        /// the parameter that has a filter expression will be returned.
        /// </remarks>
        public static RecordRestriction? CombineOr(RecordRestriction? left, RecordRestriction? right) => Combine(left, right, "OR");

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static RecordRestriction? Combine(RecordRestriction? left, RecordRestriction? right, string operation)
        {
            // Check for null operands
            if (left is null && right is null)
                return default;

            if (left is null)
                return right;

            if (right is null)
                return left;

            // Check for empty filter expressions
            bool leftEmpty = string.IsNullOrWhiteSpace(left.FilterExpression);
            bool rightEmpty = string.IsNullOrWhiteSpace(right.FilterExpression);

            if (leftEmpty && rightEmpty)
                return null;

            if (leftEmpty)
                return right;

            if (rightEmpty)
                return left;

            // Check for missing parameters
            int leftLength = left.Parameters.Length;
            int rightLength = right.Parameters.Length;

            if (leftLength == 0 && rightLength == 0)
                return new RecordRestriction($"({left.FilterExpression}) {operation} ({right.FilterExpression})");

            object?[] parameters = leftLength == 0 ? right.Parameters : rightLength == 0 ? left.Parameters : left.Parameters.Combine(right.Parameters);

            // ReSharper disable once CoVariantArrayConversion
            object[] offsetArgs = Enumerable.Range(leftLength, rightLength).Select(index => $"{{{index}}}").ToArray();

            return new RecordRestriction($"({left.FilterExpression}) {operation} ({string.Format(right.FilterExpression, offsetArgs)})", parameters);
        }

        #endregion
    }
}
