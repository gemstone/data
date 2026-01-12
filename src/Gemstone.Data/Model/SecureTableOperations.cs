//******************************************************************************************************
//  SecureTableOperations.cs - Gbtc
//
//  Copyright © 2016, Grid Protection Alliance.  All Rights Reserved.
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
//  02/01/2016 - G. Santos
//       Generated original version of source code.
//
//******************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Claims;
using System.Threading;
using System.Threading.Tasks;
using Gemstone.Expressions.Model;
using Gemstone.Reflection.MemberInfoExtensions;

namespace Gemstone.Data.Model;

/// <summary>
/// A wrapper class for <see cref="TableOperations{T}"/>
/// </summary>
/// <typeparam name="T">Modeled table.</typeparam>
public class SecureTableOperations<T> where T : class, new()
{
    /// <summary>
    /// <see cref="TableOperations{T}"/> which performs DB operations.
    /// </summary>
    public TableOperations<T> BaseOperations { get; }

    /// <summary>
    /// Creates a new <see cref="SecureTableOperations{T}"/>
    /// </summary>
    /// <param name="operations"><see cref="TableOperations{T}"/> table operation which to wrap calls of.</param>
    public SecureTableOperations(TableOperations<T> operations)
    {
        BaseOperations = operations;
    }

    /// <summary>
    /// Creates a new <see cref="SecureTableOperations{T}"/>
    /// </summary>
    /// <param name="connection"><see cref="AdoDataConnection"/> db to create secure operations to.</param>
    public SecureTableOperations(AdoDataConnection connection)
    {
        BaseOperations = new(connection);
    }

    #region [ Methods ]

    /// <summary>
    /// Transforms a <see cref="ClaimsPrincipal"/> into an equivalent <see cref="RecordRestriction"/>, as defined by the model's <see cref="ClaimQueryRestrictionAttribute"/>.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <returns><see cref="RecordRestriction"/></returns>
    /// <exception cref="InvalidOperationException"></exception>
    private static RecordRestriction? GetClaimRecordRestriction(ClaimsPrincipal principal)
    {
        if (s_claimQueryRestrictionAttribute is null)
            return null;
        
        object[] claims = s_claimQueryRestrictionAttribute.Claims
            .Select(claimKey => principal.FindFirst(claimKey) ?? throw new InvalidOperationException($"Unable to retrieve {claimKey} claim from user."))
            .Select(claim => claim.Value)
            .ToArray();

        return new RecordRestriction(s_claimQueryRestrictionAttribute.FilterExpression, claims);
    }

    /// <summary>
    /// Queries database and returns a single modeled table record for the specified <paramref name="restriction"/>.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="restriction">Record restriction to apply.</param>
    /// <returns>A single modeled table record for the queried record.</returns>
    /// <remarks>
    /// <para>
    /// If no record is found for specified <paramref name="restriction"/>, <c>null</c> will be returned.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="TableOperations{T}.QueryRecords(string, RecordRestriction, int)"/>
    /// specifying the <see cref="RecordRestriction"/> parameter with a limit of 1 record.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// </remarks>
    public T? QueryRecord(ClaimsPrincipal principal, RecordRestriction? restriction) =>
        BaseOperations.QueryRecord(restriction + GetClaimRecordRestriction(principal));

    /// <summary>
    /// Queries database and returns a single modeled table record for the specified <paramref name="restriction"/>.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="restriction">Record restriction to apply.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <returns>A single modeled table record for the queried record.</returns>
    /// <remarks>
    /// <para>
    /// If no record is found for specified <paramref name="restriction"/>, <c>null</c> will be returned.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="TableOperations{T}.QueryRecords(string, RecordRestriction, int)"/>
    /// specifying the <see cref="RecordRestriction"/> parameter with a limit of 1 record.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// </remarks>
    public ValueTask<T?> QueryRecordAsync(ClaimsPrincipal principal, RecordRestriction? restriction, CancellationToken cancellationToken) =>
        BaseOperations.QueryRecordAsync(restriction + GetClaimRecordRestriction(principal), cancellationToken);

    /// <summary>
    /// Queries database and returns a single modeled table record for the specified <paramref name="restriction"/>,
    /// execution of query will apply <paramref name="orderByExpression"/>.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="orderByExpression">Field name expression used for sort order, include ASC or DESC as needed - does not include ORDER BY; defaults to primary keys.</param>
    /// <param name="restriction">Record restriction to apply.</param>
    /// <returns>A single modeled table record for the queried record.</returns>
    /// <remarks>
    /// <para>
    /// If no record is found for specified <paramref name="restriction"/>, <c>null</c> will be returned.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="TableOperations{T}.QueryRecords(string, RecordRestriction, int)"/>
    /// specifying the <see cref="RecordRestriction"/> parameter with a limit of 1 record.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// </remarks>
    public T? QueryRecord(ClaimsPrincipal principal, string? orderByExpression, RecordRestriction? restriction) =>
        BaseOperations.QueryRecord(orderByExpression, restriction + GetClaimRecordRestriction(principal));

    /// <summary>
    /// Queries database and returns a single modeled table record for the specified <paramref name="restriction"/>,
    /// execution of query will apply <paramref name="orderByExpression"/>.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="orderByExpression">Field name expression used for sort order, include ASC or DESC as needed - does not include ORDER BY; defaults to primary keys.</param>
    /// <param name="restriction">Record restriction to apply.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <returns>A single modeled table record for the queried record.</returns>
    /// <remarks>
    /// <para>
    /// If no record is found for specified <paramref name="restriction"/>, <c>null</c> will be returned.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="TableOperations{T}.QueryRecords(string, RecordRestriction, int)"/>
    /// specifying the <see cref="RecordRestriction"/> parameter with a limit of 1 record.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// </remarks>
    public ValueTask<T?> QueryRecordAsync(ClaimsPrincipal principal, string? orderByExpression, RecordRestriction? restriction, CancellationToken cancellationToken) =>
        BaseOperations.QueryRecordAsync(orderByExpression, restriction + GetClaimRecordRestriction(principal), cancellationToken);

    /// <summary>
    /// Queries database and returns a single modeled table record for the specified SQL filter
    /// expression and parameters.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="filterExpression">
    /// Filter SQL expression for restriction as a composite format string - does not include WHERE.
    /// When escaping is needed for field names, use standard ANSI quotes.
    /// </param>
    /// <param name="parameters">Restriction parameter values.</param>
    /// <returns>A single modeled table record for the queried record.</returns>
    /// <remarks>
    /// <para>
    /// If no record is found for specified filter expression and parameters, <c>null</c> will be returned.
    /// </para>
    /// <para>
    /// Each indexed parameter, e.g., "{0}", in the composite format <paramref name="filterExpression"/>
    /// will be converted into query parameters where each of the corresponding values in the
    /// <paramref name="parameters"/> collection will be applied as <see cref="IDbDataParameter"/>
    /// values to an executed <see cref="IDbCommand"/> query.
    /// </para>
    /// <para>
    /// If any of the specified <paramref name="parameters"/> reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// <para>
    /// If needed, field names that are escaped with standard ANSI quotes in the filter expression
    /// will be updated to reflect what is defined in the user model.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="TableOperations{T}.QueryRecords(string, RecordRestriction, int)"/>
    /// specifying the <see cref="RecordRestriction"/> parameter with a limit of 1 record.
    /// </para>
    /// </remarks>
    public T? QueryRecordWhere(ClaimsPrincipal principal, string? filterExpression, params object?[] parameters) =>
        QueryRecord(principal, new RecordRestriction(filterExpression, parameters));

    /// <summary>
    /// Queries database and returns a single modeled table record for the specified SQL filter
    /// expression and parameters.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="filterExpression">
    /// Filter SQL expression for restriction as a composite format string - does not include WHERE.
    /// When escaping is needed for field names, use standard ANSI quotes.
    /// </param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">Restriction parameter values.</param>
    /// <returns>A single modeled table record for the queried record.</returns>
    /// <remarks>
    /// <para>
    /// If no record is found for specified filter expression and parameters, <c>null</c> will be returned.
    /// </para>
    /// <para>
    /// Each indexed parameter, e.g., "{0}", in the composite format <paramref name="filterExpression"/>
    /// will be converted into query parameters where each of the corresponding values in the
    /// <paramref name="parameters"/> collection will be applied as <see cref="IDbDataParameter"/>
    /// values to an executed <see cref="IDbCommand"/> query.
    /// </para>
    /// <para>
    /// If any of the specified <paramref name="parameters"/> reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// <para>
    /// If needed, field names that are escaped with standard ANSI quotes in the filter expression
    /// will be updated to reflect what is defined in the user model.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="TableOperations{T}.QueryRecords(string, RecordRestriction, int)"/>
    /// specifying the <see cref="RecordRestriction"/> parameter with a limit of 1 record.
    /// </para>
    /// </remarks>
    public ValueTask<T?> QueryRecordWhereAsync(ClaimsPrincipal principal, string? filterExpression, CancellationToken cancellationToken, params object?[] parameters) =>
        QueryRecordAsync(principal, new RecordRestriction(filterExpression, parameters), cancellationToken);

    /// <summary>
    /// Queries database and returns modeled table records for the specified parameters.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="orderByExpression">Field name expression used for sort order, include ASC or DESC as needed - does not include ORDER BY; defaults to primary keys.</param>
    /// <param name="restriction">Record restriction to apply, if any.</param>
    /// <param name="limit">Limit of number of record to return.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// If no record <paramref name="restriction"/> or <paramref name="limit"/> is provided, all rows will be returned.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// </remarks>
    public IEnumerable<T?> QueryRecords(ClaimsPrincipal principal, string? orderByExpression = null, RecordRestriction? restriction = null, int limit = -1) =>
        BaseOperations.QueryRecords(orderByExpression, restriction + GetClaimRecordRestriction(principal), limit);

    /// <summary>
    /// Queries database and returns modeled table records for the specified parameters.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="orderByExpression">Field name expression used for sort order, include ASC or DESC as needed - does not include ORDER BY; defaults to primary keys.</param>
    /// <param name="restriction">Record restriction to apply, if any.</param>
    /// <param name="limit">Limit of number of record to return.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// If no record <paramref name="restriction"/> or <paramref name="limit"/> is provided, all rows will be returned.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// </remarks>
    public IAsyncEnumerable<T?> QueryRecordsAsync(ClaimsPrincipal principal, string? orderByExpression = null, RecordRestriction? restriction = null, int limit = -1, CancellationToken cancellationToken = default) =>
        BaseOperations.QueryRecordsAsync(orderByExpression, restriction + GetClaimRecordRestriction(principal), limit, cancellationToken);

    /// <summary>
    /// Queries database and returns modeled table records for the specified <paramref name="restriction"/>.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="restriction">Record restriction to apply.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// This is a convenience call to <see cref="TableOperations{T}.QueryRecords(string, RecordRestriction, int)"/> only
    /// specifying the <see cref="RecordRestriction"/> parameter.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// </remarks>
    public IEnumerable<T?> QueryRecords(ClaimsPrincipal principal, RecordRestriction? restriction) =>
        BaseOperations.QueryRecords(restriction + GetClaimRecordRestriction(principal));

    /// <summary>
    /// Queries database and returns modeled table records for the specified <paramref name="restriction"/>.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="restriction">Record restriction to apply.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// This is a convenience call to <see cref="TableOperations{T}.QueryRecords(string, RecordRestriction, int)"/> only
    /// specifying the <see cref="RecordRestriction"/> parameter.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// </remarks>
    public IAsyncEnumerable<T?> QueryRecordsAsync(ClaimsPrincipal principal, RecordRestriction? restriction, CancellationToken cancellationToken) =>
        BaseOperations.QueryRecordsAsync(restriction + GetClaimRecordRestriction(principal), cancellationToken);

    /// <summary>
    /// Queries database and returns modeled table records for the specified SQL filter expression
    /// and parameters.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="filterExpression">
    /// Filter SQL expression for restriction as a composite format string - does not include WHERE.
    /// When escaping is needed for field names, use standard ANSI quotes.
    /// </param>
    /// <param name="parameters">Restriction parameter values.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// Each indexed parameter, e.g., "{0}", in the composite format <paramref name="filterExpression"/>
    /// will be converted into query parameters where each of the corresponding values in the
    /// <paramref name="parameters"/> collection will be applied as <see cref="IDbDataParameter"/>
    /// values to an executed <see cref="IDbCommand"/> query.
    /// </para>
    /// <para>
    /// If any of the specified <paramref name="parameters"/> reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// <para>
    /// If needed, field names that are escaped with standard ANSI quotes in the filter expression
    /// will be updated to reflect what is defined in the user model.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="TableOperations{T}.QueryRecords(string, RecordRestriction, int)"/> only
    /// specifying the <see cref="RecordRestriction"/> parameter.
    /// </para>
    /// </remarks>
    public IEnumerable<T?> QueryRecordsWhere(ClaimsPrincipal principal, string? filterExpression, params object?[] parameters) =>
        BaseOperations.QueryRecords(new RecordRestriction(filterExpression, parameters) + GetClaimRecordRestriction(principal));

    /// <summary>
    /// Queries database and returns modeled table records for the specified SQL filter expression
    /// and parameters.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="filterExpression">
    /// Filter SQL expression for restriction as a composite format string - does not include WHERE.
    /// When escaping is needed for field names, use standard ANSI quotes.
    /// </param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">Restriction parameter values.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// Each indexed parameter, e.g., "{0}", in the composite format <paramref name="filterExpression"/>
    /// will be converted into query parameters where each of the corresponding values in the
    /// <paramref name="parameters"/> collection will be applied as <see cref="IDbDataParameter"/>
    /// values to an executed <see cref="IDbCommand"/> query.
    /// </para>
    /// <para>
    /// If any of the specified <paramref name="parameters"/> reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// <para>
    /// If needed, field names that are escaped with standard ANSI quotes in the filter expression
    /// will be updated to reflect what is defined in the user model.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="TableOperations{T}.QueryRecords(string, RecordRestriction, int)"/> only
    /// specifying the <see cref="RecordRestriction"/> parameter.
    /// </para>
    /// </remarks>
    public IAsyncEnumerable<T?> QueryRecordsWhereAsync(ClaimsPrincipal principal, string? filterExpression, CancellationToken cancellationToken, params object?[] parameters) =>
        BaseOperations.QueryRecordsAsync(new RecordRestriction(filterExpression, parameters) + GetClaimRecordRestriction(principal), cancellationToken);

    /// <summary>
    /// Queries database and returns modeled table records for the specified sorting and paging parameters.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="sortField">Field name to order-by.</param>
    /// <param name="ascending">Sort ascending flag; set to <c>false</c> for descending.</param>
    /// <param name="page">Page number of records to return (1-based).</param>
    /// <param name="pageSize">Current page size.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// This function is used for record paging. Primary keys are cached server-side, typically per user session,
    /// to maintain desired per-page sort order. Call <see cref="TableOperations{T}.ClearPrimaryKeyCache"/> to manually clear cache
    /// when table contents are known to have changed.
    /// </para>
    /// <para>
    /// If the specified <paramref name="sortField"/> has been marked with <see cref="EncryptDataAttribute"/>,
    /// establishing the primary key cache operation will take longer to execute since query data will need to
    /// be downloaded locally and decrypted so the proper sort order can be determined.
    /// </para>
    /// </remarks>
    public IEnumerable<T> QueryRecords(ClaimsPrincipal principal, string? sortField, bool ascending, int page, int pageSize) =>
        BaseOperations.QueryRecords(sortField, ascending, page, pageSize, GetClaimRecordRestriction(principal));

    /// <summary>
    /// Queries database and returns modeled table records for the specified sorting and paging parameters.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="sortField">Field name to order-by.</param>
    /// <param name="ascending">Sort ascending flag; set to <c>false</c> for descending.</param>
    /// <param name="page">Page number of records to return (1-based).</param>
    /// <param name="pageSize">Current page size.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// This function is used for record paging. Primary keys are cached server-side, typically per user session,
    /// to maintain desired per-page sort order. Call <see cref="TableOperations{T}.ClearPrimaryKeyCache"/> to manually clear cache
    /// when table contents are known to have changed.
    /// </para>
    /// <para>
    /// If the specified <paramref name="sortField"/> has been marked with <see cref="EncryptDataAttribute"/>,
    /// establishing the primary key cache operation will take longer to execute since query data will need to
    /// be downloaded locally and decrypted so the proper sort order can be determined.
    /// </para>
    /// </remarks>
    public IAsyncEnumerable<T> QueryRecordsAsync(ClaimsPrincipal principal, string? sortField, bool ascending, int page, int pageSize, CancellationToken cancellationToken) =>
        BaseOperations.QueryRecordsAsync(sortField, ascending, page, pageSize, cancellationToken, GetClaimRecordRestriction(principal));

    /// <summary>
    /// Queries database and returns modeled table records for the specified sorting, paging and search parameters.
    /// Search executed against fields modeled with <see cref="SearchableAttribute"/>.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="sortField">Field name to order-by.</param>
    /// <param name="ascending">Sort ascending flag; set to <c>false</c> for descending.</param>
    /// <param name="page">Page number of records to return (1-based).</param>
    /// <param name="pageSize">Current page size.</param>
    /// <param name="recordFilters">Record Filters to be applied.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// This function is used for record paging. Primary keys are cached server-side, typically per user session,
    /// to maintain desired per-page sort order. Call <see cref="TableOperations{T}.ClearPrimaryKeyCache"/> to manually clear cache
    /// when table contents are known to have changed.
    /// </para>
    /// <para>
    /// If the specified <paramref name="sortField"/> has been marked with <see cref="EncryptDataAttribute"/>,
    /// establishing the primary key cache operation will take longer to execute since query data will need to
    /// be downloaded locally and decrypted so the proper sort order can be determined.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="TableOperations{T}.QueryRecords(string, bool, int, int, RecordRestriction[])"/> where restriction
    /// is generated by <see cref="GetSearchRestrictions(IRecordFilter[])"/> using <paramref name="recordFilters"/>.
    /// </para>
    /// </remarks>
    public IEnumerable<T> QueryRecords(ClaimsPrincipal principal, string? sortField, bool ascending, int page, int pageSize, params IRecordFilter?[]? recordFilters) =>
        BaseOperations.QueryRecords(sortField, ascending, page, pageSize, (BaseOperations.GetSearchRestrictions(recordFilters) ?? []).Append(GetClaimRecordRestriction(principal)).ToArray());

    /// <summary>
    /// Queries database and returns modeled table records for the specified sorting, paging and search parameters.
    /// Search executed against fields modeled with <see cref="SearchableAttribute"/>.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="sortField">Field name to order-by.</param>
    /// <param name="ascending">Sort ascending flag; set to <c>false</c> for descending.</param>
    /// <param name="page">Page number of records to return (1-based).</param>
    /// <param name="pageSize">Current page size.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="recordFilters">Record Filters to be applied.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// This function is used for record paging. Primary keys are cached server-side, typically per user session,
    /// to maintain desired per-page sort order. Call <see cref="TableOperations{T}.ClearPrimaryKeyCache"/> to manually clear cache
    /// when table contents are known to have changed.
    /// </para>
    /// <para>
    /// If the specified <paramref name="sortField"/> has been marked with <see cref="EncryptDataAttribute"/>,
    /// establishing the primary key cache operation will take longer to execute since query data will need to
    /// be downloaded locally and decrypted so the proper sort order can be determined.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="TableOperations{T}.QueryRecords(string, bool, int, int, RecordRestriction[])"/> where restriction
    /// is generated by <see cref="GetSearchRestrictions(IRecordFilter[])"/> using <paramref name="recordFilters"/>.
    /// </para>
    /// </remarks>
    public IAsyncEnumerable<T> QueryRecordsAsync(ClaimsPrincipal principal, string? sortField, bool ascending, int page, int pageSize, CancellationToken cancellationToken, params IRecordFilter?[]? recordFilters) =>
        BaseOperations.QueryRecordsAsync(sortField, ascending, page, pageSize, cancellationToken, (BaseOperations.GetSearchRestrictions(recordFilters) ?? []).Append(GetClaimRecordRestriction(principal)).ToArray());

    /// <summary>
    /// Queries database and returns modeled table records for the specified sorting and paging parameters.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="sortField">Field name to order-by.</param>
    /// <param name="ascending">Sort ascending flag; set to <c>false</c> for descending.</param>
    /// <param name="page">Page number of records to return (1-based).</param>
    /// <param name="pageSize">Current page size.</param>
    /// <param name="restrictions">Record restrictions to apply, if any.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// This function is used for record paging. Primary keys are cached server-side, typically per user session,
    /// to maintain desired per-page sort order. Call <see cref="TableOperations{T}.ClearPrimaryKeyCache"/> to manually clear cache
    /// when table contents are known to have changed.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restrictions"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// <para>
    /// If the specified <paramref name="sortField"/> has been marked with <see cref="EncryptDataAttribute"/>,
    /// establishing the primary key cache operation will take longer to execute since query data will need to
    /// be downloaded locally and decrypted so the proper sort order can be determined.
    /// </para>
    /// </remarks>
    public IEnumerable<T> QueryRecords(ClaimsPrincipal principal, string? sortField, bool ascending, int page, int pageSize, params RecordRestriction?[]? restrictions) =>
        BaseOperations.QueryRecords(sortField, ascending, page, pageSize, (restrictions ?? []).Append(GetClaimRecordRestriction(principal)).ToArray());

    /// <summary>
    /// Queries database and returns modeled table records for the specified sorting and paging parameters.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="sortField">Field name to order-by.</param>
    /// <param name="ascending">Sort ascending flag; set to <c>false</c> for descending.</param>
    /// <param name="page">Page number of records to return (1-based).</param>
    /// <param name="pageSize">Current page size.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="restrictions">Record restrictions to apply, if any.</param>
    /// <returns>An enumerable of modeled table row instances for queried records.</returns>
    /// <remarks>
    /// <para>
    /// This function is used for record paging. Primary keys are cached server-side, typically per user session,
    /// to maintain desired per-page sort order. Call <see cref="TableOperations{T}.ClearPrimaryKeyCache"/> to manually clear cache
    /// when table contents are known to have changed.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restrictions"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// <para>
    /// If the specified <paramref name="sortField"/> has been marked with <see cref="EncryptDataAttribute"/>,
    /// establishing the primary key cache operation will take longer to execute since query data will need to
    /// be downloaded locally and decrypted so the proper sort order can be determined.
    /// </para>
    /// </remarks>
    public IAsyncEnumerable<T> QueryRecordsAsync(ClaimsPrincipal principal, string? sortField, bool ascending, int page, int pageSize, [EnumeratorCancellation] CancellationToken cancellationToken, params RecordRestriction?[]? restrictions) => 
        BaseOperations.QueryRecordsAsync(sortField, ascending, page, pageSize, cancellationToken, (restrictions ?? []).Append(GetClaimRecordRestriction(principal)).ToArray());

    /// <summary>
    /// Gets total record count for the modeled table.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <returns>
    /// Total record count for the modeled table.
    /// </returns>
    public int QueryRecordCount(ClaimsPrincipal principal) =>
        BaseOperations.QueryRecordCount([GetClaimRecordRestriction(principal)]);

    /// <summary>
    /// Gets total record count for the modeled table.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <returns>
    /// Total record count for the modeled table.
    /// </returns>
    public Task<int> QueryRecordCountAsync(ClaimsPrincipal principal, CancellationToken cancellationToken) =>
        BaseOperations.QueryRecordCountAsync(cancellationToken, [GetClaimRecordRestriction(principal)]);

    /// <summary>
    /// Gets the record count for the modeled table based on search parameter.
    /// Search executed against fields modeled with <see cref="SearchableAttribute"/>.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="recordFilter"><see cref="IRecordFilter"/> to be filtered by</param>
    /// <returns>Record count for the modeled table based on search parameter.</returns>
    /// <remarks>
    /// This is a convenience call to <see cref="QueryRecordCount(RecordRestriction[])"/> where restriction
    /// is generated by <see cref="GetSearchRestrictions(IRecordFilter[])"/>
    /// </remarks>
    public int QueryRecordCount(ClaimsPrincipal principal, params IRecordFilter?[]? recordFilter) =>
        BaseOperations.QueryRecordCount((BaseOperations.GetSearchRestrictions(recordFilter) ?? []).Append(GetClaimRecordRestriction(principal)).ToArray());

    /// <summary>
    /// Gets the record count for the modeled table based on search parameter.
    /// Search executed against fields modeled with <see cref="SearchableAttribute"/>.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="recordFilter"><see cref="IRecordFilter"/> to be filtered by</param>
    /// <returns>Record count for the modeled table based on search parameter.</returns>
    /// <remarks>
    /// This is a convenience call to <see cref="QueryRecordCount(RecordRestriction[])"/> where restriction
    /// is generated by <see cref="GetSearchRestrictions(IRecordFilter[])"/>
    /// </remarks>
    public Task<int> QueryRecordCountAsync(ClaimsPrincipal principal, CancellationToken cancellationToken, params IRecordFilter?[]? recordFilter) =>
        BaseOperations.QueryRecordCountAsync(cancellationToken, (BaseOperations.GetSearchRestrictions(recordFilter) ?? []).Append(GetClaimRecordRestriction(principal)).ToArray());

    /// <summary>
    /// Gets the record count for the specified <paramref name="restrictions"/> - or - total record
    /// count for the modeled table if <paramref name="restrictions"/> is <c>null</c>.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="restrictions">Record restrictions to apply, if any.</param>
    /// <returns>
    /// Record count for the specified <paramref name="restrictions"/> - or - total record count
    /// for the modeled table if no <see cref="RecordRestriction"/> is provided.
    /// </returns>
    /// <remarks>
    /// If any of the <paramref name="restrictions"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </remarks>
    public int QueryRecordCount(ClaimsPrincipal principal, params RecordRestriction?[]? restrictions) =>
        BaseOperations.QueryRecordCount((restrictions ?? []).Append(GetClaimRecordRestriction(principal)).ToArray());

    /// <summary>
    /// Gets the record count for the specified <paramref name="restrictions"/> - or - total record
    /// count for the modeled table if <paramref name="restrictions"/> is <c>null</c>.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="restrictions">Record restrictions to apply, if any.</param>
    /// <returns>
    /// Record count for the specified <paramref name="restrictions"/> - or - total record count
    /// for the modeled table if no <see cref="RecordRestriction"/> is provided.
    /// </returns>
    /// <remarks>
    /// If any of the <paramref name="restrictions"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </remarks>
    public Task<int> QueryRecordCountAsync(ClaimsPrincipal principal, CancellationToken cancellationToken, params RecordRestriction?[]? restrictions) =>
        BaseOperations.QueryRecordCountAsync(cancellationToken, (restrictions ?? []).Append(GetClaimRecordRestriction(principal)).ToArray());

    /// <summary>
    /// Gets the record count for the modeled table for the specified SQL filter expression and parameters.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="filterExpression">
    /// Filter SQL expression for restriction as a composite format string - does not include WHERE.
    /// When escaping is needed for field names, use standard ANSI quotes.
    /// </param>
    /// <param name="parameters">Restriction parameter values.</param>
    /// <returns>Record count for the modeled table for the specified parameters.</returns>
    /// <remarks>
    /// <para>
    /// Each indexed parameter, e.g., "{0}", in the composite format <paramref name="filterExpression"/>
    /// will be converted into query parameters where each of the corresponding values in the
    /// <paramref name="parameters"/> collection will be applied as <see cref="IDbDataParameter"/>
    /// values to an executed <see cref="IDbCommand"/> query.
    /// </para>
    /// <para>
    /// If any of the specified <paramref name="parameters"/> reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// <para>
    /// If needed, field names that are escaped with standard ANSI quotes in the filter expression
    /// will be updated to reflect what is defined in the user model.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="QueryRecordCount(RecordRestriction[])"/>.
    /// </para>
    /// </remarks>
    public int QueryRecordCountWhere(ClaimsPrincipal principal, string? filterExpression, params object?[] parameters) =>
        BaseOperations.QueryRecordCount(new RecordRestriction(filterExpression, parameters) + GetClaimRecordRestriction(principal));

    /// <summary>
    /// Gets the record count for the modeled table for the specified SQL filter expression and parameters.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="filterExpression">
    /// Filter SQL expression for restriction as a composite format string - does not include WHERE.
    /// When escaping is needed for field names, use standard ANSI quotes.
    /// </param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">Restriction parameter values.</param>
    /// <returns>Record count for the modeled table for the specified parameters.</returns>
    /// <remarks>
    /// <para>
    /// Each indexed parameter, e.g., "{0}", in the composite format <paramref name="filterExpression"/>
    /// will be converted into query parameters where each of the corresponding values in the
    /// <paramref name="parameters"/> collection will be applied as <see cref="IDbDataParameter"/>
    /// values to an executed <see cref="IDbCommand"/> query.
    /// </para>
    /// <para>
    /// If any of the specified <paramref name="parameters"/> reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// <para>
    /// If needed, field names that are escaped with standard ANSI quotes in the filter expression
    /// will be updated to reflect what is defined in the user model.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="QueryRecordCount(RecordRestriction[])"/>.
    /// </para>
    /// </remarks>
    public Task<int> QueryRecordCountWhereAsync(ClaimsPrincipal principal, string? filterExpression, CancellationToken cancellationToken, params object?[] parameters) =>
        BaseOperations.QueryRecordCountAsync(cancellationToken, new RecordRestriction(filterExpression, parameters) + GetClaimRecordRestriction(principal));

    /// <summary>
    /// Deletes the records referenced by the specified <paramref name="restriction"/>.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="restriction">Record restriction to apply</param>
    /// <param name="applyRootQueryRestriction">
    /// Flag that determines if any existing <see cref="RootQueryRestriction"/> should be applied. Defaults to
    /// <see cref="ApplyRootQueryRestrictionToDeletes"/> setting.
    /// </param>
    /// <returns>Number of rows affected.</returns>
    /// <remarks>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="restriction"/> cannot be <c>null</c>.</exception>
    public int DeleteRecord(ClaimsPrincipal principal, RecordRestriction? restriction, bool? applyRootQueryRestriction = null) =>
        BaseOperations.DeleteRecord(restriction + GetClaimRecordRestriction(principal), applyRootQueryRestriction);

    /// <summary>
    /// Deletes the records referenced by the specified <paramref name="restriction"/>.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="restriction">Record restriction to apply</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="applyRootQueryRestriction">
    /// Flag that determines if any existing <see cref="RootQueryRestriction"/> should be applied. Defaults to
    /// <see cref="ApplyRootQueryRestrictionToDeletes"/> setting.
    /// </param>
    /// <returns>Number of rows affected.</returns>
    /// <remarks>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="restriction"/> cannot be <c>null</c>.</exception>
    public Task<int> DeleteRecordAsync(ClaimsPrincipal principal, RecordRestriction? restriction, CancellationToken cancellationToken, bool? applyRootQueryRestriction = null) =>
        BaseOperations.DeleteRecordAsync(restriction + GetClaimRecordRestriction(principal), cancellationToken, applyRootQueryRestriction);

    /// <summary>
    /// Deletes the records referenced by the specified SQL filter expression and parameters.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="filterExpression">
    /// Filter SQL expression for restriction as a composite format string - does not include WHERE.
    /// When escaping is needed for field names, use standard ANSI quotes.
    /// </param>
    /// <param name="parameters">Restriction parameter values.</param>
    /// <returns>Number of rows affected.</returns>
    /// <remarks>
    /// <para>
    /// Each indexed parameter, e.g., "{0}", in the composite format <paramref name="filterExpression"/>
    /// will be converted into query parameters where each of the corresponding values in the
    /// <paramref name="parameters"/> collection will be applied as <see cref="IDbDataParameter"/>
    /// values to an executed <see cref="IDbCommand"/> query.
    /// </para>
    /// <para>
    /// If any of the specified <paramref name="parameters"/> reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// <para>
    /// If needed, field names that are escaped with standard ANSI quotes in the filter expression
    /// will be updated to reflect what is defined in the user model.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="DeleteRecord(RecordRestriction, bool?)"/>.
    /// </para>
    /// </remarks>
    public int DeleteRecordWhere(ClaimsPrincipal principal, string filterExpression, params object?[] parameters) =>
        DeleteRecord(principal, new RecordRestriction(filterExpression, parameters));

    /// <summary>
    /// Deletes the records referenced by the specified SQL filter expression and parameters.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="filterExpression">
    /// Filter SQL expression for restriction as a composite format string - does not include WHERE.
    /// When escaping is needed for field names, use standard ANSI quotes.
    /// </param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">Restriction parameter values.</param>
    /// <returns>Number of rows affected.</returns>
    /// <remarks>
    /// <para>
    /// Each indexed parameter, e.g., "{0}", in the composite format <paramref name="filterExpression"/>
    /// will be converted into query parameters where each of the corresponding values in the
    /// <paramref name="parameters"/> collection will be applied as <see cref="IDbDataParameter"/>
    /// values to an executed <see cref="IDbCommand"/> query.
    /// </para>
    /// <para>
    /// If any of the specified <paramref name="parameters"/> reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// <para>
    /// If needed, field names that are escaped with standard ANSI quotes in the filter expression
    /// will be updated to reflect what is defined in the user model.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="DeleteRecord(RecordRestriction, bool?)"/>.
    /// </para>
    /// </remarks>
    public Task<int> DeleteRecordWhereAsync(ClaimsPrincipal principal, string filterExpression, CancellationToken cancellationToken, params object?[] parameters) =>
        DeleteRecordAsync(principal, new RecordRestriction(filterExpression, parameters), cancellationToken);

    /// <summary>
    /// Updates the database with the specified modeled table <paramref name="record"/>,
    /// any model properties marked with <see cref="UpdateValueExpressionAttribute"/> will
    /// be evaluated and applied before the record is provided to the data source.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="record">Record to update.</param>
    /// <param name="restriction">Record restriction to apply, if any.</param>
    /// <param name="applyRootQueryRestriction">
    /// Flag that determines if any existing <see cref="RootQueryRestriction"/> should be applied. Defaults to
    /// <see cref="ApplyRootQueryRestrictionToUpdates"/> setting.
    /// </param>
    /// <returns>Number of rows affected.</returns>
    /// <remarks>
    /// <para>
    /// Record restriction is only used for custom update expressions or in cases where modeled
    /// table has no defined primary keys.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// </remarks>
    public int UpdateRecord(ClaimsPrincipal principal, T record, RecordRestriction? restriction = null, bool? applyRootQueryRestriction = null) =>
        BaseOperations.UpdateRecord(record, restriction + GetClaimRecordRestriction(principal), applyRootQueryRestriction);

    /// <summary>
    /// Updates the database with the specified modeled table <paramref name="record"/>,
    /// any model properties marked with <see cref="UpdateValueExpressionAttribute"/> will
    /// be evaluated and applied before the record is provided to the data source.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="record">Record to update.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="restriction">Record restriction to apply, if any.</param>
    /// <param name="applyRootQueryRestriction">
    /// Flag that determines if any existing <see cref="RootQueryRestriction"/> should be applied. Defaults to
    /// <see cref="ApplyRootQueryRestrictionToUpdates"/> setting.
    /// </param>
    /// <returns>Number of rows affected.</returns>
    /// <remarks>
    /// <para>
    /// Record restriction is only used for custom update expressions or in cases where modeled
    /// table has no defined primary keys.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// </remarks>
    public Task<int> UpdateRecordAsync(ClaimsPrincipal principal, T record, CancellationToken cancellationToken, RecordRestriction? restriction = null, bool? applyRootQueryRestriction = null) =>
        BaseOperations.UpdateRecordAsync(record, cancellationToken, restriction + GetClaimRecordRestriction(principal), applyRootQueryRestriction);

    /// <summary>
    /// Updates the database with the specified modeled table <paramref name="record"/>
    /// referenced by the specified SQL filter expression and parameters, any model properties
    /// marked with <see cref="UpdateValueExpressionAttribute"/> will be evaluated and applied
    /// before the record is provided to the data source.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="record">Record to update.</param>
    /// <param name="filterExpression">
    /// Filter SQL expression for restriction as a composite format string - does not include WHERE.
    /// When escaping is needed for field names, use standard ANSI quotes.
    /// </param>
    /// <param name="parameters">Restriction parameter values.</param>
    /// <returns>Number of rows affected.</returns>
    /// <remarks>
    /// <para>
    /// Record restriction is only used for custom update expressions or in cases where modeled
    /// table has no defined primary keys.
    /// </para>
    /// <para>
    /// Each indexed parameter, e.g., "{0}", in the composite format <paramref name="filterExpression"/>
    /// will be converted into query parameters where each of the corresponding values in the
    /// <paramref name="parameters"/> collection will be applied as <see cref="IDbDataParameter"/>
    /// values to an executed <see cref="IDbCommand"/> query.
    /// </para>
    /// <para>
    /// If any of the specified <paramref name="parameters"/> reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// <para>
    /// If needed, field names that are escaped with standard ANSI quotes in the filter expression
    /// will be updated to reflect what is defined in the user model.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="UpdateRecord(T, RecordRestriction, bool?)"/>.
    /// </para>
    /// </remarks>
    public int UpdateRecordWhere(ClaimsPrincipal principal, T record, string filterExpression, params object?[] parameters) =>
        UpdateRecord(principal, record, new RecordRestriction(filterExpression, parameters));

    /// <summary>
    /// Updates the database with the specified modeled table <paramref name="record"/>
    /// referenced by the specified SQL filter expression and parameters, any model properties
    /// marked with <see cref="UpdateValueExpressionAttribute"/> will be evaluated and applied
    /// before the record is provided to the data source.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="record">Record to update.</param>
    /// <param name="filterExpression">
    /// Filter SQL expression for restriction as a composite format string - does not include WHERE.
    /// When escaping is needed for field names, use standard ANSI quotes.
    /// </param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">Restriction parameter values.</param>
    /// <returns>Number of rows affected.</returns>
    /// <remarks>
    /// <para>
    /// Record restriction is only used for custom update expressions or in cases where modeled
    /// table has no defined primary keys.
    /// </para>
    /// <para>
    /// Each indexed parameter, e.g., "{0}", in the composite format <paramref name="filterExpression"/>
    /// will be converted into query parameters where each of the corresponding values in the
    /// <paramref name="parameters"/> collection will be applied as <see cref="IDbDataParameter"/>
    /// values to an executed <see cref="IDbCommand"/> query.
    /// </para>
    /// <para>
    /// If any of the specified <paramref name="parameters"/> reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// <para>
    /// If needed, field names that are escaped with standard ANSI quotes in the filter expression
    /// will be updated to reflect what is defined in the user model.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="UpdateRecord(T, RecordRestriction, bool?)"/>.
    /// </para>
    /// </remarks>
    public Task<int> UpdateRecordWhereAsync(ClaimsPrincipal principal, T record, string filterExpression, CancellationToken cancellationToken, params object?[] parameters) =>
        UpdateRecordAsync(principal, record, cancellationToken, new RecordRestriction(filterExpression, parameters));

    /// <summary>
    /// Updates the database with the specified <paramref name="row"/>, any model properties
    /// marked with <see cref="UpdateValueExpressionAttribute"/> will be evaluated and applied
    /// before the record is provided to the data source.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="row"><see cref="DataRow"/> of queried data to be updated.</param>
    /// <param name="restriction">Record restriction to apply, if any.</param>
    /// <returns>Number of rows affected.</returns>
    /// <remarks>
    /// <para>
    /// Record restriction is only used for custom update expressions or in cases where modeled
    /// table has no defined primary keys.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// </remarks>
    public int UpdateRecord(ClaimsPrincipal principal, DataRow row, RecordRestriction? restriction = null) =>
        BaseOperations.UpdateRecord(row, restriction + GetClaimRecordRestriction(principal));

    /// <summary>
    /// Updates the database with the specified <paramref name="row"/>, any model properties
    /// marked with <see cref="UpdateValueExpressionAttribute"/> will be evaluated and applied
    /// before the record is provided to the data source.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="row"><see cref="DataRow"/> of queried data to be updated.</param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="restriction">Record restriction to apply, if any.</param>
    /// <returns>Number of rows affected.</returns>
    /// <remarks>
    /// <para>
    /// Record restriction is only used for custom update expressions or in cases where modeled
    /// table has no defined primary keys.
    /// </para>
    /// <para>
    /// If any of the <paramref name="restriction"/> parameters reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// </remarks>
    public Task<int> UpdateRecordAsync(ClaimsPrincipal principal, DataRow row, CancellationToken cancellationToken, RecordRestriction? restriction = null) =>
        BaseOperations.UpdateRecordAsync(row, cancellationToken, restriction + GetClaimRecordRestriction(principal));

    /// <summary>
    /// Updates the database with the specified <paramref name="row"/> referenced by the
    /// specified SQL filter expression and parameters, any model properties marked with
    /// <see cref="UpdateValueExpressionAttribute"/> will be evaluated and applied before
    /// the record is provided to the data source.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="row"><see cref="DataRow"/> of queried data to be updated.</param>
    /// <param name="filterExpression">
    /// Filter SQL expression for restriction as a composite format string - does not include WHERE.
    /// When escaping is needed for field names, use standard ANSI quotes.
    /// </param>
    /// <param name="parameters">Restriction parameter values.</param>
    /// <returns>Number of rows affected.</returns>
    /// <remarks>
    /// <para>
    /// Record restriction is only used for custom update expressions or in cases where modeled
    /// table has no defined primary keys.
    /// </para>
    /// <para>
    /// Each indexed parameter, e.g., "{0}", in the composite format <paramref name="filterExpression"/>
    /// will be converted into query parameters where each of the corresponding values in the
    /// <paramref name="parameters"/> collection will be applied as <see cref="IDbDataParameter"/>
    /// values to an executed <see cref="IDbCommand"/> query.
    /// </para>
    /// <para>
    /// If any of the specified <paramref name="parameters"/> reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// <para>
    /// If needed, field names that are escaped with standard ANSI quotes in the filter expression
    /// will be updated to reflect what is defined in the user model.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="UpdateRecord(DataRow, RecordRestriction)"/>.
    /// </para>
    /// </remarks>
    public int UpdateRecordWhere(ClaimsPrincipal principal, DataRow row, string filterExpression, params object?[] parameters) =>
        UpdateRecord(principal, row, new RecordRestriction(filterExpression, parameters));

    /// <summary>
    /// Updates the database with the specified <paramref name="row"/> referenced by the
    /// specified SQL filter expression and parameters, any model properties marked with
    /// <see cref="UpdateValueExpressionAttribute"/> will be evaluated and applied before
    /// the record is provided to the data source.
    /// </summary>
    /// <param name="principal">Claims principal which is making the request.</param>
    /// <param name="row"><see cref="DataRow"/> of queried data to be updated.</param>
    /// <param name="filterExpression">
    /// Filter SQL expression for restriction as a composite format string - does not include WHERE.
    /// When escaping is needed for field names, use standard ANSI quotes.
    /// </param>
    /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
    /// <param name="parameters">Restriction parameter values.</param>
    /// <returns>Number of rows affected.</returns>
    /// <remarks>
    /// <para>
    /// Record restriction is only used for custom update expressions or in cases where modeled
    /// table has no defined primary keys.
    /// </para>
    /// <para>
    /// Each indexed parameter, e.g., "{0}", in the composite format <paramref name="filterExpression"/>
    /// will be converted into query parameters where each of the corresponding values in the
    /// <paramref name="parameters"/> collection will be applied as <see cref="IDbDataParameter"/>
    /// values to an executed <see cref="IDbCommand"/> query.
    /// </para>
    /// <para>
    /// If any of the specified <paramref name="parameters"/> reference a table field that is modeled with
    /// either an <see cref="EncryptDataAttribute"/> or <see cref="FieldDataTypeAttribute"/>, then the function
    /// <see cref="TableOperations{T}.GetInterpretedFieldValue"/> will need to be called, replacing the target parameter with the
    /// returned value so that the field value will be properly set prior to executing the database function.
    /// </para>
    /// <para>
    /// If needed, field names that are escaped with standard ANSI quotes in the filter expression
    /// will be updated to reflect what is defined in the user model.
    /// </para>
    /// <para>
    /// This is a convenience call to <see cref="UpdateRecord(DataRow, RecordRestriction)"/>.
    /// </para>
    /// </remarks>
    public Task<int> UpdateRecordWhereAsync(ClaimsPrincipal principal, DataRow row, string filterExpression, CancellationToken cancellationToken, params object?[] parameters) =>
        UpdateRecordAsync(principal, row, cancellationToken, new RecordRestriction(filterExpression, parameters));

    #endregion

    #region [ Static ]

    private static readonly ClaimQueryRestrictionAttribute? s_claimQueryRestrictionAttribute;

    static SecureTableOperations()
    {
        typeof(T).TryGetAttribute(out s_claimQueryRestrictionAttribute);
    }

    #endregion
}
