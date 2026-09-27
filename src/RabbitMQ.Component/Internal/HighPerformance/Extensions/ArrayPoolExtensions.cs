// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Vendored from CommunityToolkit/dotnet commit b135626dd54d33b8f05f2ff31591592c004aa848.

using System.Buffers;
using System.Diagnostics.CodeAnalysis;

namespace RabbitMQ.Component.Internal.HighPerformance;

/// <summary>Helpers for working with <see cref="ArrayPool{T}"/>.</summary>
internal static class ArrayPoolExtensions
{
    /// <summary>Changes the size of a rented one-dimensional array.</summary>
    /// <remarks>
    /// Local patches validate undersized custom-pool rentals, pre-clear arrays before potentially
    /// throwing returns, and leave the replacement owned by the caller if returning the old array fails.
    /// </remarks>
    public static void Resize<T>(this ArrayPool<T> pool, [NotNull] ref T[]? array, int newSize,
        bool clearArray = false)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(newSize);

        if (array is null)
        {
            T[] rented = pool.Rent(newSize);
            if (rented.Length < newSize)
            {
                rented.AsSpan().Clear();
                pool.Return(rented, clearArray: true);
                throw new InvalidOperationException("The buffer pool returned an array smaller than requested.");
            }

            array = rented;
            return;
        }

        if (array.Length == newSize) return;

        T[] newArray = pool.Rent(newSize);
        if (newArray.Length < newSize)
        {
            newArray.AsSpan().Clear();
            pool.Return(newArray, clearArray: true);
            throw new InvalidOperationException("The buffer pool returned an array smaller than requested.");
        }

        try
        {
            Array.Copy(array, 0, newArray, 0, Math.Min(array.Length, newSize));
        }
        catch
        {
            newArray.AsSpan().Clear();
            pool.Return(newArray, clearArray: true);
            throw;
        }

        T[] oldArray = array;
        array = newArray;
        if (clearArray) oldArray.AsSpan().Clear();
        // Do not retry if a custom pool throws: it may have accepted the array before throwing.
        pool.Return(oldArray, clearArray);
    }
}
