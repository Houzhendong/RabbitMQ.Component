// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Vendored from CommunityToolkit/dotnet commit b135626dd54d33b8f05f2ff31591592c004aa848.

using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using RabbitMQ.Component.Internal.HighPerformance;

namespace RabbitMQ.Component.Internal.HighPerformance.Buffers;

/// <summary>
/// Represents a heap-based, array-backed output sink whose arrays are rented from an
/// <see cref="ArrayPool{T}"/>.
/// </summary>
/// <remarks>
/// Local patches internalize and unseal the upstream type, add an immutable logical maximum capacity,
/// validate advances against the most recently exposed region, and clear every returned array.
/// The pool may return a physically larger array, but that excess is never exposed.
/// </remarks>
internal class ArrayPoolBufferWriter<T> : IBuffer<T>, IMemoryOwner<T>
{
    private const int DefaultInitialBufferSize = 256;

    private readonly ArrayPool<T> pool;
    private readonly int maxCapacity;
    private T[]? array;
    private int index;
    private int capacity;
    private int available;

    public ArrayPoolBufferWriter()
        : this(ArrayPool<T>.Shared, DefaultInitialBufferSize, int.MaxValue)
    {
    }

    public ArrayPoolBufferWriter(ArrayPool<T> pool)
        : this(pool, DefaultInitialBufferSize, int.MaxValue)
    {
    }

    public ArrayPoolBufferWriter(int initialCapacity)
        : this(ArrayPool<T>.Shared, initialCapacity, int.MaxValue)
    {
    }

    public ArrayPoolBufferWriter(ArrayPool<T> pool, int initialCapacity)
        : this(pool, initialCapacity, int.MaxValue)
    {
    }

    internal ArrayPoolBufferWriter(ArrayPool<T> pool, int initialCapacity, int maxCapacity)
    {
        ArgumentNullException.ThrowIfNull(pool);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(initialCapacity);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxCapacity);

        this.pool = pool;
        this.maxCapacity = Math.Min(maxCapacity, Array.MaxLength);
        if (initialCapacity > this.maxCapacity)
            throw new ArgumentOutOfRangeException(nameof(initialCapacity));

        T[] rented = pool.Rent(initialCapacity);
        if (rented.Length < initialCapacity)
        {
            rented.AsSpan().Clear();
            pool.Return(rented, clearArray: true);
            throw new InvalidOperationException("The buffer pool returned an array smaller than requested.");
        }

        this.array = rented;
        this.capacity = Math.Min(rented.Length, this.maxCapacity);
    }

    Memory<T> IMemoryOwner<T>.Memory
    {
        get => MemoryMarshal.AsMemory(WrittenMemory);
    }

    public ReadOnlyMemory<T> WrittenMemory
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            T[] current = GetArray();
            return current.AsMemory(0, this.index);
        }
    }

    public ReadOnlySpan<T> WrittenSpan
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            T[] current = GetArray();
            return current.AsSpan(0, this.index);
        }
    }

    public int WrittenCount
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            _ = GetArray();
            return this.index;
        }
    }

    public int Capacity
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            _ = GetArray();
            return this.capacity;
        }
    }

    public int MaxCapacity
    {
        get
        {
            _ = GetArray();
            return this.maxCapacity;
        }
    }

    internal int RentedCapacity => GetArray().Length;

    public int FreeCapacity
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            _ = GetArray();
            return this.capacity - this.index;
        }
    }

    public void Clear()
    {
        T[] current = GetArray();
        current.AsSpan(0, this.index).Clear();
        this.index = 0;
        this.available = 0;
    }

    public void Advance(int count)
    {
        _ = GetArray();
        ArgumentOutOfRangeException.ThrowIfNegative(count);

        if (count > this.maxCapacity - this.index) ThrowLimitExceeded();
        if (count > this.available)
            throw new ArgumentOutOfRangeException(nameof(count), "The buffer writer cannot advance past the available region.");

        this.index += count;
        this.available -= count;
    }

    public Memory<T> GetMemory(int sizeHint = 0)
    {
        CheckBufferAndEnsureCapacity(sizeHint);
        this.available = this.capacity - this.index;
        return this.array!.AsMemory(this.index, this.available);
    }

    public Span<T> GetSpan(int sizeHint = 0)
    {
        CheckBufferAndEnsureCapacity(sizeHint);
        this.available = this.capacity - this.index;
        return this.array!.AsSpan(this.index, this.available);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ArraySegment<T> DangerousGetArray()
    {
        T[] current = GetArray();
        return new(current, 0, this.index);
    }

    public void Dispose()
    {
        T[]? current = Interlocked.Exchange(ref this.array, null);
        if (current is null) return;

        this.index = 0;
        this.capacity = 0;
        this.available = 0;
        current.AsSpan().Clear();
        // Do not retry if a custom pool throws: it may have accepted the array before throwing.
        this.pool.Return(current, clearArray: true);
    }

    public override string ToString()
    {
        if (typeof(T) == typeof(char) && this.array is char[] chars)
            return new string(chars, 0, this.index);

        return $"{GetType()}[{this.index}]";
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CheckBufferAndEnsureCapacity(int sizeHint)
    {
        _ = GetArray();
        ArgumentOutOfRangeException.ThrowIfNegative(sizeHint);
        if (sizeHint == 0) sizeHint = 1;

        // Local bounded-writer patch: validate before addition, growth, or pool rental.
        if (sizeHint > this.maxCapacity - this.index) ThrowLimitExceeded();
        if (sizeHint > this.capacity - this.index) ResizeBuffer(sizeHint);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void ResizeBuffer(int sizeHint)
    {
        long required = (long)this.index + sizeHint;
        long doubled = (long)this.capacity * 2;
        int newSize = (int)Math.Min(this.maxCapacity, Math.Max(required, doubled));

        this.pool.Resize(ref this.array, newSize, clearArray: true);
        this.capacity = Math.Min(this.array.Length, this.maxCapacity);
        this.available = 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private T[] GetArray()
    {
        T[]? current = this.array;
        if (current is null)
            throw new ObjectDisposedException("The current buffer has already been disposed.");
        return current;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowLimitExceeded() =>
        throw new InvalidDataException("The output exceeds the configured buffer limit.");
}
