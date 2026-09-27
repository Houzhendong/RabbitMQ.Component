// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Local byte specialization for the vendored CommunityToolkit.HighPerformance writer.

using System.Buffers;
using RabbitMQ.Component.Serialization;

namespace RabbitMQ.Component.Internal.HighPerformance.Buffers;

/// <summary>
/// A byte-specialized pooled writer whose written memory can be transferred as an owned payload.
/// </summary>
internal sealed class OwnedArrayPoolBufferWriter : ArrayPoolBufferWriter<byte>, IOwnedBuffer
{
    public OwnedArrayPoolBufferWriter(int initialCapacity = 256, ArrayPool<byte>? pool = null,
        int maxCapacity = int.MaxValue)
        : base(pool ?? ArrayPool<byte>.Shared, initialCapacity, maxCapacity)
    {
    }

    public int Length => WrittenCount;
}
