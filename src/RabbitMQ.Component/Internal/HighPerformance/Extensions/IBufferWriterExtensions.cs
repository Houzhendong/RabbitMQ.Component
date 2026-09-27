// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Vendored from CommunityToolkit/dotnet commit b135626dd54d33b8f05f2ff31591592c004aa848.

using System.Buffers;
using System.Runtime.CompilerServices;
using RabbitMQ.Component.Internal.HighPerformance.Buffers;
using RabbitMQ.Component.Internal.HighPerformance.Streams;

namespace RabbitMQ.Component.Internal.HighPerformance;

internal static class IBufferWriterExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Stream AsStream(this IBufferWriter<byte> writer)
    {
        ArgumentNullException.ThrowIfNull(writer);

        // The upstream type is sealed. The local owned byte writer derives from the internalized
        // source, so this type test intentionally includes derived instances.
        if (writer is ArrayPoolBufferWriter<byte> internalWriter)
            return new IBufferWriterStream<ArrayBufferWriterOwner>(new ArrayBufferWriterOwner(internalWriter));

        return new IBufferWriterStream<IBufferWriterOwner>(new IBufferWriterOwner(writer));
    }
}
