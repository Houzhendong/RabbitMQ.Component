// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Vendored from CommunityToolkit/dotnet commit b135626dd54d33b8f05f2ff31591592c004aa848.

using System.Runtime.CompilerServices;
using RabbitMQ.Component.Internal.HighPerformance.Buffers;
using RabbitMQ.Component.Internal.HighPerformance.Streams;

namespace RabbitMQ.Component.Internal.HighPerformance;

internal static class ArrayPoolBufferWriterExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Stream AsStream(this ArrayPoolBufferWriter<byte> writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        return new IBufferWriterStream<ArrayBufferWriterOwner>(new ArrayBufferWriterOwner(writer));
    }
}
