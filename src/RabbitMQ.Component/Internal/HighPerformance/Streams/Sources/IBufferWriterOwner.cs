// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Vendored from CommunityToolkit/dotnet commit b135626dd54d33b8f05f2ff31591592c004aa848.

using System.Buffers;
using System.Runtime.CompilerServices;

namespace RabbitMQ.Component.Internal.HighPerformance.Streams;

internal readonly struct IBufferWriterOwner(IBufferWriter<byte> writer) : IBufferWriter<byte>
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Advance(int count) => writer.Advance(count);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Memory<byte> GetMemory(int sizeHint = 0) => writer.GetMemory(sizeHint);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Span<byte> GetSpan(int sizeHint = 0) => writer.GetSpan(sizeHint);
}
