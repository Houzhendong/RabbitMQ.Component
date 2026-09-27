// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Vendored from CommunityToolkit/dotnet commit b135626dd54d33b8f05f2ff31591592c004aa848.

using System.Buffers;

namespace RabbitMQ.Component.Internal.HighPerformance.Buffers;

/// <summary>
/// An interface that expands <see cref="IBufferWriter{T}"/> with the ability to inspect
/// written data and reset the underlying buffer.
/// </summary>
internal interface IBuffer<T> : IBufferWriter<T>
{
    ReadOnlyMemory<T> WrittenMemory { get; }
    ReadOnlySpan<T> WrittenSpan { get; }
    int WrittenCount { get; }
    int Capacity { get; }
    int FreeCapacity { get; }
    void Clear();
}
