using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using RabbitMQ.Component.Configuration;
using RabbitMQ.Component.Diagnostics;
using RabbitMQ.Component.Internal;
using RabbitMQ.Component.Internal.HighPerformance.Buffers;
using RabbitMQ.Component.Internal.Publishing;
using RabbitMQ.Component.Serialization;

namespace RabbitMQ.Component.Tests;

public sealed class PublishingTests
{
    [Fact]
    public async Task Open_shares_by_message_type_and_routing_key_and_counts_each_reference()
    {
        var transport = new FakeFactory();
        await using var manager = CreateManager(transport);
        var serializer = new CompositeCodec<int>(new IntSerializer());
        PublishSnapshot<int> snapshot = Snapshot(serializer, capacity: 4);

        IPublishEndpoint<int> first = await manager.OpenAsync(snapshot);
        IPublishEndpoint<int> second = await manager.OpenAsync(snapshot);

        Assert.Same(first, second);
        first.Dispose();
        Assert.Equal(EnqueueResult.Accepted, second.TrySend(1));
        second.Dispose();
        await WaitUntilAsync(() => second.TrySend(2) == EnqueueResult.Closed);
        Assert.Throws<InvalidOperationException>(() => second.Dispose());

        IPublishEndpoint<int> reopened = await manager.OpenAsync(snapshot);
        Assert.NotSame(first, reopened);
        reopened.Dispose();
    }

    [Fact]
    public async Task Last_release_waits_for_an_inflight_open_reservation()
    {
        var openReachedCommit = NewSignal();
        var allowOpenCommit = NewSignal();
        bool pauseCommit = false;
        var transport = new FakeFactory();
        await using var manager = CreateManager(transport, beforeOpenCommit: async () =>
        {
            if (!pauseCommit) return;
            openReachedCommit.TrySetResult();
            await allowOpenCommit.Task;
        });
        PublishSnapshot<int> snapshot = Snapshot(new IntSerializer());
        IPublishEndpoint<int> first = await manager.OpenAsync(snapshot);
        pauseCommit = true;

        Task<IPublishEndpoint<int>> opening = manager.OpenAsync(snapshot).AsTask();
        await openReachedCommit.Task.WaitAsync(TimeSpan.FromSeconds(2));
        first.Dispose();

        Assert.Equal(EnqueueResult.Accepted, first.TrySend(1));
        allowOpenCommit.TrySetResult();
        IPublishEndpoint<int> second = await opening;
        Assert.Same(first, second);
        second.Dispose();
    }

    [Fact]
    public async Task Reopen_while_old_generation_drains_creates_an_independent_endpoint()
    {
        var oldStarted = NewSignal();
        var releaseOld = NewSignal();
        int channels = 0;
        var transport = new FakeFactory
        {
            ChannelFactory = () => Interlocked.Increment(ref channels) == 1
                ? new FakeChannel
                {
                    PublishHandler = async (_, cancellationToken) =>
                    {
                        oldStarted.TrySetResult();
                        await releaseOld.Task.WaitAsync(cancellationToken);
                    }
                }
                : new FakeChannel()
        };
        await using var manager = CreateManager(transport);
        PublishSnapshot<int> snapshot = Snapshot(new IntSerializer());
        IPublishEndpoint<int> oldEndpoint = await manager.OpenAsync(snapshot);
        oldEndpoint.TrySend(1);
        await oldStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        oldEndpoint.Dispose();

        IPublishEndpoint<int> reopened = await manager.OpenAsync(snapshot);
        Assert.NotSame(oldEndpoint, reopened);
        Assert.Equal(EnqueueResult.Accepted, reopened.TrySend(2));
        await WaitUntilAsync(() => transport.PublishedCount == 1);
        Assert.Equal(EnqueueResult.Closed, oldEndpoint.TrySend(3));

        releaseOld.TrySetResult();
        reopened.Dispose();
    }

    [Fact]
    public async Task Same_key_rejects_incompatible_configuration()
    {
        var transport = new FakeFactory();
        await using var manager = CreateManager(transport);
        var serializer = new CompositeCodec<int>(new IntSerializer());
        IPublishEndpoint<int> endpoint = await manager.OpenAsync(Snapshot(serializer));

        PublishSnapshot<int> incompatible = Snapshot(serializer, exchangeName: "other");
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await manager.OpenAsync(incompatible));
        endpoint.Dispose();
    }

    [Fact]
    public async Task Failed_initial_declaration_rolls_back_the_cache_entry()
    {
        int channels = 0;
        var transport = new FakeFactory
        {
            ChannelFactory = () => Interlocked.Increment(ref channels) == 1
                ? new FakeChannel { DeclareHandler = (_, _) => throw new InvalidOperationException("precondition failed") }
                : new FakeChannel()
        };
        await using var manager = CreateManager(transport);
        PublishSnapshot<int> snapshot = Snapshot(new IntSerializer());

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await manager.OpenAsync(snapshot));
        IPublishEndpoint<int> endpoint = await manager.OpenAsync(snapshot);

        Assert.Equal(2, channels);
        endpoint.Dispose();
    }

    [Fact]
    public async Task Bounded_queue_serializes_publications_and_reports_full()
    {
        var publishStarted = NewSignal();
        var releasePublish = NewSignal();
        int concurrent = 0;
        int maximumConcurrent = 0;
        var transport = new FakeFactory
        {
            ChannelFactory = () => new FakeChannel
            {
                PublishHandler = async (_, cancellationToken) =>
                {
                    int value = Interlocked.Increment(ref concurrent);
                    maximumConcurrent = Math.Max(maximumConcurrent, value);
                    publishStarted.TrySetResult();
                    await releasePublish.Task.WaitAsync(cancellationToken);
                    Interlocked.Decrement(ref concurrent);
                }
            }
        };
        await using var manager = CreateManager(transport);
        IPublishEndpoint<int> endpoint = await manager.OpenAsync(Snapshot(new IntSerializer(), capacity: 1));

        Assert.Equal(EnqueueResult.Accepted, endpoint.TrySend(1));
        await publishStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(EnqueueResult.Accepted, endpoint.TrySend(2));
        Assert.Equal(EnqueueResult.BufferFull, endpoint.TrySend(3));
        releasePublish.TrySetResult();
        await WaitUntilAsync(() => transport.PublishedCount == 2);

        Assert.Equal(1, maximumConcurrent);
        endpoint.Dispose();
    }

    [Fact]
    public async Task Unknown_publish_is_reported_with_original_message_and_is_not_retried()
    {
        var errors = new ConcurrentQueue<ComponentError>();
        int attempts = 0;
        var transport = new FakeFactory
        {
            ChannelFactory = () => new FakeChannel
            {
                PublishHandler = (_, _) =>
                {
                    Interlocked.Increment(ref attempts);
                    throw new IOException("connection disappeared after publication began");
                }
            }
        };
        await using var manager = CreateManager(transport, errors.Enqueue);
        IPublishEndpoint<int> endpoint = await manager.OpenAsync(Snapshot(new IntSerializer()));

        Assert.Equal(EnqueueResult.Accepted, endpoint.TrySend(42));
        await WaitUntilAsync(() => errors.Any(error => error.Stage == ErrorStage.Publish));
        await Task.Delay(50);

        ComponentError error = Assert.Single(errors, error => error.Stage == ErrorStage.Publish);
        Assert.Equal(ErrorOutcome.Unknown, error.Outcome);
        Assert.Equal(42, error.OriginalMessage);
        Assert.Equal(1, attempts);
        endpoint.Dispose();
    }

    [Fact]
    public async Task Broker_rejection_is_a_definite_failure()
    {
        var errors = new ConcurrentQueue<ComponentError>();
        var transport = new FakeFactory
        {
            ChannelFactory = () => new FakeChannel
            {
                PublishHandler = (_, _) => throw new DefinitePublishException(new InvalidOperationException("nack"))
            }
        };
        await using var manager = CreateManager(transport, errors.Enqueue);
        IPublishEndpoint<int> endpoint = await manager.OpenAsync(Snapshot(new IntSerializer()));

        endpoint.TrySend(7);
        await WaitUntilAsync(() => errors.Any(error => error.Stage == ErrorStage.Publish));

        ComponentError error = Assert.Single(errors, error => error.Stage == ErrorStage.Publish);
        Assert.Equal(ErrorOutcome.Failed, error.Outcome);
        Assert.Equal(7, error.OriginalMessage);
        endpoint.Dispose();
    }

    [Fact]
    public async Task Closed_channel_redeclares_exchange_and_unattempted_message_waits_for_recovery()
    {
        int createAttempts = 0;
        var first = new FakeChannel();
        var recovered = new FakeChannel();
        var transport = new FakeFactory
        {
            ChannelFactory = () =>
            {
                int attempt = Interlocked.Increment(ref createAttempts);
                if (attempt == 1) return first;
                if (attempt is 2 or 3) throw new IOException("channel unavailable");
                return recovered;
            }
        };
        await using var manager = CreateManager(transport, reconnectDelay: TimeSpan.FromMilliseconds(5));
        IPublishEndpoint<int> endpoint = await manager.OpenAsync(Snapshot(new IntSerializer()));
        first.CloseFromBroker();

        Assert.Equal(EnqueueResult.Accepted, endpoint.TrySend(9));
        await WaitUntilAsync(() => recovered.Published.Count == 1);

        Assert.Equal(1, first.Declarations);
        Assert.Equal(1, recovered.Declarations);
        Assert.Equal(9, BitConverter.ToInt32(recovered.Published.Single()));
        endpoint.Dispose();
    }

    [Fact]
    public async Task Last_release_has_its_own_bounded_drain_while_manager_remains_alive()
    {
        var errors = new ConcurrentQueue<ComponentError>();
        int createAttempts = 0;
        var first = new FakeChannel();
        var transport = new FakeFactory
        {
            ChannelFactory = () => Interlocked.Increment(ref createAttempts) == 1
                ? first
                : throw new IOException("channel unavailable")
        };
        await using var manager = CreateManager(transport, errors.Enqueue,
            reconnectDelay: TimeSpan.FromMilliseconds(5), drainTimeout: TimeSpan.FromMilliseconds(50));
        IPublishEndpoint<int> endpoint = await manager.OpenAsync(Snapshot(new IntSerializer()));
        first.CloseFromBroker();
        Assert.Equal(EnqueueResult.Accepted, endpoint.TrySend(12));
        await WaitUntilAsync(() => Volatile.Read(ref createAttempts) >= 2);

        endpoint.Dispose();
        await WaitUntilAsync(() => errors.Any(error => error.Stage == ErrorStage.Drain));
        await Task.Delay(50);
        int attemptsAfterStop = Volatile.Read(ref createAttempts);
        await Task.Delay(100);

        Assert.Equal(attemptsAfterStop, Volatile.Read(ref createAttempts));
        Assert.False(transport.Disposed.Task.IsCompleted);
        Assert.Contains(errors, error => error.Stage == ErrorStage.Drain && error.Exception is TimeoutException);
        Assert.Contains(errors, error => error.Stage == ErrorStage.Drain && Equals(error.OriginalMessage, 12));
    }

    [Fact]
    public async Task Idle_channel_shutdown_reconnects_every_generation_without_a_new_message()
    {
        var channels = new ConcurrentQueue<FakeChannel>();
        var transport = new FakeFactory
        {
            ChannelFactory = () =>
            {
                var channel = new FakeChannel();
                channels.Enqueue(channel);
                return channel;
            }
        };
        await using var manager = CreateManager(transport);
        IPublishEndpoint<int> endpoint = await manager.OpenAsync(Snapshot(new IntSerializer()));

        // Each generation needs its own shutdown signal; an idle worker must notice both closures.
        channels.Single().CloseFromBroker();
        await WaitUntilAsync(() => channels.Count == 2);
        channels.Last().CloseFromBroker();
        await WaitUntilAsync(() => channels.Count == 3);

        Assert.All(channels.Take(2), closed => Assert.Equal(1, closed.DisposeCount));
        Assert.Equal(EnqueueResult.Accepted, endpoint.TrySend(42));
        await WaitUntilAsync(() => channels.Last().Published.Count == 1);
        endpoint.Dispose();
    }

    [Fact]
    public async Task Closed_channel_is_disposed_before_process_path_replaces_it()
    {
        var first = new FakeChannel();
        var recovered = new FakeChannel();
        int channels = 0;
        var transport = new FakeFactory
        {
            ChannelFactory = () => Interlocked.Increment(ref channels) == 1 ? first : recovered
        };
        await using var manager = CreateManager(transport);
        IPublishEndpoint<int> endpoint = await manager.OpenAsync(Snapshot(new IntSerializer()));
        first.MarkClosedWithoutSignal();

        endpoint.TrySend(13);
        await WaitUntilAsync(() => recovered.Published.Count == 1);

        Assert.Equal(1, first.DisposeCount);
        endpoint.Dispose();
    }

    [Fact]
    public async Task Unexpected_worker_failure_closes_endpoint_to_new_sends()
    {
        var errors = new ConcurrentQueue<ComponentError>();
        var transport = new FakeFactory
        {
            ChannelFactory = () => new FakeChannel
            {
                CompletionProvider = () => throw new InvalidOperationException("completion source failed")
            }
        };
        await using var manager = CreateManager(transport, errors.Enqueue);
        IPublishEndpoint<int> endpoint = await manager.OpenAsync(Snapshot(new IntSerializer()));
        await WaitUntilAsync(() => errors.Any(error => error.Stage == ErrorStage.Cleanup));

        Assert.Equal(EnqueueResult.Closed, endpoint.TrySend(20));
        endpoint.Dispose();
    }

    [Fact]
    public async Task Ordinary_serializer_failure_is_reported_and_does_not_stop_the_worker()
    {
        var errors = new ConcurrentQueue<ComponentError>();
        var channel = new FakeChannel();
        var transport = new FakeFactory { ChannelFactory = () => channel };
        await using var manager = CreateManager(transport, errors.Enqueue);
        IPublishEndpoint<int> endpoint = await manager.OpenAsync(Snapshot(new FailFirstSerializer()));

        endpoint.TrySend(1);
        endpoint.TrySend(2);
        await WaitUntilAsync(() => errors.Any(error => error.Stage == ErrorStage.Encoding));
        await WaitUntilAsync(() => transport.PublishedCount == 1);

        ComponentError error = Assert.Single(errors, error => error.Stage == ErrorStage.Encoding);
        Assert.Equal(ErrorOutcome.Failed, error.Outcome);
        Assert.Equal(1, error.OriginalMessage);
        Assert.IsType<FormatException>(error.Exception);
        Assert.Equal(2, BitConverter.ToInt32(channel.Published.Single()));
        endpoint.Dispose();
    }

    [Fact]
    public async Task Owned_payload_uses_declared_length_and_is_disposed_once()
    {
        var serializer = new ConfiguredOwnedSerializer(3);
        var channel = new FakeChannel();
        var transport = new FakeFactory { ChannelFactory = () => channel };
        await using var manager = CreateManager(transport);
        IPublishEndpoint<int> endpoint = await manager.OpenAsync(Snapshot(serializer));

        endpoint.TrySend(10);
        await WaitUntilAsync(() => transport.PublishedCount == 1);
        await WaitUntilAsync(() => serializer.Owner.DisposeCount == 1);

        Assert.Equal(new byte[] { 1, 2, 3 }, channel.Published.Single());
        Assert.Equal(1, serializer.Owner.DisposeCount);
        endpoint.Dispose();
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(5)]
    public async Task Invalid_owned_length_is_reported_and_disposes_the_owner(int length)
    {
        var errors = new ConcurrentQueue<ComponentError>();
        var serializer = new ConfiguredOwnedSerializer(length);
        var transport = new FakeFactory();
        await using var manager = CreateManager(transport, errors.Enqueue);
        IPublishEndpoint<int> endpoint = await manager.OpenAsync(Snapshot(serializer));

        endpoint.TrySend(10);
        await WaitUntilAsync(() => errors.Any(error => error.Stage == ErrorStage.Encoding));

        ComponentError error = Assert.Single(errors, error => error.Stage == ErrorStage.Encoding);
        Assert.Equal(ErrorOutcome.Failed, error.Outcome);
        Assert.Equal(10, error.OriginalMessage);
        Assert.IsType<InvalidOperationException>(error.Exception);
        Assert.Equal(1, serializer.Owner.DisposeCount);
        Assert.Equal(0, transport.PublishedCount);
        endpoint.Dispose();
    }

    [Fact]
    public async Task Null_owned_buffer_is_reported_as_a_serialization_failure()
    {
        var errors = new ConcurrentQueue<ComponentError>();
        var transport = new FakeFactory();
        await using var manager = CreateManager(transport, errors.Enqueue);
        IPublishEndpoint<int> endpoint = await manager.OpenAsync(Snapshot(new NullOwnedSerializer()));

        endpoint.TrySend(10);
        await WaitUntilAsync(() => errors.Any(error => error.Stage == ErrorStage.Encoding));

        ComponentError error = Assert.Single(errors, error => error.Stage == ErrorStage.Encoding);
        Assert.Equal(ErrorOutcome.Failed, error.Outcome);
        Assert.Equal(10, error.OriginalMessage);
        Assert.IsType<InvalidOperationException>(error.Exception);
        Assert.Equal(0, transport.PublishedCount);
        endpoint.Dispose();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Owned_buffer_is_disposed_when_metadata_access_throws(bool throwFromLength)
    {
        var errors = new ConcurrentQueue<ComponentError>();
        var serializer = new ThrowingMetadataOwnedSerializer(throwFromLength);
        var transport = new FakeFactory();
        await using var manager = CreateManager(transport, errors.Enqueue);
        IPublishEndpoint<int> endpoint = await manager.OpenAsync(Snapshot(serializer));

        endpoint.TrySend(10);
        await WaitUntilAsync(() => errors.Any(error => error.Stage == ErrorStage.Encoding));

        ComponentError error = Assert.Single(errors, error => error.Stage == ErrorStage.Encoding);
        Assert.IsType<FormatException>(error.Exception);
        Assert.Equal(1, serializer.Owner.DisposeCount);
        Assert.Equal(0, transport.PublishedCount);
        endpoint.Dispose();
    }

    [Fact]
    public async Task Throwing_owned_buffer_dispose_is_reported_without_stopping_the_worker()
    {
        var errors = new ConcurrentQueue<ComponentError>();
        var transport = new FakeFactory();
        await using var manager = CreateManager(transport, errors.Enqueue);
        IPublishEndpoint<int> endpoint = await manager.OpenAsync(Snapshot(new ThrowingOwnedSerializer()));

        endpoint.TrySend(21);
        endpoint.TrySend(22);
        await WaitUntilAsync(() => transport.PublishedCount == 2);
        await WaitUntilAsync(() => errors.Count(error => error.Stage == ErrorStage.Cleanup) == 2);
        Assert.Equal(EnqueueResult.Accepted, endpoint.TrySend(23));
        await WaitUntilAsync(() => transport.PublishedCount == 3);
        await WaitUntilAsync(() => errors.Count(error => error.Stage == ErrorStage.Cleanup) == 3);

        Assert.Equal(3, errors.Count(error => error.Stage == ErrorStage.Cleanup));
        endpoint.Dispose();
    }

    [Fact]
    public async Task Owned_payload_is_released_only_after_publish_completes()
    {
        var publishStarted = NewSignal();
        var releasePublish = NewSignal();
        var serializer = new OwnedIntSerializer();
        var transport = new FakeFactory
        {
            ChannelFactory = () => new FakeChannel
            {
                PublishHandler = async (_, cancellationToken) =>
                {
                    publishStarted.TrySetResult();
                    await releasePublish.Task.WaitAsync(cancellationToken);
                }
            }
        };
        await using var manager = CreateManager(transport);
        IPublishEndpoint<int> endpoint = await manager.OpenAsync(Snapshot(serializer));

        endpoint.TrySend(11);
        await publishStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(0, serializer.Owner!.DisposeCount);
        releasePublish.TrySetResult();
        await WaitUntilAsync(() => serializer.Owner!.DisposeCount == 1);
        Assert.Equal(1, serializer.Owner.DisposeCount);
        endpoint.Dispose();
    }

    [Fact]
    public async Task Drain_timeout_reports_inflight_publication_as_unknown_and_retains_its_payload()
    {
        var publishStarted = NewSignal();
        var releasePublish = NewSignal();
        var errors = new ConcurrentQueue<ComponentError>();
        var serializer = new OwnedIntSerializer();
        var transport = new FakeFactory
        {
            ChannelFactory = () => new FakeChannel
            {
                PublishHandler = async (_, _) =>
                {
                    publishStarted.TrySetResult();
                    await releasePublish.Task;
                }
            }
        };
        var manager = CreateManager(transport, errors.Enqueue, drainTimeout: TimeSpan.FromMilliseconds(50));
        try
        {
            IPublishEndpoint<int> endpoint = await manager.OpenAsync(Snapshot(serializer));
            Assert.Equal(EnqueueResult.Accepted, endpoint.TrySend(17));
            await publishStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
            endpoint.Dispose();
            await manager.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2));

            ComponentError timeout = Assert.Single(errors, error => error.Stage == ErrorStage.Drain);
            Assert.Equal(ErrorOutcome.Unknown, timeout.Outcome);
            Assert.Equal(17, timeout.OriginalMessage);
            Assert.False(serializer.Owner!.Disposed);
        }
        finally
        {
            releasePublish.TrySetResult();
            await manager.DisposeAsync();
            await transport.Disposed.Task.WaitAsync(TimeSpan.FromSeconds(2));
        }
        Assert.Equal(1, serializer.Owner!.DisposeCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Dispose_is_bounded_after_workers_finish_when_connection_cleanup_is_stuck(bool cleanupFails)
    {
        var disposalStarted = NewSignal();
        var releaseDisposal = NewSignal();
        var cleanupFailureReported = NewSignal();
        var errors = new ConcurrentQueue<ComponentError>();
        var failure = new IOException("connection cleanup failed");
        var channel = new FakeChannel();
        var transport = new FakeFactory
        {
            ChannelFactory = () => channel,
            ConnectionDisposeHandler = async () =>
            {
                disposalStarted.TrySetResult();
                await releaseDisposal.Task;
                if (cleanupFails) throw failure;
            }
        };
        var manager = CreateManager(transport, error =>
        {
            errors.Enqueue(error);
            if (error.Stage == ErrorStage.Cleanup) cleanupFailureReported.TrySetResult();
        }, drainTimeout: TimeSpan.FromMilliseconds(50));

        try
        {
            IPublishEndpoint<int> endpoint = await manager.OpenAsync(Snapshot(new IntSerializer()));
            endpoint.Dispose();
            // Await the worker itself, not just channel disposal, before exercising connection shutdown.
            await ((IPublishEndpointControl)endpoint).BeginClose().WaitAsync(TimeSpan.FromSeconds(2));
            Assert.Equal(1, channel.DisposeCount);
            Assert.False(transport.Disposed.Task.IsCompleted);

            Task disposing = manager.DisposeAsync().AsTask();
            await disposalStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
            await disposing.WaitAsync(TimeSpan.FromSeconds(2));

            Assert.False(releaseDisposal.Task.IsCompleted);
            Assert.False(transport.Disposed.Task.IsCompleted);
            Assert.Equal(EnqueueResult.Closed, endpoint.TrySend(1));
            ComponentError timeout = Assert.Single(errors);
            Assert.Equal(ErrorStage.Drain, timeout.Stage);
            Assert.Equal(ErrorOutcome.Unknown, timeout.Outcome);
            Assert.Equal(BrokerRole.Publish, timeout.Role);
            Assert.Equal("publisher-connection", timeout.Resource);
            Assert.IsType<TimeoutException>(timeout.Exception);
            Assert.Null(timeout.OriginalMessage);
        }
        finally
        {
            releaseDisposal.TrySetResult();
            await manager.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2));
            await transport.Disposed.Task.WaitAsync(TimeSpan.FromSeconds(2));
        }

        if (cleanupFails)
        {
            await cleanupFailureReported.Task.WaitAsync(TimeSpan.FromSeconds(2));
            ComponentError cleanup = Assert.Single(errors, error => error.Stage == ErrorStage.Cleanup);
            Assert.Equal(ErrorOutcome.Failed, cleanup.Outcome);
            Assert.Equal("publisher-connection", cleanup.Resource);
            Assert.Same(failure, cleanup.Exception);
        }
        else
        {
            Assert.Single(errors);
        }
        Assert.Equal(1, channel.DisposeCount);
    }

    [Fact]
    public async Task Dispose_is_bounded_when_synchronous_serializer_is_stuck_and_cleanup_continues_in_background()
    {
        var entered = new ManualResetEventSlim();
        var release = new ManualResetEventSlim();
        var errors = new ConcurrentQueue<ComponentError>();
        var serializer = new BlockingSerializer(entered, release);
        var transport = new FakeFactory();
        var manager = CreateManager(transport, errors.Enqueue, drainTimeout: TimeSpan.FromMilliseconds(50));
        IPublishEndpoint<int> endpoint = await manager.OpenAsync(Snapshot(serializer));
        endpoint.TrySend(5);
        Assert.True(entered.Wait(TimeSpan.FromSeconds(2)));
        endpoint.Dispose();

        var stopwatch = Stopwatch.StartNew();
        await manager.DisposeAsync();
        stopwatch.Stop();

        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(1));
        ComponentError timeout = Assert.Single(errors, error => error.Stage == ErrorStage.Drain);
        Assert.Equal(5, timeout.OriginalMessage);
        release.Set();
        await transport.Disposed.Task.WaitAsync(TimeSpan.FromSeconds(2));
    }

    [Theory]
    [InlineData("null")]
    [InlineData("length")]
    [InlineData("memory")]
    [InlineData("invalid")]
    public async Task Custom_owned_codec_metadata_failures_are_reported_and_owned_memory_released(string failure)
    {
        IOwnedBuffer? owner = failure switch
        {
            "null" => null,
            "length" => new ThrowingMetadataOwner(true),
            "memory" => new ThrowingMetadataOwner(false),
            _ => new CountingOwner(5)
        };
        var errors = new ConcurrentQueue<ComponentError>();
        var transport = new FakeFactory();
        await using var manager = CreateManager(transport, errors.Enqueue);
        var codec = new DelegateOwnedCodec(() => owner!);
        IPublishEndpoint<int> endpoint = await manager.OpenAsync(Snapshot(codec));
        endpoint.TrySend(1);
        endpoint.Dispose();
        await ((IPublishEndpointControl)endpoint).BeginClose().WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Single(errors, e => e.Stage == ErrorStage.Encoding && e.Outcome == ErrorOutcome.Failed);
        Assert.Equal(0, transport.PublishedCount);
        if (owner is ThrowingMetadataOwner throwing) Assert.Equal(1, throwing.DisposeCount);
        if (owner is CountingOwner counting) Assert.Equal(1, counting.DisposeCount);
    }

    [Fact]
    public async Task Nonowned_codec_encode_failure_does_not_stop_worker()
    {
        var errors = new ConcurrentQueue<ComponentError>();
        var channel = new FakeChannel();
        var transport = new FakeFactory { ChannelFactory = () => channel };
        await using var manager = CreateManager(transport, errors.Enqueue);
        IPublishEndpoint<int> endpoint = await manager.OpenAsync(Snapshot(new PlainCodec(new FailFirstSerializer())));
        endpoint.TrySend(1);
        endpoint.TrySend(2);
        endpoint.Dispose();
        await ((IPublishEndpointControl)endpoint).BeginClose().WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Single(errors, e => e.Stage == ErrorStage.Encoding);
        Assert.Equal(1, transport.PublishedCount);
        Assert.Equal(sizeof(int), channel.Published.Single().Length);
        Assert.Equal(2, BitConverter.ToInt32(channel.Published.Single()));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Compressed_output_survives_drain_timeout_until_actual_transport_completion(bool transportFails)
    {
        var started = NewSignal();
        var release = NewSignal();
        var serializer = new OwnedIntSerializer();
        var codec = new TrackingFallbackCodec(new CompositeCodec<int>(serializer, new GzipCompressor()));
        var errors = new ConcurrentQueue<ComponentError>();
        var transport = new FakeFactory
        {
            ChannelFactory = () => new FakeChannel
            {
                PublishHandler = async (body, _) =>
                {
                    Assert.Equal(1, serializer.Owner!.DisposeCount);
                    started.TrySetResult();
                    await release.Task;
                    Assert.True(codec.Output!.WrittenCount > 0);
                    var decoded = new ArrayBufferWriter<byte>();
                    new GzipCompressor().Decompress(body.Span, decoded);
                    Assert.Equal(123, BitConverter.ToInt32(decoded.WrittenSpan));
                    if (transportFails) throw new IOException("after borrowing ends");
                }
            }
        };
        var manager = CreateManager(transport, errors.Enqueue, drainTimeout: TimeSpan.FromMilliseconds(50));
        try
        {
            IPublishEndpoint<int> endpoint = await manager.OpenAsync(Snapshot(codec));
            endpoint.TrySend(123);
            await started.Task.WaitAsync(TimeSpan.FromSeconds(2));
            endpoint.Dispose();
            await manager.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2));
            Assert.True(codec.Output!.WrittenCount > 0);
            Assert.Contains(errors, e => e.Stage == ErrorStage.Drain && e.Outcome == ErrorOutcome.Unknown);
        }
        finally
        {
            release.TrySetResult();
            await manager.DisposeAsync();
            await transport.Disposed.Task.WaitAsync(TimeSpan.FromSeconds(2));
        }
        Assert.Equal(1, serializer.Owner!.DisposeCount);
        if (transportFails) Assert.Contains(errors, e => e.Stage == ErrorStage.Publish && e.Outcome == ErrorOutcome.Unknown);
    }

    private sealed class PlainCodec(ISerializer<int> serializer) : ICodec<int>
    {
        public void Encode(int message, IBufferWriter<byte> output) => serializer.Serialize(message, output);
        public Span<byte> Encode(int message) => throw new NotSupportedException();
        public int Decode(ReadOnlySpan<byte> input) => throw new NotSupportedException();
    }

    private sealed class DelegateOwnedCodec(Func<IOwnedBuffer> encode) : IOwnedBufferCodec<int>
    {
        public bool TryEncodeOwned(int message, out IOwnedBuffer? owner)
        {
            owner = encode();
            return true;
        }
        public void Encode(int message, IBufferWriter<byte> output) => throw new NotSupportedException();
        public Span<byte> Encode(int message) => throw new NotSupportedException();
        public int Decode(ReadOnlySpan<byte> input) => throw new NotSupportedException();
    }

    private sealed class TrackingFallbackCodec(ICodec<int> inner) : ICodec<int>
    {
        public OwnedArrayPoolBufferWriter? Output { get; private set; }

        public void Encode(int message, IBufferWriter<byte> output)
        {
            Output = Assert.IsType<OwnedArrayPoolBufferWriter>(output);
            inner.Encode(message, output);
        }

        public Span<byte> Encode(int message) => inner.Encode(message);
        public int Decode(ReadOnlySpan<byte> input) => inner.Decode(input);
    }

    private static PublisherManager CreateManager(FakeFactory transport, Action<ComponentError>? observer = null,
        TimeSpan? reconnectDelay = null, TimeSpan? drainTimeout = null, Func<Task>? beforeOpenCommit = null)
    {
        TimeSpan reconnect = reconnectDelay ?? TimeSpan.FromMilliseconds(10);
        var options = new ServiceSnapshot(new BrokerConnectionOptions(), new BrokerConnectionOptions(), reconnect,
            reconnect, drainTimeout ?? TimeSpan.FromSeconds(2), observer);
        return new PublisherManager(options, new ErrorSink(observer), transport, beforeOpenCommit);
    }

    private static PublishSnapshot<int> Snapshot(ISerializer<int> serializer, int capacity = 8,
        string exchangeName = "events") => Snapshot(new CompositeCodec<int>(serializer), capacity, exchangeName);

    private static PublishSnapshot<int> Snapshot(ICodec<int> serializer, int capacity = 8,
        string exchangeName = "events") => new(
        new ExchangeSnapshot(exchangeName, "direct", true, false, AmqpTable.Empty),
        "orders.created", serializer, capacity);

    private static TaskCompletionSource NewSignal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static async Task WaitUntilAsync(Func<bool> predicate, TimeSpan? timeout = null)
    {
        var stopwatch = Stopwatch.StartNew();
        TimeSpan limit = timeout ?? TimeSpan.FromSeconds(2);
        while (!predicate())
        {
            if (stopwatch.Elapsed >= limit) throw new TimeoutException("Condition was not reached.");
            await Task.Delay(10);
        }
    }

    private sealed class IntSerializer : ISerializer<int>
    {
        public void Serialize(int message, IBufferWriter<byte> writer)
        {
            Span<byte> span = writer.GetSpan(sizeof(int));
            BitConverter.TryWriteBytes(span, message);
            writer.Advance(sizeof(int));
        }

        public int Deserialize(ReadOnlySpan<byte> body) => throw new NotSupportedException();
    }

    private sealed class FailFirstSerializer : ISerializer<int>
    {
        public void Serialize(int message, IBufferWriter<byte> writer)
        {
            Span<byte> span = writer.GetSpan(sizeof(int));
            BitConverter.TryWriteBytes(span, message);
            writer.Advance(sizeof(int));
            if (message == 1) throw new FormatException("invalid message after a partial output");
        }

        public int Deserialize(ReadOnlySpan<byte> body) => throw new NotSupportedException();
    }

    private sealed class ConfiguredOwnedSerializer(int length) : IOwnedBufferSerializer<int>
    {
        public CountingOwner Owner { get; } = new(length);
        public IOwnedBuffer SerializeOwned(int message) => Owner;
        public void Serialize(int message, IBufferWriter<byte> writer) =>
            throw new InvalidOperationException("Owned path was not used.");
        public int Deserialize(ReadOnlySpan<byte> body) => throw new NotSupportedException();
    }

    private sealed class CountingOwner(int length) : IOwnedBuffer
    {
        public Memory<byte> Memory { get; } = new byte[] { 1, 2, 3, 4 };
        public int Length => length;
        public int DisposeCount { get; private set; }
        public void Dispose() => DisposeCount++;
    }

    private sealed class NullOwnedSerializer : IOwnedBufferSerializer<int>
    {
        public IOwnedBuffer SerializeOwned(int message) => null!;
        public void Serialize(int message, IBufferWriter<byte> writer) => throw new NotSupportedException();
        public int Deserialize(ReadOnlySpan<byte> body) => throw new NotSupportedException();
    }

    private sealed class ThrowingMetadataOwnedSerializer(bool throwFromLength) : IOwnedBufferSerializer<int>
    {
        public ThrowingMetadataOwner Owner { get; } = new(throwFromLength);
        public IOwnedBuffer SerializeOwned(int message) => Owner;
        public void Serialize(int message, IBufferWriter<byte> writer) => throw new NotSupportedException();
        public int Deserialize(ReadOnlySpan<byte> body) => throw new NotSupportedException();
    }

    private sealed class ThrowingMetadataOwner(bool throwFromLength) : IOwnedBuffer
    {
        public Memory<byte> Memory => throwFromLength
            ? new byte[sizeof(int)]
            : throw new FormatException("memory unavailable");
        public int Length => throwFromLength
            ? throw new FormatException("length unavailable")
            : sizeof(int);
        public int DisposeCount { get; private set; }
        public void Dispose() => DisposeCount++;
    }

    private sealed class BlockingSerializer(ManualResetEventSlim entered, ManualResetEventSlim release) : ISerializer<int>
    {
        public void Serialize(int message, IBufferWriter<byte> writer)
        {
            entered.Set();
            release.Wait();
            Span<byte> span = writer.GetSpan(sizeof(int));
            BitConverter.TryWriteBytes(span, message);
            writer.Advance(sizeof(int));
        }

        public int Deserialize(ReadOnlySpan<byte> body) => throw new NotSupportedException();
    }

    private sealed class ThrowingOwnedSerializer : IOwnedBufferSerializer<int>
    {
        public IOwnedBuffer SerializeOwned(int message) => new ThrowingOwner(message);
        public void Serialize(int message, IBufferWriter<byte> writer) => throw new NotSupportedException();
        public int Deserialize(ReadOnlySpan<byte> body) => throw new NotSupportedException();

        private sealed class ThrowingOwner : IOwnedBuffer
        {
            private readonly byte[] _bytes = new byte[sizeof(int)];
            public ThrowingOwner(int value) => BitConverter.TryWriteBytes(_bytes, value);
            public int Length => _bytes.Length;
            public Memory<byte> Memory => _bytes;
            public void Dispose() => throw new InvalidOperationException("owner cleanup failed");
        }
    }

    private sealed class OwnedIntSerializer : IOwnedBufferSerializer<int>
    {
        public TestOwner? Owner { get; private set; }
        public IOwnedBuffer SerializeOwned(int message)
        {
            Owner = new TestOwner(message);
            return Owner;
        }
        public void Serialize(int message, IBufferWriter<byte> writer) => throw new NotSupportedException();
        public int Deserialize(ReadOnlySpan<byte> body) => throw new NotSupportedException();
    }

    private sealed class TestOwner : IOwnedBuffer
    {
        private readonly byte[] _bytes = new byte[sizeof(int)];
        public TestOwner(int value) => BitConverter.TryWriteBytes(_bytes, value);
        public int Length => _bytes.Length;
        public Memory<byte> Memory => _bytes;
        public int DisposeCount { get; private set; }
        public bool Disposed => DisposeCount != 0;
        public void Dispose() => DisposeCount++;
    }

    private sealed class FakeFactory : IPublishTransportFactory
    {
        public Func<FakeChannel> ChannelFactory { get; init; } = () => new FakeChannel();
        public Func<ValueTask>? ConnectionDisposeHandler { get; init; }
        public TaskCompletionSource Disposed { get; } = NewSignal();
        public int PublishedCount;
        public ValueTask<IPublishTransportConnection> ConnectAsync(BrokerConnectionOptions options,
            CancellationToken cancellationToken) => new(new FakeConnection(this));

        private sealed class FakeConnection(FakeFactory owner) : IPublishTransportConnection
        {
            private bool _open = true;
            public bool IsOpen => _open;
            public ValueTask<IPublishTransportChannel> CreateChannelAsync(CancellationToken cancellationToken)
            {
                FakeChannel channel = owner.ChannelFactory();
                channel.OnPublished = () => Interlocked.Increment(ref owner.PublishedCount);
                return new(channel);
            }
            public async ValueTask DisposeAsync()
            {
                _open = false;
                try
                {
                    if (owner.ConnectionDisposeHandler is not null)
                        await owner.ConnectionDisposeHandler();
                }
                finally
                {
                    owner.Disposed.TrySetResult();
                }
            }
        }
    }

    private sealed class FakeChannel : IPublishTransportChannel
    {
        private readonly TaskCompletionSource _completion = NewSignal();
        private bool _open = true;
        public Func<ExchangeSnapshot, CancellationToken, ValueTask> DeclareHandler { get; init; } = (_, _) => ValueTask.CompletedTask;
        public Func<ReadOnlyMemory<byte>, CancellationToken, ValueTask> PublishHandler { get; init; } = (_, _) => ValueTask.CompletedTask;
        public Func<Task>? CompletionProvider { get; init; }
        public Action? OnPublished { get; set; }
        public List<byte[]> Published { get; } = [];
        public int Declarations { get; private set; }
        public int DisposeCount { get; private set; }
        public bool IsOpen => _open;
        public Task Completion => CompletionProvider?.Invoke() ?? _completion.Task;

        public ValueTask DeclareExchangeAsync(ExchangeSnapshot exchange, CancellationToken cancellationToken)
        {
            Declarations++;
            return DeclareHandler(exchange, cancellationToken);
        }

        public async ValueTask PublishAsync(string exchange, string routingKey, ReadOnlyMemory<byte> body,
            CancellationToken cancellationToken)
        {
            await PublishHandler(body, cancellationToken);
            Published.Add(body.ToArray());
            OnPublished?.Invoke();
        }

        public void CloseFromBroker()
        {
            _open = false;
            _completion.TrySetResult();
        }

        public void MarkClosedWithoutSignal() => _open = false;

        public ValueTask DisposeAsync()
        {
            DisposeCount++;
            _open = false;
            _completion.TrySetResult();
            return ValueTask.CompletedTask;
        }
    }
}
