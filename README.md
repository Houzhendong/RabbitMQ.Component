# RabbitMQ.MessagePort

面向 **.NET 8（`net8.0`）** 的 RabbitMQ 组件，提供有界内存发布缓冲、共享发布端点、显式 Receiver/Binding、可组合编码/压缩和仅订阅侧的 Broker 切换。订阅消息解码后直接同步调用配置的 `Action<T>`。

> NuGet 包名为 `RabbitMQ.MessagePort`；为保持兼容，DLL 和根命名空间仍为 `RabbitMQ.Component`，DI 注册 API 仍为 `AddRabbitMQComponent`。该包目前尚未发布。
>
> 组件面向 `net8.0`，真实连接信息只应从运行环境或安全配置提供。不要把含用户名和密码的 AMQP URI 写入源码、日志或错误观察器输出。

## 公共 API 与最小用法

- `IRabbitMQService : IAsyncDisposable`：打开发布端点、打开 Receiver、切换订阅 Broker。
- `IPublishEndpoint<T> : IDisposable`：使用 `TrySend(T)` 入队。
- `IReceiver<T>`：仅提供 `BindAsync`，**没有公开的 `Dispose`**。
- `IQueueBinding : IDisposable`：调用方持有和释放的订阅引用。
- `ICodec<T>`：支持写入调用方提供的 `IBufferWriter<byte>`、返回精确长度 `Span<byte>` 的便捷编码，以及从 `ReadOnlySpan<byte>` 解码；发布和订阅配置均必须提供 `Codec`。
- `CompositeCodec<T>`：组合必需的 `ISerializer<T>` 与可选的 `ICompressor`，无压缩时可提供 `IOwnedBufferCodec<T>` 原生 owner 快路径。
- `GzipCompressor` / `BrotliCompressor`：使用 .NET 内置压缩。
- `ZstdCompressor`：Zstandard 压缩，基于核心包依赖的 [ZstdSharp.Port](https://github.com/oleg-st/ZstdSharp)（MIT，纯托管实现，无原生二进制）。

### 构造与 DI 注册

直接构造时，`RabbitMQService` 会在构造函数中对全部服务配置生成快照；之后修改原 options 不会改变该服务实例。服务拥有发布和订阅连接，必须异步释放：

```csharp
var primaryUri = new Uri(
    Environment.GetEnvironmentVariable("RABBITMQ_PRIMARY_URI")
    ?? throw new InvalidOperationException("RABBITMQ_PRIMARY_URI is required."));
var secondaryUri = new Uri(
    Environment.GetEnvironmentVariable("RABBITMQ_SECONDARY_URI")
    ?? throw new InvalidOperationException("RABBITMQ_SECONDARY_URI is required."));

await using IRabbitMQService service = new RabbitMQService(
    new RabbitMQServiceOptions
    {
        PublishBroker = new BrokerConnectionOptions { Uri = primaryUri },
        SubscriptionBroker = new BrokerConnectionOptions { Uri = secondaryUri },
        ReconnectMinDelay = TimeSpan.FromMilliseconds(250),
        ReconnectMaxDelay = TimeSpan.FromSeconds(30),
        DrainTimeout = TimeSpan.FromSeconds(10),
        ErrorObserver = error => Console.Error.WriteLine(
            $"RabbitMQ error: role={error.Role}, stage={error.Stage}, " +
            $"outcome={error.Outcome}, resource={error.Resource}, " +
            $"exceptionType={error.Exception.GetType().Name}")
    });
```

`RabbitMQService` 也接受可选的 `ILogger<RabbitMQService>`。默认错误日志和上例观察器都不输出消息正文、异常文本或连接 URI；应用自己的观察器也必须遵守相同的脱敏边界。

Microsoft DI 提供两个 singleton 注册重载：传入已经构造的 `RabbitMQServiceOptions`，或传入同步配置委托。容器拥有服务时，应异步释放容器，不要再手动释放所解析出的同一个服务：

```csharp
var services = new ServiceCollection();
services.AddLogging();
services.AddRabbitMQComponent(options =>
{
    options.PublishBroker = new BrokerConnectionOptions { Uri = primaryUri };
    options.SubscriptionBroker = new BrokerConnectionOptions { Uri = secondaryUri };
    options.DrainTimeout = TimeSpan.FromSeconds(10);
    options.ErrorObserver = ObserveRabbitMQErrorSynchronously;
});

await using ServiceProvider provider = services.BuildServiceProvider();
IRabbitMQService service = provider.GetRequiredService<IRabbitMQService>();
```

配置委托和 `ErrorObserver` 必须同步执行，不能使用 `async void`。`AddRabbitMQComponent(optionsInstance)` 与 `AddRabbitMQComponent(configure)` 都注册同一个 `IRabbitMQService` singleton；`ServiceProvider.DisposeAsync()` 会等待组件的异步清理。

以下片段假定应用已经取得 `IRabbitMQService service`。如果服务由 DI 容器管理，应由容器负责最终释放，不要重复管理同一服务的所有权。

```csharp
using RabbitMQ.Component;
using RabbitMQ.Component.Configuration;
using RabbitMQ.Component.Serialization;

public sealed record OrderCreated(Guid Id, decimal Amount);

public static async Task RunAsync(
    IRabbitMQService service,
    Action<OrderCreated> handleSynchronously,
    CancellationToken cancellationToken)
{
    var codec = new CompositeCodec<OrderCreated>(JsonMessageSerializer<OrderCreated>.Default);
    var exchange = new ExchangeConfig
    {
        Name = "example.orders",
        Type = "topic",
        Durable = true,
        AutoDelete = false
    };

    var receiver = await service.OpenReceiverAsync(
        new SubscriptionConfig<OrderCreated>
        {
            Queue = new QueueConfig
            {
                Name = "example.orders.worker",
                Durable = true,
                Exclusive = false,
                AutoDelete = false
            },
            Consumer = new ConsumerConfig
            {
                AutoAck = false,
                ConsumerTag = "", // 空字符串表示由 Broker 生成
                NoLocal = false,
                Exclusive = false,
                Arguments = new Dictionary<string, object?>
                {
                    ["x-priority"] = 5
                }
            },
            Codec = codec,
            PrefetchCount = 32
        },
        handleSynchronously,
        cancellationToken);

    // 首次 Bind 才声明订阅拓扑并启动消费。
    using var binding = await receiver.BindAsync(new BindingConfig
    {
        Exchange = exchange,
        RoutingKey = "order.created"
    }, cancellationToken);

    using var publisher = await service.OpenPublishEndpointAsync(
        new PublishConfig<OrderCreated>
        {
            Exchange = exchange,
            RoutingKey = "order.created",
            Codec = codec,
            BufferCapacity = 1024
        }, cancellationToken);

    var result = publisher.TrySend(new OrderCreated(Guid.NewGuid(), 12.50m));
    if (result != EnqueueResult.Accepted)
    {
        // 根据业务需要实施限流、记录或上游重试；不能当作已投递。
        throw new InvalidOperationException($"入队失败：{result}");
    }

    // 示例保持订阅到取消；Accepted 本身不表示消费者已经处理该消息。
    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
}
```

同一个 Receiver 可以继续绑定其他 Exchange 或 routing key。不要在最后一个 Binding 释放后复用旧 Receiver；需要订阅时重新 `OpenReceiverAsync`。

### TrySend 的边界

| 结果 | 含义 |
| --- | --- |
| `Accepted` | 已进入本地有界内存缓冲，不表示发送成功、confirm 成功、成功路由或消费完成。 |
| `BufferFull` | 本地缓冲已满，消息未被接受。 |
| `Unavailable` | 当前不可接受发送，消息未被接受。 |
| `Closed` | 端点已关闭，消息未被接受。 |

每个发布端点由一个串行 worker 执行序列化和发送。**被接受的消息及其可达的可变对象不得在入队后修改**；序列化可能晚于 `TrySend` 返回。优先使用真正不可变的 DTO，不要把内部含可变集合的 record 当作深度不可变对象。

## 共享、引用计数与释放

### 发布端点

服务实例内部严格按 **`(typeof(T), RoutingKey)`** 共享发布端点。同 key 的成功 Open 返回**同一个对象**，不是独立租约；每次成功 Open 增加一次引用。

- **N 次成功 Open 必须对应 N 次 `Dispose`，一一归还。`Dispose` 不幂等。**
- 不可因引用相等而少归还，也不可因“安全起见”反复 Dispose；多归还可能耗尽其他持有者的引用。
- 同 key 的 Exchange、codec、缓冲容量等必须兼容。不允许同 routing key、不同 Exchange 被静默复用。
- 不要假设两个分别创建但配置相似的 codec 一定兼容；快照按引用比较 codec，共享时复用同一个线程安全 codec 实例。
- 最后一次释放停止接受新消息，并按服务级超时策略完成有界排空；这不是进程崩溃后的持久恢复机制。

例如两次 Open 得到 `a`、`b`，即使 `ReferenceEquals(a, b)` 为 true，也需要各归还一次。使用两条对应 Open 的 `using` 可以表达该关系；不要再对它们额外手动 Dispose。

### Receiver 与 Binding

- Receiver 在订阅侧按 `Queue.Name` 共享；同 Queue 必须使用兼容的配置、同一个消息类型、codec 实例和 `Action<T>`，只有一个 Consumer 和一个同步回调。希望同 Queue 使用不同回调，应改用不同队列，而不是把 Open 当作添加监听器。
- `OpenReceiverAsync` 本身不增加调用方引用。未绑定 Receiver 只保存逻辑状态，不启动 Consumer。
- Binding 的身份包括 Exchange 名、Queue 名、routing key 和 Arguments。同一 Binding 重复 Bind 返回同一个对象并增加引用，**每次成功 Bind 对应一次 Dispose，Binding 的 Dispose 同样不幂等**。
- 只有某个 Binding 的最后一个本地引用释放时才撤销该 binding；其他 Binding 可以继续使用同一 Receiver。
- 所有 Binding 引用归零后，组件内部停止向该 Receiver 交付、关闭 Consumer/session 并移除 Receiver 缓存。**旧 Receiver 再 Bind 会抛出 `ObjectDisposedException`，不会复活。**
- 调用方只释放 Binding，不释放 Receiver。未绑定过的预备 Receiver 由服务最终清理。

**部署前提：同一实际 binding 由一个服务实例管理。** 不存在跨实例或跨进程的全局引用计数；若多个服务实例/进程共同管理同一 Exchange、Queue、routing key、Arguments 的 binding，其中一个实例最后释放时执行 unbind，可能破坏其他实例的订阅。应明确拓扑所有者，或使用互不重叠的队列/binding。

端点和 Binding 的 `Dispose` 立即改变逻辑状态，网络收尾异步完成；`IRabbitMQService.DisposeAsync()` 在配置的期限内等待排空和清理。组件不以“释放”为由主动删除持久化 Queue/Exchange，但 RabbitMQ 的 AutoDelete、Exclusive、TTL 等规则仍然生效。

`DrainTimeout` 限制的是组件等待的时间，不能强制终止调用方的同步 codec 或 handler。超时会通过错误入口报告；仍在使用的载荷、回调依赖和必要资源要等相关操作实际结束后在后台清理，不能提前释放或复用仍被借用的正文内存。已经在途的发布在排空超时时不能判定为确定发送失败，应按 `Unknown` 处理。发布端点独立清理与服务释放可能并发观察同一次超时；组件通过共享完成任务收敛 Drain 诊断，后到路径会等待同一次同步错误报告完成，避免 `DisposeAsync` 已返回而观察器尚未收到该诊断。这个协调只保证诊断交付顺序，不会提前释放仍被 transport 借用的载荷。请保持回调短小、避免无限阻塞，也不要把 `DisposeAsync` 返回解释为任意用户代码都已被强制停止。

消费侧正常投递只维护在途计数，不会为每条消息创建排空用的 `TaskCompletionSource`；只有关闭时确实仍有在途 handler，才按需创建并复用一个排空信号。反序列化完成后直接调用配置的 `Action<T>`，回调分发本身不创建额外的包装对象或逐消息委托；这些局部优化**不表示反序列化、消息对象、RabbitMQ 回调或整条消费管线零分配，也不代表未经测量的整体性能提升**。

## RabbitMQ 原生配置与声明约束

配置直接表达 RabbitMQ 拓扑，不另造 Exchange 类型枚举或隐藏 Broker 的声明错误。

| 配置 | 字段及用途 |
| --- | --- |
| `ExchangeConfig` | `Name`、开放字符串 `Type`、`Durable`、`AutoDelete`、`Arguments`。支持 direct/topic/fanout/headers 等原生类型；插件类型要求目标 Broker 已安装相应插件。 |
| `QueueConfig` | `Name`、`Durable`、`Exclusive`、`AutoDelete`、`Arguments`。队列名称必须非空，不支持服务端自动命名。 |
| `PublishConfig<T>` | `Exchange`、`RoutingKey`、`Codec`、`BufferCapacity`；缓冲容量必须为正数。 |
| `SubscriptionConfig<T>` | `Queue`、`Consumer`、`Codec`、`PrefetchCount`；prefetch 为 0 表示 AMQP 的无限制语义，并非不消费。 |
| `ConsumerConfig` | `AutoAck`、`ConsumerTag`、`NoLocal`、`Exclusive`、`Arguments`，完整对应 `BasicConsume` 的消费参数；空 `ConsumerTag` 由 Broker 生成。 |
| `BindingConfig` | `Exchange`、`RoutingKey`、`Arguments`；Queue 由 Receiver 决定。 |

### 四处 Arguments 各自独立

`ExchangeConfig.Arguments`、`QueueConfig.Arguments`、`BindingConfig.Arguments`、`ConsumerConfig.Arguments` 都是 AMQP field-table 参数，不可相互替代。例如：

```csharp
// QueueConfig.Arguments：TTL、队列长度、死信等由 Broker 执行。
var queueArguments = new Dictionary<string, object?>
{
    ["x-message-ttl"] = 60_000,
    ["x-max-length"] = 10_000,
    ["x-dead-letter-exchange"] = "example.dead"
};

// BindingConfig.Arguments：headers exchange 的匹配条件。
var bindingArguments = new Dictionary<string, object?>
{
    ["x-match"] = "all",
    ["event-kind"] = "order"
};

// ExchangeConfig.Arguments：例如 alternate exchange。
var exchangeArguments = new Dictionary<string, object?>
{
    ["alternate-exchange"] = "example.unrouted"
};

// ConsumerConfig.Arguments：属于 BasicConsume，例如消费者优先级。
var consumerArguments = new Dictionary<string, object?>
{
    ["x-priority"] = 5
};
```

- 参数名、值类型及适用范围遵循 RabbitMQ、队列类型和插件规则。`Type = "headers"` 的匹配参数属于 Binding，`x-priority` 属于 Consumer；插件专有参数需要相应 Broker 支持。配置不等于自动安装插件或自动创建参数引用的其他资源。
- 使用组件支持的 AMQP field-table 值，而非任意 CLR 对象。当前支持 null、string、bool、byte/sbyte、short/ushort、int/uint、long、float/double、受 AMQP 范围约束的 decimal、`AmqpTimestamp`、`byte[]`、`BinaryTableValue`、嵌套字符串键 table 与 `IList`；不要假设 POCO、所有数字类型或任意集合都能自动转换。
- 组件保存配置快照，对支持的嵌套值复制并结构化比较。调用之后修改原配置或 Arguments 不用于更新已经注册的拓扑；不支持的值应显式失败，而不是借引用绕过快照。
- Arguments 参与对应拓扑的兼容性判断，Binding Arguments 还参与 Binding 身份。声明同名资源时必须与 Broker 上已有属性兼容，包括类型、持久化、排他性及相关参数；冲突显式失败，不删除重建，也不通过 no-wait 隐藏失败。
- 首次发布 Open 成功前确保 Exchange 声明完成；订阅首次 Bind 确保 Queue、Exchange、Binding 就绪。一个 Queue 绑定多个 Exchange 时，它仍然只有一份队列声明配置。
- 默认 Exchange 使用空名称，发布跳过声明；按 RabbitMQ 默认 Exchange 规则使用队列名作为 routing key。**禁止显式 Bind 默认 Exchange。**
- RabbitMQ 4.3 的默认策略可能拒绝 `Durable = false` 且 `Exclusive = false` 的队列。本仓库真实集成测试和示例因此使用 `Durable = true` 的非排他队列，并以运行级唯一名称隔离；不要通过修改服务端策略来迁就测试拓扑。
- Queue/Exchange 的 `Durable = true` 只表示拓扑可在 Broker 重启后保留，**不表示组件内存缓冲或消息正文已经持久化**。组件当前不以该标志承诺消息 delivery mode、端到端投递成功、exactly-once 或进程崩溃恢复；需要消息持久性时仍须结合 Broker 配置、消息属性、publisher confirm、持久化存储/outbox 与业务幂等设计。
- `QueueConfig.Exclusive` 表示队列归声明它的连接独占；`ConsumerConfig.Exclusive` 表示该 Queue 上只允许当前消费者。两者是不同的 Broker 约束：普通的非独占 Queue 也可以注册独占 Consumer，消费独占不改变 Queue 的生命周期或所有权。
- `ConsumerConfig.NoLocal` 会原样传给 `BasicConsume`，但 RabbitMQ 不实现 AMQP `no-local` 的“过滤本连接发布消息”语义；组件也不会额外执行本地发布来源过滤。
- AutoDelete/Exclusive 队列在断线或切换时受 Broker 生命周期规则约束，恢复失败会显式报告。

上述死信示例只配置了队列参数。死信 Exchange、目的 Queue 和 binding 必须由应用或基础设施另行确保存在；组件不会凭 `x-dead-letter-exchange` 自动构建完整死信拓扑。

## Codec、压缩与内存所有权

配置的 `Serializer` 属性已替换为必需的 `ICodec<T> Codec`，不保留旧属性别名。原有 `ISerializer<T>`、`IOwnedBufferSerializer<T>` 和 `IOwnedBuffer` 仍作为序列化依赖与可选 owner 快路径使用；`CompositeCodec<T>` 组合一个必需的 serializer 与一个可选的 `ICompressor`：

```csharp
// 不压缩，wire body 与 serializer 输出一致。
var plain = new CompositeCodec<OrderCreated>(JsonMessageSerializer<OrderCreated>.Default);
// 发布端和订阅端必须约定相同的 serializer 和压缩格式。
var gzip = new CompositeCodec<OrderCreated>(
    JsonMessageSerializer<OrderCreated>.Default,
    new GzipCompressor(),
    maxDecompressedBytes: 8 * 1024 * 1024);
var brotli = new CompositeCodec<OrderCreated>(
    JsonMessageSerializer<OrderCreated>.Default, new BrotliCompressor());
var zstd = new CompositeCodec<OrderCreated>(
    JsonMessageSerializer<OrderCreated>.Default, new ZstdCompressor(),
    maxDecompressedBytes: 8 * 1024 * 1024);
// zstd 默认解码窗口最大 8 MiB；确需解码更大窗口的帧时，双方明确约定 maxWindowLog。
// var zstdLargeWindow = new ZstdCompressor(maxWindowLog: 27);
```

公共 codec、serializer 和 compressor 契约使用连续内存输入；调用方 scratch API `CodecBuffers`、`IReusableByteBufferWriter` 和 `DecodeScratchMaxBytes` 已移除：

```csharp
public interface ICodec<T>
{
    void Encode(T message, IBufferWriter<byte> output);
    Span<byte> Encode(T message);
    T Decode(ReadOnlySpan<byte> input);
}

public interface ISerializer<T>
{
    void Serialize(T message, IBufferWriter<byte> output);
    T Deserialize(ReadOnlySpan<byte> body);
}

public interface ICompressor
{
    void Compress(ReadOnlySpan<byte> input, IBufferWriter<byte> output);
    void Decompress(ReadOnlySpan<byte> input, IBufferWriter<byte> output);
}

public interface IOwnedBufferCodec<T> : ICodec<T>
{
    bool TryEncodeOwned(T message, out IOwnedBuffer? owner);
}
```

需要直接把编码结果交给 RabbitMQ 异步发送时，使用 BCL `ArrayBufferWriter<byte>` 和 writer 重载。`BasicPublishAsync` 接收的 `ReadOnlyMemory<byte>` 仍借用 writer 的底层数组，因此在返回的异步操作真正完成前，必须保持该 backing 存活并且不能清空、复用或改写：

```csharp
var output = new ArrayBufferWriter<byte>();
gzip.Encode(message, output);
ReadOnlyMemory<byte> body = output.WrittenMemory;

// output/body 必须保持不变并存活到该异步发送真正完成。
await channel.BasicPublishAsync(exchange, route, body, cancellationToken);
```

`Encode(T)` 的 `Span<byte>` 便捷重载会分配一个独立、非池化的托管数组；返回 span 的 `Length` 就是准确的编码字节数，不包含多余容量，也不需要归还到池。它适合同步消费结果的简短代码，但每次调用都会分配。`Span<byte>` 不能安全地跨越 `await`，也不能为异步 transport 提供可跟踪的长期所有权，所以不要用这个重载构造跨 `await` 的直接 `BasicPublishAsync` 调用；异步发布应使用上面的 writer 重载并保留 `ReadOnlyMemory<byte>` backing。

解码不再需要 scratch，直接传入连续的只读 span：

```csharp
OrderCreated decoded = gzip.Decode(delivery.Body.Span);
```

- `CompositeCodec<T>(serializer, compressor = null, maxDecompressedBytes = 64 * 1024 * 1024)` 要求 serializer 非 null、上限为正。无压缩时直接使用 serializer；压缩 decode 仍以 `maxDecompressedBytes` 限制逻辑解压输出，超限不会继续交给 serializer。该限制不是整个进程的内存或 CPU 预算。
- 压缩编码需要先取得 serializer 输出，再写入最终 output；这个内部中间 writer 可能分配。便捷 `Encode(T)` 本身也明确分配，因此 codec 路径不承诺零分配。
- **不增加自定义 wire header，不自动探测格式，也不自动协商 codec。** 两端及任何原生 AMQP 客户端都必须约定相同格式；不能把普通 JSON 发给 gzip/Brotli/zstd receiver。示例程序的组件端点和原生客户端都显式使用 JSON + gzip。
- 内置 compressor 在返回前完成尾部写入，不释放调用方 output。压缩不是加密或消息认证。
- gzip/Brotli 解码沿用 .NET 压缩流的行为：截断输入或空输入会静默返回已解出的部分，帧后的多余字节被忽略，其余格式错误按 .NET 实现抛出。
- zstd 解码为严格模式，使用标准 zstd 帧格式，可与其他语言的 zstd 实现互通：空输入、截断帧、非法数据、帧后多余字节都会抛 `InvalidDataException`，多个拼接帧按顺序解码。
- `ZstdCompressor(compressionLevel = 3, maxWindowLog = 23)`：级别范围为 ZstdSharp 支持的范围（最高 22）。`maxDecompressedBytes` 只限制输出字节，而 zstd 解码器会按帧头声明的窗口先分配内存，所以另用 `maxWindowLog` 限制可接受的窗口（默认 2^23 = 8 MiB，zstd 自身默认是 128 MiB），避免很小的恶意帧触发大块内存分配。组件编码时会写入内容大小，并让窗口不超过消息大小；级别 ≤ 19 时窗口始终不超过 8 MiB，级别 20–22 且消息超过 8 MiB 时，需要在收发两端调大 `maxWindowLog`。
- `ZstdCompressor` 每次调用都新建并释放 ZstdSharp 上下文，因此可以跨线程共享，也不会跨调用保留窗口内存；实例本身不持有需要释放的资源。
- writer、span 输入和提供的内存均为借用资源，仅在本次同步调用内使用，codec、serializer 和 compressor 不得缓存或自行释放。**`Decode` / `Deserialize` 返回对象不得借用 delivery 或内部解压内存**；需要保留字节时必须复制。
- 直接调用 writer 重载时，调用方拥有 output 及其 backing；只有在所有借用者都已完成后才能复用。组件发布路径同样会让正文 backing 存活到 transport 实际不再借用它，排空超时不授权提前释放或复用。
- `TryEncodeOwned` 只有在能返回独立、可转移并由调用方最终释放的 owner 时才返回 `true`。`false` 表示调用方应走 `Encode(message, output)`；`CompositeCodec` 仅在无压缩且 serializer 实现 `IOwnedBufferSerializer<T>` 时返回 `true`。`IOwnedBuffer.Length` 必须在 `Memory` 范围内，成功转移的 owner 同样必须保留到实际 transport 不再借用正文。
- 内部 writer 与 writer→`Stream` 适配器的部分实现以源码方式取自 [CommunityToolkit/dotnet](https://github.com/CommunityToolkit/dotnet) 提交 `b135626dd54d33b8f05f2ff31591592c004aa848` 的 `CommunityToolkit.HighPerformance`。许可和 .NET Foundation notice 见 `licenses/CommunityToolkit.HighPerformance.LICENSE.md`；vendored 类型保持内部可见，不构成公共 caller scratch API。
- codec、serializer、compressor 及 JSON converter 可能被并发调用，必须线程安全；组件不 Dispose 外部依赖。stream 适配器、gzip/Brotli/zstd 的压缩状态、serializer/compressor 自身、内部中间 writer 以及便捷编码数组都可能分配。
- 诊断阶段使用 `ErrorStage.Encoding` / `ErrorStage.Decoding`，覆盖序列化、压缩、解压和反序列化异常；消费解码失败仍在原结算边界处理。

## 消费成功、ACK 与失败处理

默认 `ConsumerConfig.AutoAck = false`。在此模式下，**只有同步 `Action<T>` 正常返回后组件才发送 ACK**；组件直接同步调用该回调，业务回调异常会传回消费边界。

- **禁止 `async void`，也不要向 `Action<T>` 传入 `async message => ...`。** 它会在异步业务完成前返回，导致过早 ACK，后续异常也无法通过同步边界处理。
- 若处理逻辑只是向下游队列入队，下游入队失败必须抛出异常；静默忽略失败仍会使回调正常返回并 ACK。
- 下游仅进入内存队列不等于业务已经持久化。应用应自己确定“正常返回”所代表的可靠性边界。
- `AutoAck = false` 时，解码失败或 Action 异常会发送 Reject（`requeue: false`）并报告错误；组件不会自动重新入队。没有可用死信路由时，被拒绝的消息会丢弃；即使配置了 DLX，也应独立验证死信资源、路由和权限。
- `AutoAck = true` 是 **Broker 不等待应用 ACK** 的投递模式，不是“handler 成功后组件自动 ACK”。成功、解码失败和 Action 异常都不会由组件发送 ACK 或 Reject；处理异常仍会报告，但已经由 Broker 结算的消息不能再通过 Reject 进入 DLX，也不会由组件重新入队。
- `AutoAck = true` 时不能依赖 `PrefetchCount` 限制未确认消息，因为不存在等待应用确认的投递；组件不会另加本地确认、流控或重投来伪装可靠交付。
- Broker 切换或恢复会先注册尚未激活的候选消费者。候选准备失败、取消或关闭前收到的手动确认消息可以保持未确认并由 Broker 重新处理，但 **AutoAck 消息可能已经由 Broker 结算却尚未进入业务 handler**。这是候选预激活阶段的丢失窗口，组件不承诺 AutoAck 切换期间零丢失。

## Publisher confirms 与可观察错误

发布设计启用 publisher confirms，并使用 mandatory/return 检测不可路由情况。成功入队不能代替发布结果确认，confirm 本身也不是业务消费完成通知。

- 错误通过统一的服务级错误入口报告，并可结合日志观察；包含阶段、资源标识和结果确定性。配置应用时必须接入该入口，不能只检查 `TrySend` 返回值。
- 编码失败、确定发布失败、不可路由和排空超时均应可观察，不静默丢弃。
- **已经发起发送、却因连接中断等原因无法确定 confirm 结果时，报告 Unknown，不自动重发。** 消息可能已到达 Broker，也可能没有到达；应用自行决定如何对账、补偿和去重。
- 尚未尝试发送、明确仍在本地缓冲中的消息可等待连接恢复；已尝试且结果未知的消息不得盲目重新入队。
- 本地缓冲只在内存中，不承诺进程崩溃后的恢复。业务如需更强保障，应另行设计持久化 outbox、业务幂等键及对账机制。
- 错误观察器自身抛异常不得终止 worker；默认日志不输出消息正文或凭据。应用自定义日志也应脱敏。

错误观察器、重连退避和最终排空超时通过 `RabbitMQServiceOptions` 配置：

```csharp
var options = new RabbitMQServiceOptions
{
    PublishBroker = publishBroker,
    SubscriptionBroker = subscriptionBroker,
    ReconnectMinDelay = TimeSpan.FromMilliseconds(250),
    ReconnectMaxDelay = TimeSpan.FromSeconds(30),
    DrainTimeout = TimeSpan.FromSeconds(10),
    ErrorObserver = error =>
    {
        // ComponentError 提供 Role、Stage、Outcome、Resource、Exception、MessageType、
        // DeliveryTag、Generation 和 Timestamp。不要记录 OriginalMessage 或异常文本。
        Console.Error.WriteLine(
            $"role={error.Role}, stage={error.Stage}, outcome={error.Outcome}, " +
            $"resource={error.Resource}, exceptionType={error.Exception.GetType().Name}");
    }
};
```

`ErrorObserver` 可能被并发调用，必须线程安全且同步返回。观察器自身抛出的异常会被隔离，不能改变 ACK、Reject 或发布结果；但应用仍应保持观察器简单、无阻塞并避免输出 `OriginalMessage`、`Exception.Message`、`BrokerConnectionOptions.Uri` 或其他可能含正文/凭据的值。

## 订阅局部恢复、连接恢复与 Broker 切换

服务级连接配置分别指定初始发布 Broker 和订阅 Broker。即使两者起初指向同一地址，也使用不同连接。

```csharp
// nextSubscriptionBroker 为应用构造的 BrokerConnectionOptions。
await service.SwitchSubscriptionBrokerAsync(
    nextSubscriptionBroker, cancellationToken);
```

订阅侧有三种不同的修复或切换边界：

- **单个消费 channel 或 Consumer 故障**：只关闭并重建受影响 Receiver 的 Session/channel，在同一条仍健康的订阅 connection 上恢复该 Receiver 的 Queue、Exchange、Binding、QoS 和 ConsumerConfig。其他 Receiver 的 channel、Consumer 和正在执行的 handler 不参与排空或重建，可以继续处理和 ACK。独占 Consumer 的局部修复只先关闭它自己的旧 Session，再注册自己的替代 Consumer，不会为了修复它而关闭同连接上的其他独占 Consumer。
- **订阅 connection 故障**：连接上的所有 channel 都已失去健康基础，组件才重建整个订阅连接世代，并恢复全部仍有效的 Receiver/Binding。整个连接故障后的同 Broker 恢复若包含独占 Consumer，会先建立候选连接和故障监听，再关闭旧世代消费者，然后重新声明拓扑并注册全部候选消费者；期间可能有短暂消费间隙，旧世代关闭后不能回滚，其他连接上的消费者也可能使独占注册失败并进入退避重试。
- **调用方显式执行 `SwitchSubscriptionBrokerAsync`**：这是用户发起的 Broker 迁移，不是故障 channel 的局部修复。组件在目标 Broker 上准备有效的订阅 Queue/Exchange/Binding，并协调消费者及在途处理；已释放的 Binding 不会因此复活，ACK 仍必须使用原投递所在的 channel/连接世代。

显式切换还遵循以下边界：

- **不会改变发布 Broker、已有发布端点、发布缓冲或发布 worker。** 切换后若仍向原发布 Broker 发布，不会因此自动投递到新订阅 Broker。
- **不会搬迁旧 Broker 队列中的积压消息**，也不保证跨 Broker exactly-once。需要迁移消息时应另行设计迁移流程。
- 目标 Broker 必须有相应权限、插件和兼容的声明条件。切换准备失败与提交后收尾失败是不同阶段；不要把提交后的取消理解为“切换肯定没发生”。
- 若显式切换指向与当前订阅连接相同的 Broker/vhost，且旧世代仍有活动的独占 Consumer，组件会在创建候选连接前拒绝切换，以免为了同 Broker 的候选注册而中断健康消费者。该保护只按配置中的 Broker 身份判断，不推断 DNS、代理或集群节点是否实际相同；切换到不同 Broker/vhost 时仍按候选准备、提交、再关闭旧世代的顺序执行。
- 组件负责连接和拓扑恢复，关闭客户端自动恢复以避免双重恢复；发布侧独立恢复自己的 Exchange。恢复策略采用有界退避，并支持取消和错误观察。

## 构建与测试

在仓库根目录执行；使用能够构建 `net8.0` 的 .NET SDK，运行测试需兼容的 .NET 8 运行环境。

```powershell
dotnet restore RabbitMQ.Component.sln
dotnet build RabbitMQ.Component.sln -c Release --no-restore
# 本地验证不继承真实 Broker 配置，真实 Broker 用例应明确 skip。
Remove-Item Env:RABBITMQ_PRIMARY_URI, Env:RABBITMQ_SECONDARY_URI -ErrorAction SilentlyContinue
dotnet test RabbitMQ.Component.sln -c Release --no-build
dotnet test tests/RabbitMQ.Component.Tests/RabbitMQ.Component.Tests.csproj -c Release --no-build --filter FullyQualifiedName~CodecTests
```

真实 RabbitMQ 集成测试需要两个可访问且具有测试拓扑权限的 Broker/vhost。仅使用测试资源；环境变量中的 URI 含凭据，应按敏感信息处理，特殊字符需 URI 编码。

```powershell
# 以下地址、用户名、密码及 vhost 均为占位值，请替换为自己的测试环境。
$env:RABBITMQ_PRIMARY_URI = 'amqp://TEST_USER:TEST_PASSWORD@127.0.0.1:5672/TEST_VHOST'
$env:RABBITMQ_SECONDARY_URI = 'amqp://TEST_USER:TEST_PASSWORD@127.0.0.1:5673/TEST_VHOST'

dotnet test tests/RabbitMQ.Component.IntegrationTests/RabbitMQ.Component.IntegrationTests.csproj -c Release --no-build

# 本地测试入口。
./scripts/Invoke-IntegrationTests.ps1
```

`scripts/Invoke-IntegrationTests.ps1` 是本地测试入口，**不自动部署远端 Broker**。请先准备两个测试 Broker 及环境变量；它不是远程容器创建、SSH 配置或生产环境部署工具。本文不提供真实凭据或特定远端部署信息。

验证时需分别记录构建、单元测试和真实集成测试的实际结果；环境变量缺失、认证/网络失败、集成测试跳过，都不能算作双 Broker 切换验证通过。真实测试应使用运行级唯一资源名，并只清理本次创建的拓扑，不操作共享生产队列。

重点验证同 key 共享和成对释放、配置冲突、最后 Binding 关闭 Receiver、序列化和回调失败、confirm Unknown 不重发、不可路由、内存释放时序，以及双 Broker 切换后参数恢复、旧 ACK 隔离与发布目标不变。

**Codec 池化与关闭诊断修正的最终核验状态（2026-09-23）：** 最新主会话核验成功退出：Release 全解决方案构建通过，0 警告、0 错误；单元测试 141/141 通过；集成测试项目 3 项本地测试通过、当前版本全部 14 项真实 Broker 用例明确跳过；Codec、Publishing、Subscription 的 122 项聚焦测试连续运行 10 轮，共 1220/1220 通过；`Compressed_final_owner_survives_drain_timeout_until_actual_transport_completion` 的两个 variant 连续运行 50 轮，共 100/100 通过。较早核验曾在第 10 轮暴露 Drain 诊断竞态：端点清理先设置幂等标志但尚未完成同步错误报告时，manager disposal 可能观察标志并提前返回；现已通过共享报告完成任务修正，以上最新重复核验覆盖了该路径。初始独立审查发现有界解压曾使用逐消息非池化数组，现已恢复池化并由不同 agent 静态复核；关闭诊断共享任务修正也已独立静态复核，两项复核均无已确认问题。独立复核只检查代码，没有执行测试，不把它们表述为测试通过。真实 Broker 用例和样例程序均未运行，本轮结果不包含也不沿用旧的 Secondary Broker 认证失败；这些本地结果不代表真实 Broker 验证通过。完整真实 Broker 测试仍须在具备已授权配置的环境中运行；认证、连接、声明或切换失败必须 fail，skip 或未运行不得记为通过。

**CommunityToolkit.HighPerformance 源码移植核验状态（2026-09-24）：** 自定义 pool 异常归还路径修正后，主会话在清除 `RABBITMQ_PRIMARY_URI` 与 `RABBITMQ_SECONDARY_URI` 后独立复跑并成功退出：Release 全解决方案构建通过，0 警告、0 错误；单元测试 148/148 通过；集成测试项目 3 项本地测试通过、14 项真实 Broker 用例明确跳过；Codec、Publishing、Subscription、Serialization 的 134 项聚焦测试连续运行 10 轮，共 1340/1340 通过；原 Drain 竞态专项的两个 variant 连续运行 50 轮，共 100/100 通过。此前 146 项单测及 126 项聚焦测试连续 10 轮的结果属于本次异常路径修正前的中间历史，不作为最终核验结果。不同 agent 的只读独立审查先发现：若自定义 `ArrayPool.Return` 抛异常且未自行清理，增长时的旧数组或 Dispose 时的最终数组可能在 payload 清除前失去最后引用；现已在 Return 前显式预清理并增加对应测试，后续只读复核无剩余已确认问题。该独立复核未执行测试或 pack，不把它表述为测试或打包通过。worker 本地执行 `dotnet pack` 并检查归档，确认包内包含 `licenses/CommunityToolkit.HighPerformance.LICENSE.md`；pack 有一条既有的缺少 package README 建议性警告，因此不表述为零警告，且该包未发布。真实 Broker 用例和样例程序本轮均未运行，skip 不等于通过，这些本地结果不代表真实 Broker 验证通过。
