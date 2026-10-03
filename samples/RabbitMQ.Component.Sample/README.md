# 启动 Sample

1. 填写仓库根目录 `.env` 中的 `RABBITMQ_PRIMARY_URI` 和 `RABBITMQ_SECONDARY_URI`。
   两者应指向不同的、可访问的 RabbitMQ Broker/vhost，账号需具有声明、发布、消费及删除示例拓扑的权限。
   URI 格式为 `amqp://USER:PASSWORD@HOST:5672/VHOST`（或 `amqps://`）；特殊字符需 URI 编码，默认 vhost `/` 可写为 `%2F`。
2. 安装 .NET 8 SDK，在仓库根目录执行：

   ```powershell
   dotnet run --project samples/RabbitMQ.Component.Sample
   ```

Sample 自动加载仓库根目录 `.env`，支持不带引号或单/双引号包裹的值；已有非空进程环境变量优先。`.env` 已被 Git 忽略，不要提交凭据。

默认模式依次验证主 Broker 收消息、订阅切换到次 Broker、发布端仍指向主 Broker，然后清理本次创建的拓扑并退出。未填写配置或 Broker 不可访问时不能完成运行。

## 长时间生产/消费与恢复检查

仅需填写 `RABBITMQ_PRIMARY_URI`，执行：

```powershell
dotnet run --project samples/RabbitMQ.Component.Sample -- --continuous
```

每秒发送一条 JSON + gzip 消息，持续运行直到 Ctrl+C；不切换 Broker。使用独立的发布和订阅连接、手动 ACK、prefetch 32 和容量 1024 的发布缓冲。收到消息时输出 `RECEIVED`、序号、累计消费数和延迟；每秒输出 `SEND`、入队结果与累计计数。错误日志包含角色、阶段和结果确定性，不打印连接凭据。

手动验证：

1. 等待 `Ready` 和持续出现的 `RECEIVED` 日志，记录程序输出的唯一队列名。
2. 管理页面进入 **Queues and Streams → 该队列 → Consumers**，查看消费者对应的 channel/connection。
3. 如果你的管理界面提供 channel 关闭操作，关闭该消费 channel；也可以通过管理 HTTP API 关闭 channel。观察消费者/channel 是否重新出现、`RECEIVED` 计数是否继续增长。仅关闭消费 channel 验证的是订阅局部恢复，通常无需重建整个连接。
4. 管理页面通常提供 **Connections → 连接 → Close connection**。关闭消费连接可单独验证整个订阅连接的恢复；关闭发布连接可检查发布侧恢复。注意只操作当前 Sample 的连接，不要影响其他应用。
5. 按 Ctrl+C 正常结束，程序释放组件并删除本次运行的 Queue/Exchange。意外退出后，Broker 检测到订阅连接断开时会删除独占队列；网络中断时可能需要等待心跳检测。Exchange 仍为持久化资源，强制退出可能留下 Exchange，需按本次输出的精确名称手动清理。

## 临时队列的限制

两个模式均使用 `Durable=false`、`Exclusive=true`、`AutoDelete=true`，显式指定 `x-queue-type=classic`，避免 Broker 默认 quorum 类型（不支持这类临时独占队列）。RabbitMQ 4.3 对非持久、非排他队列可能有限制，因此不能仅设置 `Durable=false` 和 `AutoDelete=true`。

- Auto-delete 仅在队列曾有消费者、最后一个消费者离开后触发；未成功注册过消费者的队列不会仅凭此标志删除。`Exclusive=true` 保证拥有者连接关闭时删除，另加 `x-expires=60000`，使无消费者且未被使用的队列在约 60 秒后过期（非精确计时，不影响持续活跃的消费者）。
- 独占队列仅可由声明它的连接访问。发布连接向 Exchange 发布不受此限制，但其他连接不能消费或声明同一队列。默认切换样例因此会在旧订阅连接关闭后，由验证连接重新声明、绑定主 Broker 队列。
- 关闭消费 channel 会使最后一个消费者消失，队列及 binding 可能被删除；恢复不能只重新注册消费者，还必须重新声明队列和绑定。关闭拥有者连接则会删除整个独占队列。
- 每次运行使用唯一队列名；同一次恢复使用原队列名，旧队列删除和重新声明可能发生竞态。需要观察错误日志及后续是否恢复，不承诺无间隙恢复；重连期间可能暂时出现资源锁定或队列不存在错误。
- 队列删除会丢失其中的消息，未重新绑定期间的发布也可能不可路由。该模式用于检查恢复，不提供离线积压保留或零丢失保证。

`Accepted` 只表示进入本地缓冲，不代表发送或消费成功。断线期间可能积压、缓冲满或出现 `Unknown`；样例不会重发结果未知的消息，不应期待发送和消费计数必然相等，也不要仅凭错误日志判定恢复成功，应观察后续消费及管理页面拓扑。控制台消费日志在 handler 中打印，先于该次 ACK 完成。
