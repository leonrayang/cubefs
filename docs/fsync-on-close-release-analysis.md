# fsyncOnClose 在 Release 路径上的分析

## 背景

本文整理了 commit
`69b54f48cd3da56accf17269c01bb8aefe8bc279`
（`feat(client,stream): honor fsyncOnClose on release path`）
相关的 close 时 flush 行为变化，并结合 `release-oppo-v3.5.3` 上的运行时分析，说明：

- `fsyncOnClose` 打开或关闭时，客户端在 release 路径上到底会等待什么
- flush 失败会通过什么方式暴露出来
- async flush 什么时候能真正带来性能收益

## 这个 Commit 改了什么

在这个 commit 之前，`File.Release()` 一直调用 `CloseStream()`，而
`CloseStream()` 在 release 路径上总是等待 pending flush 完成。

这个 commit 之后：

- `File.Release()` 会把 `f.super.fsyncOnClose` 传给
  `CloseStreamWithWait(ino, wait)`
- `CloseStream()` 对其他调用方仍然保持同步语义，现在只是内部转调
  `CloseStreamWithWait(inode, true)`
- `Streamer.release(wait)` 现在可以按参数选择：
  - `wait=true`：release 返回前，必须完成 close 时的 flush
  - `wait=false`：release 不再等待 pending 的 async flush 完成
- 当 `wait=false` 时，release 路径上的 async flush 失败会写入专用的
  release audit 日志

一句话总结：这个 commit 让 FUSE release 路径真正遵循 `fsyncOnClose`，
而不是始终等待 flush。

## 关键调用链

close 路径的主链路是：

1. `client/fs/file.go: File.Release()`
2. `sdk/data/stream/extent_client.go: CloseStreamWithWait()`
3. `sdk/data/stream/stream_writer.go: IssueReleaseRequest(wait)`
4. `sdk/data/stream/stream_writer.go: release(wait)`
5. `sdk/data/stream/stream_writer.go: closeOpenHandlerWithAudit(wait, auditOp)`
6. `sdk/data/stream/stream_writer.go: flush(wait, id)`

真正关键的分支只有一个：

- `wait=true`：release 会等待 pending flush 全部完成
- `wait=false`：release 不等待，失败改走 release audit

## Write / Flush / Fsync / Release 的语义

### Write

`File.Write()` 在以下场景会立即失败：

- `ExtentClient.Write()` 或 `fWriter.Write()` 直接返回错误
- `direct I/O` 或 `OpenSync` 强制 `waitForFlush=true`，并且写完后的显式
  `ec.Flush()` 失败

所以即使 `fsyncOnClose=false`，写路径上的即时错误仍然会同步返回给上层。

### Flush

`File.Flush()` 只有在 `fsyncOnClose=true` 时才会真正执行 flush。

当 `fsyncOnClose=false` 时，`File.Flush()` 会直接返回 `ENOSYS`，不会调用
`ec.Flush()` / `fWriter.Flush()`。

这意味着：当 close 时 fsync 被关闭后，由 FUSE close 触发的这条 `Flush`
路径不再提供同步错误面。

### Fsync

`File.Fsync()` 一直是同步行为，仍然会做真实 flush。

显式用户态 `fsync()` 依然是写错误最强的同步暴露点，不受
`fsyncOnClose` 开关影响。

### Release

这个 commit 之后：

- `fsyncOnClose=true`
  - `File.Release()` 调 `CloseStreamWithWait(..., true)`
  - release 会等待 stream 上 pending 的 flush 工作全部完成
  - flush 失败仍然可能通过 close 返回给上层

- `fsyncOnClose=false`
  - `File.Release()` 调 `CloseStreamWithWait(..., false)`
  - release 不再等待 pending 的 async flush
  - 延迟 flush 失败不再走同步 close 返回，而是转到 audit 观测

## Async Flush：内部流水与用户可见完成时间

stream 层虽然有 async flush 机制，但这并不等价于“用户看到的 close 也是异步的”。

### `fsyncOnClose=true` 时

这里有个最容易误解的点：

- 客户端内部在正常写入过程中，仍然可能使用 async flush
- 但到了 release 阶段，stream 会带着 `wait=true` 执行
- `flushAsync(wait=true, id)` 仍然会等所有 pending async flush 完成后才返回

所以从调用方视角看：

- 一次拷贝并不会因为内部用了 async flush 就提前完成
- 只有当 release 路径把 flush 全部收口后，用户态才算真正返回

这会带来一个实际现象：

- 小文件：async flush 的用户可见收益很有限，因为 close 最终还是要等，而且 dirty handler 很少
- 大文件：旧 handler 的 flush 可以和后续写入重叠，最后 close 的尾巴会更短

换句话说，收益不在于“close 不等了”，而在于“很多 flush 工作提前做掉了，并和写入过程形成了流水”。

### `fsyncOnClose=false` 时

此时 release 路径不再等待，所以 async flush 才真正脱离 close 的关键路径。

好处是：

- close-heavy 的 buffered workload 延迟更低

代价是：

- 一部分延迟 flush 失败不再同步返回给调用方

## 对普通 `cp` 的影响

对于一次普通的 buffered file copy：

- 当 `fsyncOnClose=true`
  - 写请求内部仍可能形成流水
  - 但最终完成时间仍然要等 release 路径上的 flush draining
  - 小文件从 async flush 获得的收益有限
  - 大文件收益更明显，因为 dirty handler 更多，旧分段 flush 更容易和后续写入重叠

- 当 `fsyncOnClose=false`
  - 写路径的即时错误仍然会同步返回
  - direct/sync write 触发的显式 flush 失败也仍然会同步返回
  - 最后的 close 不再等待 buffered async flush 完成
  - 调用方可能看到更低的 close 延迟，但延迟 flush 失败会从同步 close 错误转到 audit

## 错误可见性矩阵

### `fsyncOnClose=true`

- 写路径即时失败：对调用方可见
- sync write 上的显式 flush 失败：对调用方可见
- close 时 pending flush 失败：通过 release 路径同步返回
- release async audit：不是主要通路

### `fsyncOnClose=false`

- 写路径即时失败：仍然对调用方可见
- sync write 上的显式 flush 失败：仍然对调用方可见
- close 时延迟 flush 失败：不会通过 release 同步返回
- release async audit：成为 release 路径延迟 flush 失败的主要观测手段

## Audit 行为

这个 commit 为 release 路径上的 async flush 失败补了一条专门的 audit 通路。

当 `wait=false` 时，stream 会使用一个特殊的审计操作名
`ReleaseAsyncFlush`，然后通过 `auditlog.LogReleaseAsyncError(...)`
记录异步 flush 失败。

这条机制只有在 client audit 打开时才生效：

- `enableAudit=true` 时，会同时初始化普通 client audit 和专门的 release audit
- 如果 release audit 没初始化，`LogReleaseAsyncError(...)` 就是 no-op

所以实际语义是：

- `fsyncOnClose=false` 不等于“flush 失败消失了”
- 它等于“release 时的延迟 flush 失败从同步 close 错误，切换成 release audit 观测”

## 诊断日志补充说明

客户端退出时，还会在 client 日志目录下额外写一份
`goroutine.<timestamp>.log`。

这和 release async audit 不是一回事。

这个文件是 client 进程退出时主动 dump 的 goroutine 栈，不是直接的 flush 错误记录。

## 运维建议

### 适合使用 `fsyncOnClose=true` 的场景

- 更关心 close 时的持久化语义，而不是 close 延迟
- 业务依赖 close 返回值来感知延迟 flush 失败
- 以小文件 workload 为主，async overlap 本身带来的收益有限

### 适合使用 `fsyncOnClose=false` 的场景

- 更关心 close 延迟
- 以大文件 buffered write 为主，内部流水重叠有明显收益
- 可以接受通过 audit、监控或显式 `fsync()` 来观测错误，而不是依赖 close 的同步返回

## 最终结论

- 这个 commit 没有移除内部 async flush 能力
- 它改变的是 FUSE release 路径是否等待这些 flush 工作
- 当 `fsyncOnClose=true` 时，普通 buffered copy 仍然要等 release 路径把 flush 收完才算真正完成
- 当 `fsyncOnClose=false` 时，close 会更快返回，但 release 时的延迟 flush 失败会从同步 close 错误转到 release audit 记录
