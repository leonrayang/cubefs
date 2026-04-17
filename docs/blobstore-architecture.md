# BlobStore 架构与数据单元说明

本文基于 `blobstore/` 与 **`sdk/data/blobstore`** 相关代码整理：**先说明设计取舍与组件**，再分层给出 **逻辑地址（Location/Slice/Blob）→ 卷与物理单元（Vuid/Shard）→ 纠删码与多 AZ**，最后附 **Access 读写函数速查**、**调度**、**术语表**、**CubFS SDK 示例**与审核要点。阅读时可按需跳读目录。

## 目录

1. 设计权衡（相对 DataNode 三副本）  
2. 组件一览  
3. 逻辑单元：对象地址与条带  
4. 集群与卷：路由与副本 placement  
5. 物理单元：盘与存储对象  
6. 纠删码（EC / LRC）：算法、模块、流程与多 AZ  
7. Access 读写路径（关键函数）  
8. 术语对照  
9. 调度与迁移  
10. CubFS SDK 视角：文件如何落在 BlobStore 上（示例）  
11. 审核关注点  

*建议配合编辑器「大纲 / 符号列表」折叠浏览。*

---

## 1. 设计权衡：写放大与读写路径（相对 DataNode 三副本）

- **写路径与写放大**  
  同一份用户数据在 Access 侧需经 **EC（N+M(+L)）**，再 **并行写入多条 Shard**（多 BlobNode、多 RPC）。相对经典 **DataNode 三副本**（各副本写入一份完整用户数据），BlobStore 往往在 **磁盘写入量、网络 RPC 次数、连接扇出** 上更重，**写放大与写路径开销通常更明显**，属于架构层面的常态。

- **读路径：大文件「连续读」**  
  逻辑上的顺序读在 BlobStore 往往对应 **多个 Location / 多次 Get**，或单次 Get 内 **多 blob、多 Vid、多机 Shard**，**不像单副本单盘那样一条顺序流扫到底**。

- **读路径：多租户 / 多文件 / 随机读**  
  当 workload 已在 **不同卷、不同盘、不同 inode** 之间频繁跳转时，**随机性本来就高**；此时 EC 条带分散带来的额外寻址成本，**相对三副本「三挑一读」的优势差距会被冲淡**——读侧是否「更亏」**强依赖访问模式**：高并发随机场景下，读 IO 对比三副本**未必更差**；**单流大顺序读、低并发**时，三副本从单副本顺序读往往更容易呈现带宽优势。

- **Access 读优化（补充）**  
  实现上仍有 **优先读 data shard、按 IDC 排序** 等路径（见后文 `stream_get`），可缓解部分读成本，但不改变「条带化存储」的基本形态。

纠删码算法、多 AZ 布局见 **§6**；Access HTTP 读写函数速查见 **§7**。

---

## 2. 组件一览

| 组件 | 路径 | 职责 |
|------|------|------|
| Access | `blobstore/access/` | 对外 HTTP：`/put`、`/get`、`/alloc`、`/delete`、`/putat`、`/sign`；EC 编解码；并发访问 BlobNode。 |
| Proxy | `blobstore/proxy/` | 卷/空间分配、缓存、向 ClusterMgr 心跳；Kafka（删除、分片修复等）。 |
| ClusterMgr | `blobstore/clustermgr/` | Raft 元数据：服务注册发现、卷与路由等。 |
| BlobNode | `blobstore/blobnode/` | 磁盘侧存储：**Disk → Chunk → Shard**。 |
| ShardNode | `blobstore/shardnode/` | 卷目录/分配相关能力（Raft、catalog 等），与 CM 协同。 |
| Scheduler | `blobstore/scheduler/` | 迁移、均衡、盘修复/下线、删 blob、手动任务。 |
| 公共 | `blobstore/common/` | `proto`、`codemode`、`ec`、RPC、trace。 |

入口与路由：`blobstore/cmd/cmd.go`（HTTP + 可选 rpc2）；Access 路由：`blobstore/access/service.go` → `NewHandler`。

---

## 3. 逻辑单元：对象地址与条带

### 3.1 `Location`（客户端持有的「对象地址」）

- **定义**：`blobstore/common/proto/blob.pb.go` → `Location`。
- **含义**：描述**一个逻辑对象**在某一集群内如何被 EC 条带化并落到哪些卷上。
- **主要字段**：
  - `ClusterID`：集群。
  - `CodeMode`：纠删/副本模式（见 `common/codemode`）。
  - `Size_`：对象总字节数。
  - `SliceSize`：单条 **逻辑 blob** 的最大数据量（与分配、切分一致；注释里 json 曾用 `blob_size`）。
  - `Crc`：完整性签名（与 `security` 配合）。
  - `Slices`：`[]Slice`，见下。

**关键函数**：`blobstore/common/proto/blob_ext.go`

- `Location.Spread() []BlobUnit`：把 `Slices` 展开为**逐个逻辑 blob**（`Vid` + `Bid` + 该条大小），供 Put/Get 按条读写。

### 3.2 `Slice`（逻辑分组，不是磁盘上的 shard）

- **定义**：`blob.pb.go` → `Slice`。
- **含义**：在**同一 Vid** 上的一段 **连续 BlobID**，用于压缩描述多个 blob。
- **字段**：
  - `MinSliceID`（JSON `min_bid`）：起始 `BlobID`。
  - `Vid`：卷 ID。
  - `Count`：连续 blob 个数。
  - `ValidSize`：扩展字段（部分场景有效字节）。

**规约**：`Spread()` 中第 `i` 个 blob 的 `Bid = MinSliceID + i`；除最后一个 blob 外，大小一般为 `SliceSize`，最后一个可能为 `Size_ % SliceSize`（余数非 0 时）。

### 3.3 `BlobUnit` / 逻辑 **Blob**

- **定义**：`blob_ext.go` → `BlobUnit`（`Bid`, `Vid`, `Size`）。
- **含义**：**一次 EC 条带对齐的「一条」用户数据块**（读入 `ec.Buffer`、再 `Split` → `Encode` 的那一层）。  
  大对象会被切成多个 `BlobUnit`（多个 `SliceSize`，最后一块可能更小）。

**与 EC 的关系**：每个 BlobUnit 对应一轮 **N+M(+L)** 条 **Shard**（见下），不是「一个文件一个 blob」的狭义对象存储命名。

### 3.4 `Blob`（proto 消息）

- **定义**：`blob.pb.go` → `Blob`（`Name` + `Location` + `Sealed`）。
- **含义**：带名字的 blob 记录（例如清单层使用）；日常读写 API 以 `Location` 为主。

---

## 4. 集群与卷：路由与副本 placement

### 4.1 `Vid`（Volume ID）

- **类型**：`proto.Vid`（`basic.go`）。
- **含义**：逻辑卷标识；**分配器**在某一 `Vid` 上分配 **BlobID** 范围（通过 `Slice` 描述）。

### 4.2 `VolumePhy`（Access 侧缓存的卷物理视图）

- **定义**：`blobstore/access/controller/volume.go` → `VolumePhy`。
- **字段**：`Vid`、`CodeMode`、`Units`（来自集群）、`IsPunish`、`Version` 等。
- **获取**：`VolumeGetter.Get(ctx, vid, isCache)` → 经 **Proxy/ClusterMgr** 拉取并本地缓存。

### 4.3 `Unit`（卷在集群中的一条「分片槽位」）

- **定义**：`blobstore/api/clustermgr/volume.go` → `Unit`。
- **字段**：
  - `Vuid`：该槽位在**某一盘**上的 chunk 标识（见 5.1）。
  - `DiskID`：盘。
  - `Host`：BlobNode 地址（路由用）。

**规约**：`VolumePhy.Units` 的下标顺序与 **EC 条带下标**一致（Access 写路径里 `volume.Units[i]` 对应 `shards[i]`）。`CodeMode.Tactic()` 给出 N、M、L、AZ 等，决定条带数量与机房亲和。

### 4.4 关键函数（卷）

| 作用 | 路径 |
|------|------|
| 取卷物理信息 | `access/controller/volume.go`：`volumeGetterImpl.Get` |
| Put 中选卷、分配 | `access/stream/stream_put.go`：`Handler.Put`、`allocFromAllocatorWithHystrix` |
| Get 中取卷、排序读 | `access/stream/stream_get.go`：`Handler.Get`、`getVolume`、`genSortedVuidByIDC` |

---

## 5. 物理单元：盘与存储对象

### 5.1 `Vuid`（Volume unique id on disk）

- **定义**：`blobstore/common/proto/vuid.go`。
- **含义**：**某个 Vid 在某一 EC 下标、某一 epoch 下，在单块盘上的 Chunk 实例标识**。  
  编码：`NewVuid(vid, idx, epoch)` → 高 32 位为 `Vid`，中间为 **index**（EC 条带下标），低 24 位为 **epoch**（卷迁移/重建时递增，避免旧数据混淆）。

**规约**：`Vuid` 与 `DiskID` 一起唯一定位 **BlobNode 上的一个 Chunk**。

### 5.2 `DiskID`

- **类型**：`proto.DiskID`（`basic.go`）。
- **含义**：集群内一块物理盘/逻辑盘的标识；ClusterMgr 维护 **DiskID → BlobNode Host**；Access `GetDiskHost` 用于路由。

### 5.3 Chunk（BlobNode 上、按 Vuid 管理的容器）

- **HTTP**：`blobstore/blobnode/service.go` — `/chunk/create/.../vuid/:vuid`、`/chunk/release/...` 等。
- **含义**：在**指定 Disk** 上为某个 **Vuid** 分配的一段持久化上下文（创建、只读、压缩、inspect）。  
  **Shard** 挂在 Chunk 下。

### 5.4 Shard（磁盘上真实存储的 EC 分片）

- **API 类型**：`blobstore/api/blobnode/shard.go` → `PutShardArgs`、`ShardInfo`。
- **含义**：**一条 EC 分片**的物理对象：在某 **DiskID** 的某 **Vuid** 下，以 **Bid**（BlobID）为键存一段字节。
- **关键字段**：
  - `DiskID` + `Vuid` + `Bid`：唯一定位（HTTP：`/shard/put/diskid/.../vuid/.../bid/...`）。
  - `Size`：分片字节长度。
  - `Flag` / `ShardStatus`：`Normal`、`MarkDelete` 等。

**规约**：

- 对**同一个逻辑 Blob**（同一 `Vid` + `Bid`），EC 第 `i` 条分片写入 `Units[i]` 对应的 `(DiskID, Vuid)`，数据内容为 `shards[i]`（`stream_put.go` 中 `writeToBlobnodes`）。
- **Shard 大小**由 `CodeMode` 与条带长度决定；读侧从多个 BlobNode 拉齐足够分片后 `Reconstruct` / `Join`（`stream_get.go`）。

---

## 6. 纠删码（EC / LRC）：算法、模块、流程与多 AZ

本节把 **数学模型**、**代码模块**、**读写流程** 与 **多可用区（AZ）** 串起来；shard 与卷的对应关系仍与 **§5** 一致。

### 6.1 算法基础：里德-所罗门（RS）与工程实现

- **库**：`github.com/klauspost/reedsolomon`，在 `blobstore/common/ec/encoder.go` 中封装为 **`ec.Encoder`** 接口（`Encode` / `Reconstruct` / `ReconstructData` / `Split` / `Join` / `Verify` 等）。
- **经典 RS（纯 EC，`L=0`）**  
  - 将 `N` 个等长 **数据片** 编码为 `N+M` 片（`M` 个全局校验片）。  
  - **可丢任意 ≤ M 片**（在 GF 域上线性无关前提下）并 **重构** 原始数据。  
  - 实现类型：`encoder` 结构体（`NewEncoder` 在 `L==0` 时返回该实现）。
- **LRC（Locally Repairable Code，`L>0`）**  
  - **第一层**：对前 `N+M` 片做一次 **全局 RS**（跨 AZ 的「宽条带」）。  
  - **第二层**：按 AZ 把「落在该 AZ 的数据片 + 该校验组内的全局校验片」再取出一组子条带，用 **`localEngine`**（参数 `localN=(N+M)/AZCount`、`localM=L/AZCount`）做 **本地 RS**，得到 **`L` 个局部校验片**，分布在各 AZ。  
  - **目的**：单盘或单 AZ 内少量损坏时，**优先用本地校验修复**，减少跨 AZ 读带宽。  
  - 实现类型：`lrcEncoder`（`NewEncoder` 在 `cfg.CodeMode.L != 0` 时返回），核心逻辑在 `lrcencoder.go`（`Encode` 先全局再并行各 AZ `localEngine.Encode`；`Reconstruct` 可先尝试本地重构再回退全局 RS，见注释 *use local ec reconstruct, saving network bandwidth*）。
- **副本模式（`M=0` 且 `L=0`）**  
  - `Tactic.IsReplicateMode()`：`Replica3`、`Replica3OneAZ` 等；**无 RS 编码**，`N` 份完整拷贝语义，仍带 `AZCount` / `PutQuorum` 用于写入确认策略。

### 6.2 `codemode.Tactic`：参数含义与校验规约

定义见 **`blobstore/common/codemode/codemode.go`** → `Tactic` 与 `constCodeModeTactic`。

| 字段 | 含义 |
|------|------|
| **N** | 数据片（data shard）数量。 |
| **M** | **全局**校验片数量。 |
| **L** | **局部**校验片总数（仅 LRC；`0` 表示非 LRC）。 |
| **AZCount** | 逻辑 AZ 个数；**N、M、L 均需能被 AZCount 整除**（`Tactic.IsValid`），用于把条带划到各 AZ。 |
| **PutQuorum** | 写成功所需 **成功写入的分片数下限**（注释：**在一整个 AZ 故障时仍须可恢复**；`codemode` 注释说明 quorum 设计时应 **忽略 local 片** 参与可恢复性下界）。实际判定与 **`stream_put` 中 `writtenNum >= PutQuorum`** 及 **多 AZ 容忍分支** 共同作用。校验：`PutQuorum` 落在 \[(N+M)/AZCount+N, N+M\] 闭区间内（`codemode.init()` 中断言）。 |
| **GetQuorum** | 读路径配置位（当前表内多为 `0`，具体读策略在 Access 侧组合 `MinReadShardsX` 等）。 |
| **MinShardSize** | 每片最小字节对齐（如 2KB、512B、0）；与 `ec.NewBuffer` 中 `shardSize` 计算有关（`common/ec/buf.go`）。 |

**按 AZ 的片下标布局**：`Tactic.GetECLayoutByAZ()` 返回 `[][]int`，第 `idx` 个 AZ 对应一组 **全局下标**（先该 AZ 的 `n=N/AZCount` 个 data，再该 AZ 的 `m=M/AZCount` 个全局 parity，再该 AZ 的 `l=L/AZCount` 个 local）。  
源码注释中的 **EC6P10L2** 示意（`codemode.go` 顶部注释）：

- 全局：`N=6` data + `M=10` parity；  
- 两个 AZ 各一条 **local stripe**：各含 `n=3` data、`m=5` global parity、`l=1` local parity，便于 **单 AZ 内** 用较小集合做修复。

### 6.3 缓冲布局与 `Split` / `Join`

- **`ec.Buffer`**（`common/ec/buf.go`）：一块逻辑 blob 的内存布局 — `DataBuf`（用户数据）→ 对齐/填充 → `ECDataBuf` / 整片 `ECBuf`（含 parity、local）。  
- **`Encoder.Split`**：把连续 `ECDataBuf` 切成 `N`（或 LRC 时更多）条等长 shard 视图；**`Encode`** 原地生成校验字节；**`Join`** 从足够多片中还原用户数据写出。

### 6.4 涉及模块一览（纠删与条带全链路）

| 模块 | 路径 | 作用 |
|------|------|------|
| **码型与 AZ 布局** | `common/codemode/codemode.go` | 预置 `CodeMode` ↔ `Tactic`；`GetECLayoutByAZ`、`LocalStripeInAZ` 等。 |
| **EC 核心** | `common/ec/encoder.go`、`lrcencoder.go`、`buf.go`、`doc.go` | RS/LRC 编解码、缓冲、文档图。 |
| **策略注入** | `access/stream/stream.go` | `NewStreamHandler`：从 **ClusterMgr** 拉 `CodeModeConfigKey` JSON → 为每个 `CodeMode` **`ec.NewEncoder`** 建缓存 `handler.encoder[mode]`。 |
| **写路径** | `access/stream/stream_put.go` | `Split` → `Encode` → `writeToBlobnodes`；**PutQuorum** 与 **多 AZ 降级成功**（见 6.6）。 |
| **读路径** | `access/stream/stream_get.go` | `getDataShardOnly`、并行拉片、`Reconstruct`/`ReconstructData`/`Join`；**`genSortedVuidByIDC`**（见 6.6）。 |
| **卷与片槽** | `access/controller`、`api/clustermgr/volume.go` | `VolumePhy.Units[i]` ↔ 全局 shard 下标 `i`。 |
| **集群与 AZ 数一致** | `blobstore/clustermgr/svr.go`（等） | 配置校验：`ShardCodeModeName.Tactic().AZCount` 与集群 **IDC 列表长度** 一致，否则拒绝（避免布局与物理 AZ 不一致）。 |
| **创建卷 / 分配** | `clustermgr/volumemgr/createvolume.go`、`catalog/createshard.go` | 按 `CodeMode.Tactic().AZCount` 参与卷与 shard 布局。 |

### 6.5 写入流程（单逻辑 Blob，与 EC 相关步骤）

1. **`Handler.Put`**（`stream_put.go`）按对象大小 **`allCodeModes.SelectCodeMode(size)`** 选定 `CodeMode`。  
2. **向 Proxy 分配** `Location`（含 `Vid`、Bid 范围等）。  
3. **`ec.NewBuffer(blobSize, tactic, memPool)`** 分配条带缓冲；**`encoder.Split`** 得到 `[][]byte` shards。  
4. **`encoder.Encode(shards)`** — 纯 EC 或 LRC 双阶段编码。  
5. **`writeToBlobnodes`**：对 `volume.Units[i]` 并发 **PutShard**，body 为 `shards[i]`。  
6. **写成功判定**（`stream_put.go` 内 `writeToBlobnodes` 逻辑摘要）：  
   - 统计 **`writtenNum >= PutQuorum`** 则成功；  
   - 未齐的分片可走 **修复队列**（`sendRepairMsgBg` / `sendRepairMsg` → Proxy **ShardRepair**）。  
   - **多 AZ 特例**（`tactic.AZCount >= 3`）：若 **恰有一个 AZ 全挂**（该 AZ 内所有 shard 失败）、其余 **AZCount-1** 个 AZ **全部成功**，则仍视为成功（**容忍整 AZ 故障**），与 `PutQuorum` 注释「单 AZ down 仍可恢复」一致。

### 6.6 多 AZ 场景细化

**1）布局与物理 IDC**

- 逻辑 **AZCount** 与 **CubeFS 集群配置的 IDC 个数**应对齐（见 `clustermgr` 校验）；**`GetECLayoutByAZ`** 给出每个 AZ 负责的 **全局 shard 下标集合**。  
- 每个下标 `i` 对应 **`VolumePhy.Units[i]`** → 某 **DiskID / Vuid / Host**；调度上应尽量使 **同一 AZ 的片** 落在 **同一 IDC**（由卷创建与节点注册策略保证，非 `ec` 包内算法）。

**2）写路径：整 AZ 故障容忍**

- 见 **`stream_put.go`** 中 `tactic.AZCount >= 3` 分支：对每个 `tactic.GetECLayoutByAZ()` 的 AZ 组统计 `azFine` / `azDown`，若 **`allFine == AZCount-1` 且 `allDown == 1`**，则 **quorum 写成功返回**（其余 AZ 已写满且坏 AZ 全失败）。

**3）读路径：优先本 IDC、惩罚盘靠后**

- **`genSortedVuidByIDC`**（`stream_get.go`）：对每个 shard 槽位查 **`GetDiskHost`**，用 **`distance(access所在IDC, hostIDC, punished)`** 排序 — **同 IDC 优先**（`dis=0`），跨 IDC 次之，**惩罚盘**在同 IDC 为 `2`、跨 IDC 为 `3`，从而 **减少跨 AZ 流量、避开问题盘**。  
- **`StreamHandler` 接口注释**（`stream.go`）中的 **EC6P10L2** 读序示例：按「先本 AZ 相关 data+parity、再扩到 N+X」做 **渐进式重构读**，降低首次拉取的片数。

**4）LRC 读修复**

- **`lrcEncoder.Reconstruct`**：若当前 shard 集合长度对应 **仅某一 AZ 的 local stripe**，可走 **`localEngine.Reconstruct`**；否则先 **全局 RS 重构** 再按需补 **各 AZ local**（见 `lrcencoder.go` 分支）。

**小结（与 §3～§5、§8 对照）**：对象 → `BlobUnit` → **`N+M(+L)`** 条逻辑 shard → **`Location` / `ObjExtentKey`**；下标 `i` → **`Units[i]`** → **`(DiskID,Vuid,Bid)`** 上的 **Shard**。多 AZ 由 **`GetECLayoutByAZ`** 与 **`PutQuorum` / 整 AZ 容忍 / `genSortedVuidByIDC`** 共同刻画。

---

## 7. Access 读写路径（关键函数）

EC 编解码、PutQuorum、多 AZ 容忍等见 **§6**；本节只列 **HTTP 边界 → Stream → BlobNode RPC** 的快速索引，便于 `grep`/跳转。

### 7.1 写路径（`POST/PUT /put`、`/putat`）

| 步骤 | 文件 | 函数 |
|------|------|------|
| HTTP 入口 | `access/server.go` | `Service.Put`、`Service.PutAt` |
| 业务 | `access/stream/stream_put.go`（putat：`stream_putat.go`） | `Handler.Put`、`Handler.PutAt` |
| 分配 | `stream_put.go` | `allocFromAllocatorWithHystrix`、`Handler.allocFromAllocator`（内部） |
| 展开 blob | `common/proto/blob_ext.go` | `Location.Spread` |
| EC | `stream_put.go` | `ec.NewBuffer`、`Encoder.Split`、`Encoder.Encode` |
| 写各盘 | `stream_put.go` | `writeToBlobnodesWithHystrix` → `writeToBlobnodes` |
| RPC | `api/blobnode/shard.go` | `Client.PutShard` → `POST .../shard/put/...` |
| 服务端 | `blobnode/service.go` | `Service.ShardPut` |

失败清理：`Handler.clearGarbage`（避免分配后写失败遗留）。

### 7.2 读路径（`POST /get`）

| 步骤 | 文件 | 函数 |
|------|------|------|
| HTTP 入口 | `access/server.go` | `Service.Get` |
| 业务 | `access/stream/stream_get.go` | `Handler.Get` |
| 拆范围 | 同上 | `genLocationBlobs` |
| 小读优化 | 同上 | `getDataShardOnly`（尽量只读数据片） |
| 取卷/排序 | 同上 | `getVolume`、`genSortedVuidByIDC` |
| 拉 shard / 管道 | 同上 | `readBlob`、pipeline 与 `pipeBuffer` |
| EC 还原 | 同上 | `Reconstruct` / `ReconstructData` / `Join`（`common/ec`） |
| RPC | `api/blobnode/shard.go` | `GetShard` 等 |

**Access 对外路由注册**：`access/service.go` → `NewHandler`（`/put`、`/get`、`/alloc`、`/delete`、`/sign` 等）。

---

## 8. 术语对照（避免混淆）

阅读 **§3～§5** 后可用本表快速对齐「逻辑 vs 物理」叫法；与 **§6** 中 shard 下标、`Units[i]` 对照使用。

| 术语 | 层次 | 说明 |
|------|------|------|
| **Location** | 逻辑 | 整个对象的编码地址（含多 `Slice`）。 |
| **Slice** | 逻辑 | 同一 `Vid` 上连续 `Bid` 段的描述，非磁盘 shard。 |
| **Blob / BlobUnit** | 逻辑 | EC 条带前的一条用户数据块（`Vid`+`Bid`+`Size`）。 |
| **Shard** | 物理 | 某 `(DiskID,Vuid,Bid)` 上存储的**一条** EC 分片数据。 |
| **Chunk** | 物理 | BlobNode 上某 `Vuid` 的存储上下文。 |
| **Unit** | 集群 | 卷上一条槽位：`Vuid` + `DiskID` + `Host`。 |

---

## 9. 调度与迁移（与物理单元的关系）

- **任务抽象**：`blobstore/scheduler/migrate.go` — `Migrator`：Acquire/Renewal/Complete 等；针对 **盘、卷、分片** 的迁移与修复。
- **含义**：迁移会改变 **DiskID / Vuid(epoch)** 与路由，需与 ClusterMgr、Proxy 卷版本一致；**Bid** 作为 blob 内逻辑键在条带内保持不变（视具体任务类型而定）。

关键子模块：`shard_migrate.go`、`disk_repairer.go`、`balancer.go`、`manual_migrater.go`。

---

## 10. CubFS SDK 视角：文件如何落在 BlobStore 上（示例）

本节对应 **冷存 / BlobStore 卷** 写路径：`sdk/data/blobstore/` 使用 `blobstore/api/access` 访问 Access，**元数据**仍由 **MetaNode** 记录「逻辑 extent → BlobStore 地址」。

### 10.1 文件在体系里的「最终形态」

对客户端而言是一个 **inode + 文件字节范围**；在系统里拆成两层地址：

| 层次 | 存什么 | 典型结构 |
|------|--------|----------|
| **元数据（MetaNode）** | 该文件有哪些「冷存逻辑块」、在文件内偏移 | 有序列表 `proto.ObjExtentKey`（按 `FileOffset` 排序，见 `proto/obj_extent_key.go`） |
| **BlobStore（Access + BlobNode）** | 每个逻辑块对应一次 Put 返回的 **Location** | `ClusterID`、`CodeMode`、`Size_`、`SliceSize`、`Slices[]`（再展开为 Vid/Bid 与 EC 条带） |
| **磁盘** | EC 后的 **Shard** | `(DiskID, Vuid, Bid)` 上的字节 |

**`ObjExtentKey` 与 `Location` 的对应关系**（SDK 显式转换）：

- 写入成功后，`blobstore/common/proto.Location` 被填进 **`proto.ObjExtentKey`**：`Cid`←`ClusterID`，`CodeMode`，`Size`←`Size_`，`BlobSize`←`SliceSize`，`Blobs`←各 `Slice` 的 `(MinBid, Count, Vid)`，`FileOffset`←本段在文件中的起始偏移，`Crc`←`Location.Crc`。  
- 实现见：`sdk/data/blobstore/blobstore_client.go` 的 `locationToObjExtentKey`，以及 `writer.go` 里 `writeSlice` 组装 `wSlice.objExtentKey`。

**一句话**：**Meta 不存 Shard 细节**，只存足够信息重建 **`Location`**；真正条带在 **ClusterMgr/Proxy 维护的 Vid→磁盘** 上，读时再解析。

### 10.2 数值示例（便于脑补）

假设单卷配置下单次 `Put` 对应数据量受 **`util.ExtentSize`（如 128MB）** 等策略约束，`BlobStoreClient.Put` 按块上传（见 `blobstore_client.go` 中按 `ExtentSize` 循环 `Put` 的路径）。

- 用户连续向一个 **BlobStore 文件** 追加 **300MB**（简化：三次 flush，各约 100MB，或两块 128MB + 一块剩余；以实际 `blockSize`/flush 为准）。  
- 每次 `Put` 返回一个 **`Location`**，SDK 转成 **`ObjExtentKey`**，字段示例含义：
  - `FileOffset`：`0`、`134217728`、`268435456`（示例，按实际块边界）。  
  - `Size`：本段逻辑大小（如 128MB 或最后一块较小）。  
  - `Blobs`：若干 `{Vid, MinBid, Count}`，表示本段在哪些卷、哪些连续 Bid 上占用的**逻辑 blob**。  
- 每个逻辑 blob 在 Access 内部再被 **EC 切成 N+M(+L) 个 Shard**，落到不同 **BlobNode** 的 **(DiskID,Vuid)** 上（与 **§6（纠删码）**、**§7（读写路径）** 一致）。

因此：**一个文件 = 多条 `ObjExtentKey`（Meta）→ 每条对应 BlobStore 一次对象写入 → 物理上多条 EC Shard**。

### 10.3 SDK 调用链（写入）

1. **`sdk/data/blobstore/writer.go`**：`Writer.Write` / `doParallelWrite` / `writeSlice` → 调 **`BlobStoreClient.Write`**（或流式 `Put`）。  
2. **`sdk/data/blobstore/blobstore_client.go`**：`access.New` 得到 HTTP 客户端 → **`client.Put(ctx, &access.PutArgs{Size, Body})`**。  
3. **网络对端**：`blobstore/access` 服务 **`POST /put`** → `stream_put.go` 分配、EC、写 BlobNode（见前文）。  
4. **写回 Meta**：`writer.go` 在 slice 全部成功后 **`mw.AppendObjExtentKeys(ino, oeks)`**（`sdk/meta/api.go`），把 **`ObjExtentKey`** 持久化到 inode。

### 10.4 写入过程中各模块的支持与意义

| 模块 | 在 CubFS 写路径中的作用 | 意义 |
|------|-------------------------|------|
| **客户端 SDK（blobstore Writer + BlobStoreClient）** | 缓冲、按块并发写 EBS、把 `Location` 转为 `ObjExtentKey`、触发 Meta 追加 | 隔离上层文件语义与 BlobStore HTTP API；重试、限流（如 `LimitManager`）可在此层做。 |
| **Access** | 选 `CodeMode`、向 Proxy **申请空间**、**EC 编码**、并发 **PutShard** 到各 BlobNode | **统一入口**与纠错策略；失败时 `clearGarbage` 避免悬挂分配。 |
| **Proxy** | **卷/空间分配**、缓存卷视图、向 ClusterMgr 心跳 | 决定 **Vid/Bid** 从哪来；Access 依赖其返回的分配结果。 |
| **ClusterMgr** | **集群拓扑、服务注册、卷与磁盘路由** | **权威元数据**：DiskID→Host、卷健康与版本；无它则无法从 Vid 找到物理节点。 |
| **BlobNode** | 在指定 **DiskID/Vuid/Bid** 上持久化 **Shard** | **真实落盘**；Chunk 管理生命周期；迁移/修复时 Scheduler 与此交互。 |
| **MetaNode（CubFS Meta）** | 存储 **`ObjExtentKey` 列表** 与文件长度 | **命名空间与逻辑偏移**：读时根据 offset 找到对应 `ObjExtentKey`，再调 Access `Get`。 |
| **Scheduler（异步）** | 迁移、均衡、修复、删 blob 等 | **不阻塞单次 Put**；保证长期一致性与空间回收。 |

**纠删码在这一条链上的位置**：仅在 **Access** 对「一个逻辑 blob」做 **Encode**；SDK 只处理 **`Location`/`ObjExtentKey`**，不直接操作 Shard。

---

## 11. 审核关注点（自行走查）

- **高风险**：`PutQuorum` 与写成功判定、删除与 GC、`Location.Crc` 校验。
- **中风险**：`Vuid` epoch 与迁移一致性、惩罚盘（`Punished`）跳过行为。
- **低风险**：HTTP 路径与指标。

---

*文档随代码演进可能需更新；纠删与缓冲以 `blobstore/common/codemode`、`common/ec` 为准，读写以 `access/stream`、元数据以 `blobstore/common/proto` 为准。*
