# #278 追加对账：完整快照改为只传差量

关联：<https://github.com/anywhere-labs/Agents-Anywhere/issues/278>

## 问题

完整快照（`timeline.sync`，`complete=true`）会替换整条时间线，所以以前只能整份放在一个请求里。DSH 的一个 62 MiB 会话超过了公网入口约 50 MiB 的上限，请求被 413 拒绝，永远上传不了。PR 291 只在 Connector 侧止血：被拒的会话被隔离为 `history_too_large`，其他会话照常同步，但这个会话的历史仍然看不到。

这个问题不只出现在 DSH。Pi 每个会话第一次同步都是完整快照；Codex 在条目被删除或检查点失效时，也会发送完整快照。而且只要删掉一个条目，就得把整个会话重新上传一遍。

## 参考：Claude Code Remote Control

调研了 Claude Code 2.1.296 的公开文档和本机二进制中的字符串。它们踩过同样的坑：v2.1.209 把整份对话记录放在创建会话的一个请求里，约 10 MB 时失败（anthropics/claude-code#78825），后来改掉了。RC 的对话记录只追加不改写：按 uuid 幂等，按条数和字节数分批上传，遇到 413 把这一批拆小重发，冲突时采用服务端的当前位置，读取全部用游标分页。

AA 的时间线是 runtime 原生历史的投影，会出现删除和重排，例如 Pi 切换分支、Codex 删除条目、DSH 的投影版本升级，所以不能照搬“只追加”。这里采用的是追加加对账，对应关系如下：

| RC | AA |
| --- | --- |
| 按 uuid 追加，重复写入不出错 | 按条目 ID 写入，内容和顺序都没变的条目，服务端直接跳过 |
| 客户端不知道服务端有什么时，用 `Last-Uuid` / 409 采用服务端的位置 | 先读服务端清单（ID、`orderSeq`、内容哈希），只传不同的部分 |
| 分批上传，每批有上限 | 差量走原来的增量分页，每页 8 MiB |
| 只追加，没有删除 | 增加 `timeline.delete`，只删除从清单里读到、快照中已经没有的 ID |
| 只回填最近 200 条 | **不采用**：AA 保留完整历史 |

## 协议

字段和语义见 [`docs/api/realtime.md`](../../api/realtime.md) 的 Reconciling complete snapshots 一节，这里只记录设计取舍：

- 新增 `POST /api/v2/connector/timeline/manifest`，分页返回服务端已存条目的 `(id, orderSeq, contentHash)`。
- `timeline.sync` 增加 `orderAuthoritative`：按 Connector 给的 `orderSeq` 写入，已有条目也照此调整。
- 新增 `timeline.delete`，用于删除条目。
- 删除条目、移动条目、插到中间都视为 reset：服务端设置 `timeline_reset_seq` 并发出 refetch，客户端用现有的重新拉取逻辑收敛。Web、Desktop、Android、iOS 都不用改。追加在末尾的条目仍然按普通的条目事件推送，大会话首次同步时可以逐步看到。
- 只删除从清单里读到的 ID。读完清单之后才写入服务端的条目不会被删，在这一点上比整份替换更安全。
- 服务端解析会话时，用的是第一个带 `source.sessionId` 的条目，而不是参数里的 `externalSessionId`。所以清单和删除请求都使用条目里的这个 ID；没有变化时，同步里也放一个没变的条目，保证三者落到同一个会话。
- 读清单发生在改写之前；改写后的通知会替换原来的完整快照，留在待发队列里。网络中断后，从第一个未被接受的页继续，不会重新读清单，也不会重传已被接受的页。
- 兼容性：
  - 旧版 Server 对清单接口返回 404，Connector 就照旧整份发送，10 分钟后再试。
  - 旧版 Connector 继续发送完整快照，服务端照旧整份替换。
- DSH 插件等 commit 的 ACK 最多 60 秒，所以 relay 在第一页到达时读取清单，并先发送会话元数据（条目必须先有所属的会话）。之后每一页的差量都在这一页的 ACK 之前上传完；commit 时只发送元数据和删除请求。relay 的对账遇到问题时（Server 没有清单、某一页被拒），commit 时退回通用路径，由 ingest 客户端重新对账。DSH 插件和 bridge 协议都没有改动。

被放弃的方案是“分片暂存后一次性替换”，代码保存在 `git stash` 中（`issue-278: staged snapshot implementation`）。放弃的原因：它保住了原子性，但每次完整快照仍然要全量上传，服务端提交时还要在内存里拼出整份快照。

## 验证

环境：Windows 11，Python 3.12 / 3.13，Server 使用临时 SQLite，没有连接任何外部数据库或正式服务器。

- Server `tests/test_timeline_reconcile.py`：10 项通过。覆盖以下场景：
  - 清单分页，以及未建会话时返回空清单；
  - 清单接口需要 Connector 令牌；
  - 按顺序追加不触发 reset；
  - `orderAuthoritative` 移动已有条目、插到中间，都会触发 reset；
  - 删除只删除已知 ID，重复执行结果不变；
  - 畸形的删除请求返回 400；
  - 按清单对账的结果和整份替换一致；
  - 端到端：真实的 Connector ingest 客户端连真实的 Server 应用，中间加一个 256 KiB 上限的入口，首次同步约 1.2 MiB 的会话没有出现 413；再次同步时只上传了移动、编辑和新增的 4 个条目，并删除了 1 个。
- Server 原有的 `test_issue_278_snapshot_semantics.py`、`test_timeline_reconciliation.py`、`test_architecture_boundaries.py`：通过。
- Connector `tests/test_issue_278_reconcile.py`：12 项通过。覆盖以下场景：
  - 55 MiB 快照经过 50 MiB 入口同步到空的 Server，每个请求不超过 8 MiB；
  - 只上传变化的条目，删除已经没有的条目；
  - 内容没变时只发送一个锚点条目；
  - 空快照会删除全部条目；
  - 跟随清单分页，删除请求分块发送；
  - 旧版 Server（返回 404）收到整份快照，10 分钟内不再询问；
  - 清单被拒绝时整份发送；
  - 清单服务暂时不可用时，快照留待重试；
  - 网络中断后从第一个未被接受的页继续，不重读清单；
  - DSH 边收边传，commit 只发送元数据；
  - DSH 再次同步时只上传修改、删除多余的条目；
  - DSH 某一页被拒时，退回通用路径对账。
- PR 291 的相关测试（`test_bounded_ingest.py`、`test_bounded_sync.py`、`test_issue_278_dsh_snapshot.py` 等）：断言没有改，只是把 mock 换成会对清单接口返回 404 的“旧版 Server”（`tests/ingest_servers.py`）。另外，两个测试替身补上了读取清单的方法。
- 全量套件：
  - Server 947 项通过，9 项失败；Connector 1198 项通过，94 项失败、6 个错误。
  - 逐项放到基线提交 `51041ebe` 上对比：这些失败在基线上同样出现，或者属于本来就时有时无的用例，单独重跑能通过。
  - 时有时无的原因已经查明：
    - Connector 的本机记录锁会把路径哈希成 49152–65535 的端口，落进 Windows 保留端口段时 bind 必定失败。这台机器上约 4.6% 的端口被保留，已经单独提出修复建议。
    - Server 锁统计的计时用的是 `time.monotonic()`，在 Windows 上只有 15.6 毫秒精度，很短的持锁时间会被量成 0。
- 本机测量（SQLite，没有网络开销），会话约 60 MiB：

  | 场景 | 27354 个约 2 KB 的条目 | 1250 个约 50 KB 的条目 |
  | --- | --- | --- |
  | 首次同步（Server 上是空的） | 4.4 秒，8 个请求，最慢 0.58 秒 | 2.3 秒，最慢 0.40 秒 |
  | 内容没变，再同步一次 | 1.3 秒，读清单 1.1 秒，几乎不上传 | 0.4 秒，几乎不上传 |
  | 改 30 条、删 100 条 | 1.5 秒，上传 0.1 MiB | 0.5 秒，上传 0.1 MiB |

没有验证的部分：真实的 DSH Desktop 和 Nginx 闭环、PostgreSQL / Redis 部署、正式服务器上的耗时，以及 Web、Desktop、Android、iOS 客户端在对账过程中实际的显示效果（服务端发出的是这些客户端已有的 refetch 事件）。

## 已知局限

- 对账过程中，客户端可能看到中间状态：已上传的新条目先出现，删除在最后才生效。首次同步时，客户端会逐步看到历史。
- 读取清单时，服务端需要逐行解析存储的 JSON 来取出 `contentHash`。条目很多的会话，读一次清单要花一些时间，见下文的测量。
- 滚动发布时，如果清单请求打到新版 Server，删除请求却打到旧版 Server，旧版会忽略删除，留下多余的条目，直到下一次对账。
- 按 `docs/versioning.md`，新增接口和通知属于 MINOR 更新，所有端要升到 2.1.0。本 PR 按要求暂不升版本号，发版时统一处理。
