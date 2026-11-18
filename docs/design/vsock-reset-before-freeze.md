# 方案 v3：把 vsock transport reset 挪到 freeze 前（去掉 restore 的 epoch gate）

> 范围：只改 restore 时 host↔guest 控制通道的建立（差异 C）。不改 guest ABI、不改 MUX、不动模板/进程/API。
> 目标：restore 的 dial 从 60–70 ms 降到 1–3 ms；正确性不退化（失败自动回退到现有 gate）。
> 版本记录：v1 RST 注入（已否决：连接表在冻结时基本为空，覆盖不到 guest 残留）→ v2 reset-before-freeze → **v3 补齐 CH 状态机/ACK 语义/失败回退/测试口径**。

## 1. 背景（实测）

cube90，2C2G，C=1，n=9，开 runner 池：

| 段 | 耗时 | 说明 |
|---|---:|---|
| create 客户端总计 | 244.7 ms | 含 26 ms 轮询量化 |
| restore 总计 | 162 ms | |
| ├ 元数据/memory source/磁盘图 | 17 ms | |
| ├ spawn CH + CH API ready | 35 ms | |
| ├ `/vm.resume` | 1.6 ms | VMM 恢复本身很快 |
| └ **vsock dial + guest ack** | **108 ms** | dial 60–70 + ack 38–40 |

dial 是"等对端就绪"：在 `/vm.resume` 后人为延迟再拨号，dial = **60.4 / 23.8 / 0.8 ms**（延迟 0/50/100 ms），端到端不变（235/262/287 ms）。guest 日志在 `/vm.resume` 后 **71.9 ms** 才出现第一条 restore 日志。

根因：`ch-patches/0006` 在**快照时** stage `VIRTIO_VSOCK_EVENT_TRANSPORT_RESET`，restore 后 CH 扣住 RX，直到 guest 处理完 reset（关闭所有 connected socket、归还 event descriptor、kick）才放行。

`0005`（host 本地端口游标持久化）与 gate 无关，**保留**。

## 2. 设计：reset-before-freeze

### 2.1 顺序

```
quiesce（app 冻结、MUX/forward/exec/pinger/usage/mem_report 全部关闭）
  → 可失败检查全部做完（CaptureState、disk check 等）
  → reset-now（guest 仍在运行，页面全热；guest 内核关闭所有 connected socket 并 kick）
  → /vm.pause → /vm.snapshot（不再 stage）
restore：backend 空表 + guest 无旧 socket → 不需要 gate
```

- reset 时 guest 运行中、内存全热，处理 reset 不需要 UFFD 回读；预期远快于 restore 时的 60–70 ms（T 待实测）。
- 快照 state 为 `transport_reset_pending=false`（若 ACK 成功）；restore 的 dial 立即返回。
- 旧快照（`pending=true`）继续走现有 gate 路径，**天然兼容**。

### 2.2 CH 状态机（`ch-patches/0015`）

新增两个状态，语义必须严格：

| 状态 | 含义 | 存储 |
|---|---|---|
| `transport_reset_pending` | RX gate 是否关闭（等 guest ACK） | **已序列化**（0006 已有） |
| `reset_published` | 自上次 activate/resume/reset 以来，是否已经发布过 reset 事件 | **运行期，不序列化** |

规则：

1. **发布 reset**（运行中或快照时）：写 event ring、推进 used、拉 IRQ；`pending=true`、`reset_published=true`。
2. **ACK**：guest 处理完事件后 **推进 event queue 的 avail idx**（不是"看到一次 kick"）。ACK 后 `pending=false`；`reset_published` 保持 true。
3. **`snapshot()`**：若 `reset_published==true` → **不再 stage**，直接序列化当前 state（`pending` 可能是 true：发布成功但 ACK 超时；也可能是 false：ACK 成功）。
   若 `reset_published==false` → 走 0006 原逻辑 stage（并置两个状态）。
4. **超时**：绝不主动清 `pending`、绝不重复 stage。超时即"已发布未 ACK"，快照 state 为 `pending=true`，restore/source-resume 走 gate（现有行为）。
5. **`activate()` / `resume()` / `reset()`**：`reset_published=false`；`pending` 仅从 state 恢复（旧快照为 true）。
6. **旧快照**：state 里 `pending=true` 且没有 `reset_published` 字段（不序列化）→ restore 走 gate；完全兼容。
7. **无 vsock 设备/设备未激活**：返回 `published=false`，不视为成功。

### 2.3 CH 执行位置与并发

- **发布必须在 vsock worker 内执行**（命令 eventfd + `VSOCK_RESET_EVENT`），使用 worker 手里**活的** `queues[2]`/`mem`/`queue_evts[2]`：
  - 保证与 `process_rx` 的原子性（先完成在途 RX，再置 gate）；
  - 避免 VMM 线程直接动 ring；
  - 不需要暂停设备 worker（避免与后续 `/vm.pause` 的二次暂停死锁）。
- API 线程只写命令 eventfd，然后**不持锁**轮询 `pending`（原子变量）直到超时；worker 在 EVT 分支检测 guest kick + avail 推进，清 `pending`。
- **ACK 判定**：发布前记录 event queue 的 `avail.idx`；只有 guest 归还 descriptor 并推进 avail 才认为 ACK。单纯 kick 不算（防止 stale kick 误判）。
- 等待期间不得持有 VMM/API 大锁；`/vm.info`、`/vm.pause` 等不能被阻塞超过超时窗口。

### 2.4 CH API

```
PUT /api/v1/vm.vsock-reset
body: {"timeout_ms": 200}          # 可选，默认 200
resp: {"published": true, "acked": true}
```

- `published=false`：未发布（设备未激活、无 descriptor 等），host 继续让 snapshot 走 0006 staging；
- `published=true, acked=false`：已发布但超时，host 继续 snapshot（state=pending=true，restore 走 gate）；**不得**再次 stage；
- 非 `Running` 状态 → 4xx/5xx 明确错误；
- 幂等：`reset_published==true` 时返回 `published=true` 与当前 `acked` 状态，不重复发布。

### 2.5 Host 层（sandboxer）

1. `pkg/chapi`：`VsockReset(ctx, timeout) (published, acked bool, err error)`；读取 JSON body（现有 `do` 只读 4 KiB，需补一个读体变体）。
2. `pkg/sandbox/lifecycle.go`（**只 snapshot 路径**）：
   - 插入点：`forwarder.Drain()` + `memoryController.CaptureState()` 之后、`snapshot.Take(...)` 之前；
   - `handleExportRequest`（memory=false 的磁盘导出）**不调用**：它不承载 vsock 状态，冷启动恢复，没有 gate；
   - 失败处理：调用失败或 `published=false` → 直接继续（snapshot 会 stage）；`published=true&&acked=false` → 记录 + 继续（回退 gate）。
3. **不允许**在 reset-now 之后、`/vm.pause` 之前插入任何可失败操作（避免"已 reset 但 pause 失败"的窗口）。
4. 开关（host 配置/环境变量）：`vsock_reset_before_freeze = off|on`，默认 **off（先灰度）**；关闭时完全走 0006。可随时回退，不需要换二进制。
5. 埋点：
   - `snap.vsock_reset`（Dur，reset-now 调用到返回）；
   - `snap.vsock_reset_result`（MarkKV: `published`, `acked`, `timeout`）；
   - `restore.snapshot_state`（MarkKV: `transport_reset_pending`，来自解析 state.json）；
   - `restore.dial.first_try`、`restore.dial_attempts`、`restore.dial_failed`；
   - `restore.dial` 的 mark set 需要带 sid（现在没带，C10 下会归属错乱）。

### 2.6 Guest 层
- **不改 ABI**。guest 内核照常处理 `TRANSPORT_RESET`（`vsock_for_each_connected_socket`，listener 保留），由内核 workqueue 完成，不需要 userspace。
- `sandbox-init` 的 TypeRestore 流程不变。

### 2.7 不变量

1. reset-now 只在 quiesce 成功、app 冻结之后；关闭的是残留 socket，不影响已排空的 stdio。
2. 旧 MUX 关闭在 thaw 之前；新 MUX 在 `restore_ack` 之后 attach（按 guest ABI 实际顺序）。
3. `0005` 保留。
4. **不允许出现"既不 reset 也不 gate"的快照**：要么 `reset_published=false`（snapshot 会 stage），要么 state 为 `pending=true`（restore gate）。
5. 运行中的 VM 若 `pending=true`，必须能通过 guest 处理事件自行解除；host 侧加 watchdog（超时告警，超过 N 秒未解除则终止沙箱），不允许静默悬挂。

## 3. 预期收益与代价

| 指标 | 现状 | 目标 |
|---|---:|---:|
| restore dial | 60–70 ms | 1–3 ms |
| restore 通道 | 108 ms | ~40 ms（guest ack 不变） |
| create C1（开池，含轮询） | 244.7 ms | ~180 ms |
| resume C1 | 271 ms | ~210 ms |
| **pause 增量 T** | — | 待实测（热内存预期 5–20 ms） |

- create 要拿到收益，**模板必须用新路径重新拍一次**（不是重建镜像/不换 ABI，只是重新 snapshot）；旧模板（`pending=true`）走 gate。
- 若 T 接近 60 ms，则变成 pause 与 create 的 1:1 交换；是否只在自动 pause 启用由产品决定。

## 4. 工作分解

| # | 工作 | 位置 | 验收 |
|---|---|---|---|
| 1 | CH 状态机（`reset_published`/`pending`/snapshot skip）+ worker 内发布 + ACK(avail) | `ch-patches/0015-*.patch` | `git apply --check`；单测 |
| 2 | CH API（route/action/handler/device_manager/vm）+ 幂等 | 同上 | 单测 + 集成 |
| 3 | Host chapi + lifecycle 插入 + 开关 + watchdog | `pkg/chapi`、`pkg/sandbox/lifecycle.go` | 单测 + 实机 |
| 4 | 埋点 + `stage_tree.py`（PAUSE_TREE 增加 `snap.vsock_reset`；sid 归属修复） | `pkg/perfmark`、`test/perf` | 报告 |
| 5 | 压力/回归工具（负向对照、失败注入、残留 socket、C10） | `test/perf/restore_stress.py` | 可重复 |
| 6 | 文档（vmm.md 0006 章节、guest-abi quiesce 文本、guide） | docs | 评审 |

## 5. 测试计划

### 5.1 负向对照（必须有）
- 同一二进制：`vsock_reset_before_freeze=off` → 快照 `pending=true`、`restore.dial` 回到 60–70 ms；
- `on` → `pending=false`、`restore.dial` 1–3 ms；
- 若 off 复现不出 60–70 ms，测量无效。

### 5.2 指标
- 主指标（stage marks）：`snap.vsock_reset`（T）、`restore.dial.first_try`、`restore.dial`、`restore.guest_ack`、`runtime.total`；
- 弃用 n=9 的 p95；n≥30 配对轮次，报告配对差值 median + bootstrap 95% CI；
- 客户端 wall（含 50 ms 轮询）只作次要指标；
- 分别报告 create 路径与 pause→resume 路径；再报告"每快照 N 次 resume"的加权成本。

### 5.3 功能/正确性
- 快照 state 断言：新路径 `pending=false`；回退路径 `pending=true`；
- 每个循环：resume 后第一个 exec 里 `cat /proc/net/vsock`，**stale 判定按端口**（host port ≤ 快照 `local_port_last`、guest 本地端口=5000、state=01/02/03 才算 stale）；CLOSE/CLOSE_WAIT 行不算；
- MUX 输出完整性（pause 前标记、resume 后无丢/重/截断）；
- exec 并发、envd `/init`、forward、usage/mem_report 连续；
- 旧快照（`pending=true`）restore 仍走 gate 且功能正常；
- 嵌套快照 5 层；
- `--resume`（source 继续）：reset 后不重复 reset，MUX attach 正常；
- pause 失败注入（`/vm.pause` 失败、disk check 失败）：VM 不长期处于 gated；watchdog 生效。

### 5.4 失败注入
CH 测试钩子（`KUASAR_TEST_HOOKS=1`，release 二进制忽略）：
- `delay-ack`、`drop-ack`、`no-desc`、`fail-after-publish`、`stale-kick`；
- 断言：`published=true,acked=false` → 快照 `pending=true` → restore gate 且功能正常；无二次 stage；source resume 后可用；
- ACK 落在超时边界前后各一次，结果一致（要么加速、要么回退），不允许中间态。

### 5.5 并发
- C10：10 个沙箱同时 pause（reset）、同时 resume；统计 T p50/p95/p99、超时率、pause p99；
- reset 与 forwarder.Drain 顺序（先 Drain 再 reset），带大流量 forward 的 pause；
- 连续 reset-now（不 pause）验证 ring/worker 完整性。

### 5.6 拒绝条件（即使 create 变快）
- dial 没降到 1–3 ms，或只在模板路径/只在 pause 路径生效；
- 出现 stale `01` 行、`pending=false` 但 guest 未完成 reset、或新路径快照 `pending=true`；
- 回退后 VM 可用性受损或长期 gated；
- MUX/exec/envd/forward/usage 任一回归；
- CH API 等待期间持锁阻塞其他调用；
- 负向对照失败或两臂 artifact/页缓存不可比。

## 6. 回退
- `vsock_reset_before_freeze=off` 一行回退到 0006（新 pause 继续 stage）；
- 已产生的 `pending=false` 快照无需迁移（restore 不 gate 也安全，因为 guest 已在 freeze 前 reset 完成）；
- 旧 CH 读新快照：`pending=false` 且无 `reset_published` 字段 → 不 stage、不 gate；安全性取决于新路径确实完成了 reset（本方案保证）；
- 不需要模板迁移、不需要换二进制。

## 7. 未决问题
1. CH API 的 `timeout_ms` 默认值需按 C10 T 分布定（≥ p99.9 × margin）；host 的 `ch_api` deadline 必须大于它。
2. `reset_published` 是否需要提供诊断端点（`PUT timeout_ms=0` 返回状态）。
3. `handleExportRequest` 是否永远不需要 reset（当前结论：不需要）。
4. v1 的 `connections` 导出是否保留作诊断（不作为机制）。
