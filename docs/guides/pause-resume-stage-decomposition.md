# kuasar-sandbox create / pause / resume 阶段分解

> 2026-11-18 · cube90 · 2C2G 模板（`python:3.12-slim`）· C1 n=9 / C10 n=50
> 对照数据：CubeSandbox issue #24 §二（v0.7.1 + PR 栈 + dirtylog，C1 n=9）
> 采集工具：`test/perf/e2b_stage_bench.py`（客户端）+ `test/perf/stage_tree.py`（阶段树）

## 0. 结论（先看）

1. **pause 200 ms，CubeSandbox 36.2 ms（5.5×）**：真正“冻结 + 写快照”的 `snapshot.Take` 只有 **23.7 ms**（CubeSandbox shim 14.2 ms，同量级）；
   69% 的时间在 **ownership cleanup 138.3 ms**——fence runner **86.5**、checkpoint 清理 **31.8**、vswitch detach **18.9**。
   CubeSandbox 的对应段 `DestroyLive` 只有 7.1 ms。**kuasar 的 pause 贵在拆运行时，不在存快照。**
2. **resume 324 ms，CubeSandbox 92.8 ms（3.5×）**：`restore ACK + stdio MUX` **98.3 ms**（CubeSandbox `ResetVm` 25.7 ms）、
   spawn CH **22.4**（1.5）、CH API ready **15.1**（CubeSandbox 无此段）、`complete prepare` **19.9**；
   envd `/init` 34.9 与 CubeSandbox `sandbox-probe` 38.6 基本相当。
3. **create 312 ms**：kuasar 模板是**内存快照**（`e2b-snp-…`），create 走 restore 路径（与 resume 同一条 runtime），
   与 CubeSandbox 的 create（冒烟 36–49 ms，磁盘模板冷启）**不是同一路径**，绝对值不可比。
4. **调度/单元启动不是瓶颈**：runner 池预热让“分配并启动 runner”只有 1.4 ms。CubeSandbox 的 `sandbox-start`（44.7 ms）
   还包含 fork/exec 新 shim 与 create 机制，**口径不同、不可直接比**；能确定的是 kuasar 这一段不是瓶颈，
   它的开销都落在后面的 restore/runtime。
5. **C10**：单沙箱 pause 323.8 ms（单轮 wall 420 ms）、resume 479.0 ms；cleanup 仍占 pause 的 64%（205.8/323.8）；
   resume 的 `admission` 涨到 41.4 ms（route barrier 只占 0.39 ms，其余是 store/锁竞争，未细分）。

## 1. 方法（与 pauseprof 一致）

- **插桩**：`KUASAR_PERF=1` 打开，组件在关键点往 stderr 打一行
  `KUASAR_PERF {"pid":…,"comp":"conductor|runner|runtime","op":"create|resume|pause","rid":…,"sid":…,"evt":…,"kind":"cumulative|interval","off_ms":…,"wall_ns":…}`。
  `cumulative` 是相对本次请求起点的累计值，**只有相邻相减才有意义**；`interval` 本身就是时长。标记默认关闭，未开启时零开销。
- **采集**：客户端按轮记录每个沙箱的 create / pause / resume 墙钟（`--ops-out`），
  服务端用 `journalctl -o json` 抓同一窗口，按 `sid + 时间窗` 归属到每个操作。
- **对账**：同一进程内用单调时钟算区间；跨进程只用**时长**对齐（conductor 的 `capture` vs runtime 的 `snap.*`；
  conductor 的 `readiness_wait` vs runtime 的 `restore.total`），不拿两个进程的绝对偏移相加。
- **缺失**：日志里没有的阶段显示 `-`，不补零。跨进程包含关系在图里用缩进表示，**父子与并列不相加**。

## 2. 计时边界（三层，嵌套，不可替代）

| 层 | 口径 | 计时方 | pause C1 | create C1 | resume C1 |
|---|---|---|---:|---:|---:|
| ① 客户端 | API 调用 → 状态落地（含 HTTP + 状态轮询） | 负载脚本 | 199.7 | 312.4 | 323.8 |
| ② conductor 请求窗口 | `POST /sandboxes/{id}/pause` 的 handler 墙钟 | conductor | 197.5 | 1.5（异步） | 1.7（异步） |
| ③ runtime 进程 | 实际冻结/写盘/恢复的工作段 | run 进程 | 36.6（quiesce+take） | 172.8 | 157.9 |

- pause 是**同步**接口：① ≈ ②，差 2.2 ms 是响应写出与客户端轮询粒度。
- create / connect 是**异步**接口：② 只有 admission（1.5 ms），③ 在 response 之后由 launch worker 执行，
  客户端的 `state=running` 轮询覆盖了 ③。因此 create/resume 的 `②` 与 `③` 之间是 `[客户端等待]`，不是缺口。
- kuasar 没有 CubeSandbox 的 CubeAPI/CubeMaster 壳层：`① − ②` 只有 HTTP + 轮询，CubeSandbox 是 11.3 ms。

## 3. C=1 阶段树

### 3.1 create 树（C=1）

阶段（缩进=包含）                                       n      mean       p50       p95  CubeSandbox
--------------------------------------------------------------------------------------------
① 客户端：POST /sandboxes → state=running           9    312.39    312.07    365.94            -
    ② API 请求窗口（同步 admission 部分）                 9      1.54      1.49      2.30            -
        decode/校验                               9      0.08      0.07      0.13            -
        core.Create（admission）                  9      1.45      1.41      2.14            -
            launch preflight（镜像/快照解析）           9      0.00      0.00      0.01            -
            claim + 落库 + acceptedAt             9      0.59      0.54      1.00            -
            cache/publish 初始状态                  9      0.02      0.02      0.03            -
            proxy 路由 barrier                    9      0.51      0.50      0.68            -
    [客户端等待] response → state=running（含轮询）       9    310.84    310.76    363.65            -
    ③ 异步 launch 总计（conductor）                   9    284.03    280.55    345.53        92.80
        accept→launch worker 调度延迟               9      0.68      0.67      0.89            -
        runner 单元启动（systemd 到 assigned）         9      1.42      1.33      1.98            -
            pidfile / session                   9      0.05      0.05      0.05            -
            等待 assignment（config-socket）        9      0.98      0.94      1.32            -
            readiness socket 连接                 9      0.06      0.06      0.07            -
        runner 任务引导（bootstrap）                  9      0.83      0.82      1.05            -
        artifact prepare（runner 进程内）            9      8.53      8.68      9.49            -
        complete prepare（回执 + final spec）       9     22.04     19.82     43.26            -
        runtime 总计（restore/cold）                9    172.83    170.08    232.23        39.20
            打开 Snapshot/Sandbox 元数据             9      1.01      0.85      2.19            -
            memory 层打开 + UFFD source            9     13.29     13.33     15.06        11.10
            磁盘图重建                               9      2.98      2.92      3.52            -
            spawn CH                            9     21.75     22.07     24.55         1.50
            CH API ready                        9     16.46     16.20     19.27            -
            POST /vm.resume                     9      1.77      1.68      2.46        11.10
            restore ACK + stdio MUX             9    115.28    110.59    169.56        25.70
        readiness 等待（conductor 侧）               9    187.34    185.20    246.97            -
        workspace 磁盘 bind                       9      0.00      0.00      0.00            -
        envd /init                              9     13.77     12.43     20.19        38.60
        CommitPreparedRunning（store）            9      0.61      0.64      0.74            -
        publish/observe running                 9      0.02      0.03      0.03            -

### 3.2 pause 树（C=1）

阶段（缩进=包含）                                       n      mean       p50       p95  CubeSandbox
--------------------------------------------------------------------------------------------
① 客户端：POST /pause → state=paused                9    199.73    200.33    213.92        36.20
    ② API 请求窗口                                  9    197.48    198.31    212.06        24.90
        decode/校验                               9      0.06      0.06      0.10            -
        core.Pause（同步整段）                        9    197.40    198.23    212.00            -
            admission（锁/状态/checkpoint 限额）       9      0.02      0.01      0.02            -
            source operation 互斥                 9      0.00      0.00      0.01            -
            acceptedOps.Begin                   9      0.00      0.00      0.00            -
            proxy barrier + SetPausing          9      1.48      1.42      1.68            -
                barrier 等待 proxy 静默             9      1.00      0.96      1.22            -
                barrier commit                  9      0.07      0.07      0.08            -
                SetPausing（store）               9      0.38      0.36      0.43            -
            capture（exec sandbox-ctl snapshot）  9     56.36     55.91     60.01        14.20
                runtime: 进入快照请求                 9      0.06      0.05      0.08            -
                runtime: history/dependency 准备  9      4.86      4.48      7.36            -
                runtime: memory barrier + memory.high  9      0.08      0.08      0.12            -
                guest quiesce（sync/drop_caches/MUX）  9      6.36      6.31      6.79         0.02
                snapshot.Take 总计                9     23.69     23.51     25.47        14.20
                    CH /vm.pause                9      1.32      1.32      1.34            -
                    磁盘 E 捕获                     9      1.12      1.07      1.29         0.73
                    CH /vm.snapshot + config/state  9      4.33      4.34      4.62        14.10
                    memory 视图 + 压缩              9      4.80      4.88      5.99            -
                    BuildCaptureSource          9      6.87      6.78      7.65            -
                    写 memory/Snapshot S         9      7.54      7.63      7.92            -
                    commit + close sink         9      0.09      0.08      0.10            -
            CommitPaused（store）                 9      0.48      0.46      0.64         0.16
            ownership cleanup 总计                9    138.26    136.29    151.98         7.10
                fence runner（SIGKILL+回收）        9     86.47     84.42    102.78            -
                vswitch detach                  9     18.90     19.38     20.64            -
                checkpoint 清理                   9     31.78     31.75     35.06            -
                run dir 删除                      9      0.46      0.44      0.57            -
            publish paused                      9      0.03      0.02      0.03            -
    [客户端] response → state=paused（含轮询）          9      2.24      2.25      2.81            -
            [ctl] exec/连接 sandbox-ctl 开销        9     19.74     19.68     20.68            -

### 3.3 resume 树（C=1）

阶段（缩进=包含）                                       n      mean       p50       p95  CubeSandbox
--------------------------------------------------------------------------------------------
① 客户端：POST /connect → state=running             9    323.78    315.74    362.47        92.80
    ② API 请求窗口（同步 admission）                    9      1.66      1.66      1.93        86.70
        decode/校验                               9      0.06      0.05      0.08         6.10
        core.Connect（admission + 恢复清理）          9      1.58      1.54      1.83            -
    [客户端等待] response → state=running（含轮询）       9    322.11    314.05    360.54            -
    ③ 异步 launch 总计（conductor）                   9    295.38    298.33    343.75            -
        accept→launch worker 调度延迟               9      0.07      0.07      0.10            -
        runner 单元启动（systemd 到 assigned）         9      1.29      1.31      1.38            -
        runner 任务引导（bootstrap）                  9      0.75      0.70      1.03            -
        artifact prepare（runner 进程内）            9      6.33      6.28      6.89            -
        complete prepare（回执 + final spec）       9     19.89     19.44     22.60            -
        runtime 总计（restore）                     9    157.86    158.66    186.71        39.20
            打开 Snapshot/Sandbox 元数据             9      0.42      0.40      0.51            -
            memory 层打开 + UFFD source            9     15.73     15.73     17.18        11.10
            磁盘图重建                               9      3.88      3.67      5.11            -
            spawn CH                            9     22.36     23.35     25.04         1.50
            CH API ready                        9     15.11     13.93     18.07            -
            POST /vm.resume                     9      1.88      1.81      2.69        11.10
            restore ACK + stdio MUX             9     98.33    100.48    126.33        25.70
        readiness 等待（conductor 侧）               9    172.03    172.56    201.38            -
        workspace 磁盘 bind                       9      0.00      0.00      0.00            -
        envd /init                              9     34.86     39.40     44.73        38.60
        CommitPreparedRunning（store）            9      0.56      0.55      0.70            -
        publish/observe running                 9      0.03      0.03      0.04            -

### 3.4 pause 归因

| 段 | kuasar | CubeSandbox | 读法 |
|---|---:|---:|---|
| ① 客户端 | 199.7 | 36.2 | 5.5× |
| ② 请求窗口 | 197.5 | 24.9 | |
| capture（含 ctl 进程） | 56.4 | shim 14.2 | ctl exec/连接 19.7 + runtime 36.6 |
| └ guest quiesce | 6.4 | DisconnectAgent 0.02 | kuasar 走 sync + drop_caches + MUX 关闭，CubeSandbox 只断 agent 连接 |
| └ snapshot.Take | 23.7 | VmmSnapshot 14.1 | 冻结/快照本体同量级 |
| └└ CH /vm.pause | 1.3 | （含在 VmmSnapshot） | |
| └└ CH /vm.snapshot | 4.3 | 14.1 | kuasar 只写 config/state，memory 走 sink 直写 |
| └└ memory 读取/压缩/构建/写 | 19.2 | mem_send 5.7 | kuasar 多了一次 working-set 复制 + BuildCaptureSource |
| CommitPaused | 0.5 | MarkPaused 0.16 | |
| **ownership cleanup** | **138.3** | **DestroyLive 7.1** | **最大差距** |
| └ fence runner | 86.5 | — | SIGKILL 后轮询 cgroup `TasksCurrent==0`（5 ms 间隔；cgroup v1 上进程回收慢） |
| └ checkpoint 清理 | 31.8 | — | `orchestrator-ctl checkpoint-cleanup` 子进程 + diffstore 释放 |
| └ vswitch detach | 18.9 | — | 每次 pause 都摘端口，resume 再挂 |

结论：pause 的优化空间不在 VMM（14 ms 级），而在**收尾**。可做的方向：
(a) fence 不依赖 cgroup 任务数，改为 runner 的 pidfd/子进程直接回收；
(b) checkpoint 清理由常驻线程做，不 exec 子进程；
(c) vswitch detach 与 checkpoint 清理并行（两者互不依赖）。

## 4. resume 归因

resume = Connect → 分配 runner → restore：UFFD source → spawn CH → /vm.resume → restore ACK + MUX → envd `/init`。

| 段 | kuasar | CubeSandbox | 读法 |
|---|---:|---:|---|
| ① 客户端 | 323.8 | 92.8 | 3.5× |
| launch 总计 | 295.4 | — | |
| unit.start | 1.4 | （口径不同） | kuasar runner 池预热；CubeSandbox `sandbox-start` 44.7 还含 shim fork/exec 与 create，不能直接比 |
| artifact + complete prepare | 6.3 + 19.9 | 含在 start | 读 Snapshot S/E 元数据 + 回执 |
| runtime 总计 | 157.9 | CreatePodSandbox 39.2 | |
| └ 打开元数据 + memory source | 16.1 | RestoreVm 11.1 | |
| └ 磁盘图重建 | 3.9 | 含在 RestoreVm | |
| └ spawn CH | 22.4 | LaunchVmm 1.5 | 进程启动 + CH 加载恢复态 |
| └ CH API ready | 15.1 | — | CubeSandbox 没有单独的 API 就绪等待 |
| └ /vm.resume | 1.9 | — | |
| └ **restore ACK + stdio MUX** | **98.3** | **ResetVm 25.7** | guest 解冻后回 restore_ack，再重建 stdio MUX；最大单项 |
| envd /init | 34.9 | sandbox-probe 38.6 | 基本对等 |

结论：resume 的差距集中在 **restore ACK/MUX（98 ms）** 和 **CH 启动/就绪（37.5 ms）**。
ACK 段是 guest 侧解冻 + 反向通道建链，属于 ABI 决定的往返；若要压缩，需要把 MUX 建链与 ACK 解耦
（先 ready 再补 MUX），或者让 guest 在 restore 时并行做解冻准备。

## 5. C=10

### 5.1 create 树（C=10）

阶段（缩进=包含）                                       n      mean       p50       p95  CubeSandbox
--------------------------------------------------------------------------------------------
① 客户端：POST /sandboxes → state=running          50    440.11    440.07    528.74            -
    ② API 请求窗口（同步 admission 部分）                50     17.99     16.98     48.93            -
        decode/校验                              50      0.30      0.24      0.77            -
        core.Create（admission）                 50     17.66     16.68     48.36            -
            launch preflight（镜像/快照解析）          50      0.00      0.00      0.01            -
            claim + 落库 + acceptedAt            50      9.33      5.72     34.32            -
            cache/publish 初始状态                 50      0.02      0.02      0.03            -
            proxy 路由 barrier                   50      0.46      0.39      0.60            -
    [客户端等待] response → state=running（含轮询）      50    422.12    427.14    500.09            -
    ③ 异步 launch 总计（conductor）                  50    397.69    406.24    485.95        92.80
        accept→launch worker 调度延迟              50      0.63      0.57      0.78            -
        runner 单元启动（systemd 到 assigned）        50      3.16      1.78     12.62            -
            pidfile / session                  50      0.05      0.05      0.07            -
            等待 assignment（config-socket）       50      2.58      1.26     12.11            -
            readiness socket 连接                50      0.07      0.07      0.10            -
        runner 任务引导（bootstrap）                 50      5.28      1.20     22.58            -
        artifact prepare（runner 进程内）           50      9.10      9.03     10.70            -
        complete prepare（回执 + final spec）      50     83.35     80.20    148.40            -
        runtime 总计（restore/cold）               50    175.56    172.21    208.99        39.20
            打开 Snapshot/Sandbox 元数据            50      1.12      0.90      2.88            -
            memory 层打开 + UFFD source           50     15.01     14.90     18.24        11.10
            磁盘图重建                              50      3.62      3.50      4.85            -
            spawn CH                           50     23.15     21.64     29.38         1.50
            CH API ready                       50     18.66     18.05     28.73            -
            POST /vm.resume                    50      1.93      1.76      3.00        11.10
            restore ACK + stdio MUX            50    111.88    110.98    145.29        25.70
        readiness 等待（conductor 侧）              50    194.80    193.63    228.07            -
        workspace 磁盘 bind                      50      0.00      0.00      0.00            -
        envd /init                             50     17.24     15.40     35.25        38.60
        CommitPreparedRunning（store）           50      0.59      0.51      0.77            -
        publish/observe running                50      0.02      0.02      0.03            -

### 5.2 pause 树（C=10）

阶段（缩进=包含）                                       n      mean       p50       p95  CubeSandbox
--------------------------------------------------------------------------------------------
① 客户端：POST /pause → state=paused               50    323.78    324.00    433.55        36.20
    ② API 请求窗口                                 50    320.63    321.28    430.80        24.90
        decode/校验                              50      0.25      0.22      0.73            -
        core.Pause（同步整段）                       50    320.35    320.53    430.65            -
            admission（锁/状态/checkpoint 限额）      50      0.02      0.02      0.03            -
            source operation 互斥                50      0.01      0.00      0.03            -
            acceptedOps.Begin                  50      0.00      0.00      0.00            -
            proxy barrier + SetPausing         50      2.81      1.71      4.38            -
                barrier 等待 proxy 静默            50      1.01      0.92      1.53            -
                barrier commit                 50      0.08      0.07      0.10            -
                SetPausing（store）              50      1.68      0.53      3.45            -
            capture（exec sandbox-ctl snapshot） 50     75.91     72.10    116.58        14.20
                runtime: 进入快照请求                50      0.06      0.06      0.08            -
                runtime: history/dependency 准备 50      4.61      4.46      5.45            -
                runtime: memory barrier + memory.high 50      0.09      0.09      0.14            -
                guest quiesce（sync/drop_caches/MUX） 50      7.44      6.70     13.89         0.02
                snapshot.Take 总计               50     29.02     25.99     47.91        14.20
                    CH /vm.pause               50      2.17      1.42      4.60            -
                    磁盘 E 捕获                    50      1.61      1.20      4.20         0.73
                    CH /vm.snapshot + config/state 50      5.84      4.60     10.98        14.10
                    memory 视图 + 压缩             50      4.71      4.66      6.08            -
                    BuildCaptureSource         50      8.09      7.48      9.95            -
                    写 memory/Snapshot S        50     10.22      8.18     27.42            -
                    commit + close sink        50      0.10      0.09      0.12            -
            CommitPaused（store）                50      8.06      2.08     29.15         0.16
            ownership cleanup 总计               50    205.78    191.21    298.46         7.10
                fence runner（SIGKILL+回收）       50    109.97    108.82    165.60            -
                vswitch detach                 50     59.77     55.03    105.80            -
                checkpoint 清理                  50     34.42     33.08     44.76            -
                run dir 删除                     50      0.49      0.45      0.55            -
            publish paused                     50      0.03      0.03      0.04            -
    [客户端] response → state=paused（含轮询）         50      3.15      2.85      4.63            -
            [ctl] exec/连接 sandbox-ctl 开销       50     32.97     28.24     74.37            -

### 5.3 resume 树（C=10）

阶段（缩进=包含）                                       n      mean       p50       p95  CubeSandbox
--------------------------------------------------------------------------------------------
① 客户端：POST /connect → state=running            50    479.02    486.71    560.38        92.80
    ② API 请求窗口（同步 admission）                   50     41.41     42.54     69.48        86.70
        decode/校验                              50      0.17      0.08      0.43         6.10
        core.Connect（admission + 恢复清理）         50     41.20     42.46     69.43            -
    [客户端等待] response → state=running（含轮询）      50    437.62    442.19    505.48            -
    ③ 异步 launch 总计（conductor）                  50    407.82    409.95    484.79            -
        accept→launch worker 调度延迟              50      0.09      0.08      0.13            -
        runner 单元启动（systemd 到 assigned）        50      3.42      1.57     21.41            -
        runner 任务引导（bootstrap）                 50      3.14      1.03     15.46            -
        artifact prepare（runner 进程内）           50      6.82      6.82      7.59            -
        complete prepare（回执 + final spec）      50     79.68     70.89    143.74            -
        runtime 总计（restore）                    50    168.95    168.27    198.95        39.20
            打开 Snapshot/Sandbox 元数据            50      0.48      0.44      0.95            -
            memory 层打开 + UFFD source           50     17.92     17.66     21.40        11.10
            磁盘图重建                              50      4.57      4.20      7.42            -
            spawn CH                           50     25.44     25.80     30.75         1.50
            CH API ready                       50     15.67     15.25     19.70            -
            POST /vm.resume                    50      2.25      1.69      7.11        11.10
            restore ACK + stdio MUX            50    102.42     98.72    128.56        25.70
        readiness 等待（conductor 侧）              50    188.92    186.58    215.72            -
        workspace 磁盘 bind                      50      0.00      0.00      0.00            -
        envd /init                             50     41.22     39.44     60.79        38.60
        CommitPreparedRunning（store）           50      0.59      0.48      1.50            -
        publish/observe running                50      0.03      0.03      0.04            -

### 5.4 C10 读法

- pause 单沙箱从 199.7 → 323.8 ms（单轮 wall 420 ms）：cleanup 从 138.3 → 205.8（fence 110、detach 60），`CommitPaused` 从 0.5 → 8.1（store 写放大），
  capture 从 56 → 76（memory 写 7.5 → 10.2、ctl 开销 20 → 33）。
  **并发下依然是收尾主导**，且 vswitch detach 的锁竞争开始显现（p95 105.8）。
- resume 单沙箱从 323.8 → 479.0 ms（单轮 wall 567 ms）：admission 从 1.7 → 41.4。这 41 ms **不是**上次 pause 的残留清理
  （窗口内没有任何 cleanup 标记），proxy route barrier 也只占 0.39 ms；它是 Connect 内部 store/生命周期锁在 10 并发下的争用，
  目前没有更细的埋点，**列为未观测**。
- 与 CubeSandbox §2.1 的 C10 同型：CubeSandbox pause 34.7 → 84.3（+49.6），kuasar 199.7 → 323.8（+124）；
  但 kuasar 的增长主要来自 fence/detach，CubeSandbox 来自 VMM snapshot 写盘（mem_send write 5.7 → 26.2）。
  **优化重点不同：CubeSandbox 要压脏页写，kuasar 要压运行时回收。**

## 6. 差距清单（按收益排序）

| # | 阶段 | kuasar C1 | CubeSandbox C1 | 差 | 建议 |
|---|---|---:|---:|---:|---|
| 1 | ownership cleanup（pause） | 138.3 | 7.1 | +131.2 | fence 改 pidfd/kill 后立即返回；checkpoint 清理异步化；detach 与清理并行 |
| 2 | restore ACK + MUX（resume/create） | 98.3 / 115.3 | 25.7 | +72~90 | MUX 建链与 restore_ack 解耦；guest 侧并行化解冻准备 |
| 3 | spawn CH + API ready | 38.2 | 1.5 | +36.7 | 预热 CH 进程/复用 API socket；检查 CH 启动参数与恢复态加载 |
| 4 | complete prepare | 19.9 | — | — | 回执与 final spec 合并为一次 RPC |
| 5 | ctl exec/连接（pause） | 19.7 | 0 | +19.7 | capture 改为持久 UDS 客户端，避免每次 exec `sandbox-ctl` |
| 6 | envd /init | 34.9 | 38.6 | -3.7 | 已对等 |
| 7 | runner 单元分配/启动 | 1.4 | — | — | kuasar 池化，已不是瓶颈（不与 `sandbox-start` 直接比） |

## 7. 复现

```bash
# 1) 打开埋点（两个 drop-in 文件，值为 1）
#    /etc/systemd/system/kuasar-conductor.service.d/20-perf.conf
#    /etc/systemd/system/sandbox-runner@.service.d/20-perf.conf
#    Environment=KUASAR_PERF=1
systemctl daemon-reload && systemctl restart kuasar-conductor

# 2) 采集（C=1 n=9，预热 checkpoint 页缓存）
source /home/lzh/kuasar/config/tenant.env
source /home/lzh/kuasar/config/template-2c2g.env
cd /home/lzh/kuasar/kuasar-sandbox
export E2B_API_URL=http://127.0.0.1:3000 E2B_API_KEY="$AK" E2B_TEMPLATE_ID="$TID"
date +%s > /tmp/t0
CONCURRENCY=1 ROUNDS=9 WARM=1 OUT=/tmp/c1.jsonl bash test/perf/e2b-stage-bench.sh

# 3) 导出 journal 并出树
journalctl -o json --no-pager --since "@$(cat /tmp/t0)" > /tmp/c1.journal.json
python3 test/perf/stage_tree.py --ops /tmp/c1.jsonl.ops.jsonl \
  --journal /tmp/c1.journal.json --md c1.tree.md --json c1.tree.json
```

原始数据（本次）：cube90 `/home/lzh/kuasar/perf-2c2g/{c1,c10}.jsonl*`、`{c1,c10}.journal.json`。

## 8. 局限

- 环境是 privileged 容器 + cgroup v1 + `KUASAR_NO_CGROUP`，与生产 cgroup v2 的 `Delegate=` 路径不同；
  fence/cleanup 的绝对值会受影响（本报告已把这一点计入归因）。
- kuasar 的 pause 走 **destroy** 语义（runner 单元退出、CH 进程销毁），CubeSandbox 是 shim 内 DestroyLive，
  两者“pause 后还能不能保留进程”本就不同，cleanup 不可完全对消。
- 模板/镜像不对等：kuasar `python:3.12-slim`（43.5 MB）vs CubeSandbox `sandbox-code-envd-ci`（≈379 MiB）；
  本报告只比较**同一平台内的阶段结构**，跨平台数字看量级。

## 9. 补充实验：create C1 归因（2026-11-18 晚）

第 3 节的 create 树是在 **runner 池未开启**（`units.runner_pool_size` 缺省 0）时采集的。补做两个实验：

### 9.1 开 runner 池：create 312 → 245 ms

`conductor.yaml` 加：

```yaml
units:
  runner_pool_size: 2
```

重启后 systemd 常驻 2 个 idle `sandbox-runner@`，create C1（n=9）从 **312.4 → 244.7 ms（−67.3，−21%）**。
未开池时 `launch.enter → runner 进程真正开始` 平均 **49.4 ms**（systemd StartUnit + fork/exec），
开池后这段变成 0（单元已就绪，只等 assignment）。

### 9.2 restore 段再拆：dial 67 ms + guest ack 40 ms

在 `guestlink.OpenMUXViaRestoreContext` 里把原来的 `restore ACK + MUX`（108.5 ms）再拆成：

| 子段 | 含义 | mean | 范围 |
|---|---|---:|---:|
| `restore.dial` | unix dial CH hybrid-vsock + `CONNECT`/`OK` 握手（等 guest accept） | **~67** | 54–98 |
| `restore.guest_ack` | 写 `restore{epoch}` → guest 回 `restore_ack`（含 usage-report barrier、MUX 重建） | **~40** | 34–48 |

没有出现 `pre-request connect recovered on attempt N` 日志，说明不是重试退避；两次都是第一次连接就成功，
耗时在**握手与 guest 处理本身**。

### 9.3 create C1 账本（开池，n=10，含 1 次预热）

| 段 | ms | 占客户端 |
|---|---:|---:|
| ① 客户端（含 26 ms 轮询量化） | 251.7 | 100% |
| API 窗口（HTTP + admission） | 1.5 | 0.6% |
| runner 池等待 | ~0 | 0% |
| bootstrap + artifact prepare | 9.8 | 3.9% |
| complete prepare（config-socket 回执） | 20.5 | 8.1% |
| exec `sandbox-ctl` | 14.3 | 5.7% |
| **restore 总计** | **162.1** | **64%** |
| ├ 打开元数据 + memory source + 磁盘图 | 17.2 | 6.8% |
| ├ spawn CH + CH API ready | 34.6 | 13.7% |
| ├ `/vm.resume` | 1.6 | 0.6% |
| └ vsock dial + guest ack | 108.5 | 43% |
| envd `/init` | 14.5 | 5.8% |
| store commit + publish | 0.6 | 0.2% |
| 客户端轮询量化 | 26.2 | 10.4% |

按 CubeSandbox 的 create 口径（`POST /sandboxes` 返回即完成、无轮询），kuasar 的可比数字是 **~220 ms**（`orch.launch.exit`），
仍为 CubeSandbox v0.7.1 C1（57.6 ms）的 3.8×。

### 9.4 与 CubeSandbox C1（57.6 ms）逐段对照

| 段 | kuasar（开池） | CubeSandbox v0.7.1 C1 | 差 | 说明 |
|---|---:|---:|---:|---|
| HTTP + admission | 1.5 | ~9.6 | −8 | kuasar 没有 CubeAPI/Master 壳层 |
| runner/进程启动 | ~0（池化） | ~7（shim spawn） | −7 | 池化后更快；未开池时 49 |
| bootstrap + artifact + complete prepare | 30.3 | — | +30 | 读 S/E 元数据 + 两次 config-socket RPC |
| exec `sandbox-ctl` | 14.3 | 0 | +14 | CubeSandbox shim 常驻，不 exec 新进程 |
| restore 总计 | 162.1 | 27.6（CreatePodSandbox） | +134 | |
| ├ 元数据/内存/磁盘 | 17.2 | 8.0（RestoreVm） | +9 | FICLONE+惰性内存 vs 打开 S/E + UFFD source |
| ├ spawn CH + API ready | 34.6 | 含在 RestoreVm/LaunchVmm 1.0 | +26~34 | CH 进程启动 + /vm.info 就绪 |
| ├ `/vm.resume` | 1.6 | 含 | — | 两边都很快 |
| └ vsock dial + guest ack | 108.5 | 4.5（ResetVm） | +104 | **最大单项** |
| envd `/init` | 14.5 | 9–14（sandbox-probe） | ~0 | 对等 |
| store commit + publish | 0.6 | — | — | |
| 客户端轮询 | 26.2 | 0（同步接口） | +26 | 测量口径差异 |

**create 的结论**：

1. 不是调度问题：开池后 runner 启动为 0；不开池的 49 ms 也只是配置缺失（`runner_pool_size` 默认 0）。
2. 不是 VMM 恢复本身：`/vm.resume` 只要 1.6 ms，状态恢复不慢。
3. **47% 的客户端时间在 restore 后的 guest 握手**：hybrid-vsock `CONNECT/OK` 67 ms + `restore_ack` 40 ms。
   CubeSandbox 的 agent 通道在 restore 时只花 4.5 ms。下一步应查：CH hybrid-vsock 的 accept 是否被 guest 调度/缺页拖慢
   （UFFD 惰性内存下，guest vsock/agent 代码页第一次触碰要回读快照），以及 guest 侧 usage-report barrier 能否与 ACK 解耦。
4. 进程模型带来 ~44 ms 固定开销（artifact/complete prepare 30 + exec sandbox-ctl 14），CubeSandbox 靠常驻 shim 省掉。
5. 另有 26 ms 是客户端 50 ms 轮询的量化误差，按对方口径应剔除。
