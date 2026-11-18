# cube90 · kuasar-sandbox 运行环境与操作指南

> 环境快照：2026-11-18。本机为 cgroup v1，平台通过本地 `KUASAR_NO_CGROUP` 补丁运行；所有组件都在 `lzh-sandbox-test` 容器内，不修改宿主机已有的 CubeSandbox。

## 1. 环境概览

| 项 | 值 |
|---|---|
| 主机 | cube90 / `192.168.25.90` |
| OS / 架构 | openEuler 24.03 LTS-SP3 / aarch64（Kunpeng 950） |
| CPU / 内存 | 384 vCPU / 2.2 TiB |
| Kernel | `6.6.0-tlbi-rcu-prefetch` |
| Docker / containerd | 25.0.3 / 1.7.13 |
| cgroup | **v1**（cgroup2 挂载后 controllers 为空） |
| SELinux / firewalld | Disabled / inactive |
| 容器 | `lzh-sandbox-test`（Ubuntu 20.04 + systemd，privileged） |
| 容器镜像 | `lzh-sandbox-test-base:systemd`（由 `cube-sandbox-builder:fix1-jinja2` 提交并加 systemd） |

容器启动方式（重建时使用）：

```bash
docker run -d --name lzh-sandbox-test \
  --privileged --security-opt label=disable \
  --cgroupns=private \
  --device=/dev/kvm --device=/dev/vhost-vsock --device=/dev/vfio/vfio \
  --device=/dev/net/tun --device=/dev/userfaultfd \
  -v /sys/fs/cgroup:/sys/fs/cgroup:rw \
  -v /home/lzh:/home/lzh \
  --tmpfs /run --tmpfs /run/lock \
  --network bridge -p 3100:3000 -p 3443:443 \
  --stop-signal SIGRTMIN+3 --stop-timeout 60 \
  lzh-sandbox-test-base:systemd /sbin/init
```

## 2. 目录与组件

| 路径 | 内容 |
|---|---|
| `/home/lzh/kuasar/{kuasar-sandbox,accelerator,connector,sandboxer,guest-runtime,orchestrator}` | 六个源码仓 |
| `/home/lzh/kuasar/kuasar-sandbox/bin/aarch64/` | 平台全部二进制、`vmlinux`、`sandbox-runtime.bundle`、`cloud-hypervisor` |
| `/home/lzh/kuasar/config/` | `conductor.yaml`、`proxy.yaml`、`tenant.env`、`template-2c2g.env` |
| `/home/lzh/kuasar/demo/` | `store.yaml`、`cache.yaml`、`manifest.yaml`、`run/`（store/cache UDS） |
| `/home/lzh/kuasar/assets/` | `sandbox.yaml`、`root.img`、`overlay.ext4` |
| `/home/lzh/images/registry` + `/home/lzh/registry.yml` + `/home/lzh/registry-data` | 本地 OCI registry |
| `/opt/sandbox/{kernel,runtime,overlay-templates}` | conductor 使用的 kernel/runtime/overlay 模板 |

源码版本：`kuasar-sandbox 9c0f1cc`、`accelerator 0fd72e3`、`connector 909dc15`、`sandboxer db3d6e3`、`guest-runtime fb32783`、`orchestrator 5185dfd`。

## 3. 服务与启动

容器内由 systemd 管理 6 个 `kuasar-*` unit：

| Unit | 作用 | 监听/端点 |
|---|---|---|
| `kuasar-registry` | 本地 OCI registry（docker distribution 2.8.3） | `127.0.0.1:5000` |
| `kuasar-store` | Manifest Store（FS 后端，generation G1） | `unix:///home/lzh/kuasar/demo/run/store.sock` |
| `kuasar-cache` | tiered cache（RocksDB L1） | `unix:///home/lzh/kuasar/demo/run/cache.sock` |
| `kuasar-vswitch` | eBPF/TC 虚拟交换机 `sw0` | netns `sandbox_switch`；mgmt `kdm00` = `169.254.1.0/31` |
| `kuasar-conductor` | 控制面 / E2B API | `:3000` |
| `kuasar-proxy` | 数据面入口 | `:443`（宿主映射 3443） |

常用命令：

```bash
docker exec lzh-sandbox-test systemctl status kuasar-conductor --no-pager
docker exec lzh-sandbox-test systemctl restart kuasar-conductor kuasar-proxy
docker exec lzh-sandbox-test systemctl is-active kuasar-registry kuasar-store kuasar-cache kuasar-vswitch kuasar-conductor kuasar-proxy
docker exec lzh-sandbox-test curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:3000/health
```

关键配置 `config/conductor.yaml`：

```yaml
api: { domain: 'sandbox.local', listen: ':3000' }
proxy: { auth: enforce }
encryption_key: "<redacted>"
manifest_config: '/home/lzh/kuasar/demo/manifest.yaml'
paths: { run_root: '/run/sandbox', base_root: '/var/lib/sandbox', config_socket: '/run/sandbox/node-ctl.socket' }
sandbox:
  timeout_sec: 300
  resources:
    capacity: { cpu: 2, memory: 2GiB }
    allocatable: { cpu: 2, memory: 2GiB }
    startup: { memory: 512MiB }
  network:
    switch: 'sw0'
  boot:
    kernel: '/opt/sandbox/kernel/6.1/vmlinux'
    runtime: '/opt/sandbox/runtime/v1/sandbox-runtime.bundle'
    overlay_diff_template: '/opt/sandbox/overlay-templates/basic-1G.ext4'
builder:
  insecure_registry: true
  diff_template: '/home/lzh/kuasar/assets/overlay.ext4'
  pull_timeout_sec: 1800
  step_timeout_sec: 300
  ready_timeout_sec: 120
  total_timeout_sec: 2400
checkpoint: { mode: local }
```

## 4. cgroup v1 适配（`KUASAR_NO_CGROUP`）

上游平台路径硬依赖 cgroup v2。本环境使用本地补丁 `KUASAR_NO_CGROUP=1`：

- 跳过 runner/builder 的 `prepareRunnerCgroup` / `prepareBuilderCgroup`；
- 生成的 `sandbox-runner@.service` / `sandbox-builder@.service` 不含 `Delegate=yes`，`TimeoutStopSec=5`；
- pause 清理不再等 unit 进入 dead 状态：对 runner `SIGKILL`，轮询 `TasksCurrent==0` 后再做 checkpoint cleanup（避免 cgroup v1 上 delegated cgroup 释放卡满超时）；
- `sandboxcfg` 去掉 `overhead`/`watermark_high`，`allocatable` 固定等于 `capacity`。

涉及文件（本地未提交）：

```text
orchestrator/internal/nocgroup/nocgroup.go
orchestrator/cmd/node-ctl/{run_sandbox.go,taskrun.go,run_builder.go}
orchestrator/internal/sandboxproc/start.go
orchestrator/internal/sandboxcfg/resource.go
orchestrator/internal/orch/{units.go,sandbox_finalizer.go}
orchestrator/internal/launcher/launcher.go
```

生效方式：

```bash
# unit drop-in（构建 runner/builder 时继承）
ls /etc/systemd/system/sandbox-runner@.service.d/10-no-cgroup.conf
# conductor 进程本身
systemctl show kuasar-conductor -p Environment
```

限制：无 per-sandbox CPU/内存硬隔离、无 `memory.high` Budget/OOM deflate/usage 计量、不能用 dynamic 资源模式。单沙箱与并发功能不受影响。

## 5. 快速操作：创建 / 执行 / pause / resume

```bash
docker exec -it lzh-sandbox-test bash
source /home/lzh/kuasar/config/tenant.env        # AK / MK / API_SECRET
source /home/lzh/kuasar/config/template-2c2g.env # TID（canonical template）
export PATH=/home/lzh/kuasar/kuasar-sandbox/bin/aarch64:$PATH
API=http://127.0.0.1:3000
```

### 5.1 创建

```bash
curl -s -X POST -H "X-API-KEY: $AK" -H 'Content-Type: application/json' \
  -d "{\"templateID\":\"$TID\",\"timeout\":600}" "$API/sandboxes" | jq
SID=<返回的 sandboxID>
```

### 5.2 在 guest 内执行

```bash
sandbox-ctl exec --sandbox-id "$SID" --run-root /run/sandbox/sandboxes -- \
  /bin/sh -c 'python3 -V; echo hello-from-guest'
```

远程 SDK 路径：`POST /sandboxes/{id}/exec-sessions`（签发 ExecAccessToken）或 envd 的 `49983`（`commands.run` / files），由 Proxy 转发；本地路径就是上面的 `sandbox-ctl exec`。

### 5.3 pause / resume

```bash
# memory snapshot（默认）
curl -s -o /dev/null -w 'pause=%{http_code}\n' -X POST \
  -H "X-API-KEY: $AK" -H 'Content-Type: application/json' \
  -d '{"memory":true}' "$API/sandboxes/$SID/pause"

# 仅磁盘
curl -s -o /dev/null -w 'pause-disk=%{http_code}\n' -X POST \
  -H "X-API-KEY: $AK" -H 'Content-Type: application/json' \
  -d '{"memory":false}' "$API/sandboxes/$SID/pause"

# resume（connect）
curl -s -X POST -H "X-API-KEY: $AK" -H 'Content-Type: application/json' \
  -d '{"timeout":600}' "$API/sandboxes/$SID/connect" | jq .state

# 删除
curl -s -o /dev/null -X DELETE -H "X-API-KEY: $AK" "$API/sandboxes/$SID"
```

### 5.4 租户凭据

首次或重建 conductor 后：

```bash
MK=$(e2b-key-ctl gen-key)
node-ctl manifest-key add --socket /run/sandbox/node-ctl.socket "$MK"
API_SECRET=$(e2b-key-ctl derive-api-secret "$MK")
AK=$(e2b-key-ctl gen-apikey "$API_SECRET")
```

把 `MK/API_SECRET/AK` 写入 `config/tenant.env`。

## 6. 构建模板（image → template）

```bash
source /home/lzh/kuasar/config/tenant.env
API=http://127.0.0.1:3000
REG='169.254.169.254:5000/library/python@sha256:c61cffc3bacb94ed026ff8b45d5114b7693dd0420baca0a1ace0e3d0ce9035fd'

reg=$(curl -s -X POST -H "X-API-KEY: $AK" -H 'Content-Type: application/json' \
  -d '{"name":"py2c2g","tags":["py2c2g"],"cpuCount":2,"memoryMB":2048}' \
  "$API/v3/templates")
tid=$(echo "$reg" | jq -r .templateID); bid=$(echo "$reg" | jq -r .buildID)

body=$(jq -n --arg img "$REG" \
  '{fromImage:$img, startCmd:"sleep 3600",
    steps:[{type:"RUN", args:["useradd -m -u 1000 user"]}]}')
curl -s -X POST -H "X-API-KEY: $AK" -H 'Content-Type: application/json' \
  -d "$body" "$API/v2/templates/$tid/builds/$bid"

# 轮询到 ready；canonical templateID 取 status.templateID
curl -s -H "X-API-KEY: $AK" "$API/templates/$tid/builds/$bid/status" | jq
```

要点：

- 模板容量取 conductor `sandbox.resources.capacity`，不是注册时的 `memoryMB`；要 2C2G 就把 conductor capacity 设为 2GiB 再构建。
- `python:3.12-slim` 没有 E2B 要求的 `user`（1000:1000），必须加 `useradd` RUN step。
- guest 无外网：构建沙箱通过 vswitch mgmt-service 访问 `169.254.169.254:5000`（映射到本地 registry）。

## 7. Benchmark

仓库新增用例（本地未提交）：

```text
kuasar-sandbox/test/perf/e2b_stage_bench.py    # create / pause-resume / analyze
kuasar-sandbox/test/perf/e2b-stage-bench.sh    # 入口脚本
kuasar-sandbox/Makefile                        # perf-e2b target
```

```bash
cd /home/lzh/kuasar/kuasar-sandbox
source /home/lzh/kuasar/config/tenant.env
source /home/lzh/kuasar/config/template-2c2g.env
export E2B_API_URL=http://127.0.0.1:3000 E2B_API_KEY="$AK" E2B_TEMPLATE_ID="$TID"

# create
MODE=create CONCURRENCY=10 ROUNDS=5 bash test/perf/e2b-stage-bench.sh

# create -> pause -> resume（1,5,10 并发，预热本地 checkpoint）
CONCURRENCY=1,5,10 ROUNDS=3 WARM=1 OUT=/tmp/e2b-samples.jsonl \
  bash test/perf/e2b-stage-bench.sh

# 阶段分解
journalctl -u kuasar-conductor --no-pager > /tmp/conductor.log
python3 test/perf/e2b_stage_bench.py analyze --log /tmp/conductor.log \
  --samples /tmp/e2b-samples.jsonl
```

当前 2C2G 基线（`python:3.12-slim`，n=2/3）：

| 场景 | 结果 |
|---|---:|
| create C=2 | wall 385 ms，per 193 ms，req avg 351 ms / p95 419 ms |
| pause-resume C=1 | create 338 ms，pause 200 ms，resume 338 ms |
| pause-resume C=2 | create 344 ms，pause 232 ms，resume 370 ms |
| create 阶段 p50 | runtime_ready 218 ms、artifact prepare 76 ms、prepare 99 ms、runner_wait 65 ms |
| pause capture p50 | 80 ms |
| resume 阶段 p50 | runtime_ready 189 ms |

注意：CubeSandbox benchmark 模板使用 `sandbox-code-envd-ci`（Ubuntu 22.04 + code interpreter + envd，约 379 MiB）；本环境使用仓库官方默认 `python:3.12-slim`（43.5 MB）。两者 workload 不对等，绝对值不可直接对比。

## 8. 从零复现（源码构建）

1. 容器内安装依赖：

```bash
apt-get install -y autoconf automake libtool pkg-config liblz4-dev libzstd-dev \
  uuid-dev libselinux1-dev bison flex bc libelf-dev libssl-dev cpio rsync xz-utils wget \
  iproute2 iptables procps busybox-static golang
```

2. erofs-utils 1.9.1：

```bash
git clone --depth 1 -b v1.9.1 https://github.com/erofs/erofs-utils.git
cd erofs-utils && ./autogen.sh && ./configure --prefix=/usr/local && make -j"$(nproc)" && make install
```

3. Go 工具链（1.26.x，经 goproxy.cn）：

```bash
export GOPROXY=https://goproxy.cn,direct GOSUMDB=sum.golang.org GOTOOLCHAIN=auto
export GOWORK=/home/lzh/kuasar/go.work GOFLAGS=-buildvcs=false
```

4. go.work 必须用**版本级 replace**（`use` 单独不够）：

```bash
cd /home/lzh/kuasar
go work init ./accelerator ./connector ./sandboxer ./guest-runtime ./orchestrator
go work edit -replace github.com/kuasar-sandbox/accelerator@v0.2.0=./accelerator
go work edit -replace github.com/kuasar-sandbox/connector@v0.2.0=./connector
go work edit -replace github.com/kuasar-sandbox/guest-runtime@v0.2.0=./guest-runtime
go work edit -replace github.com/kuasar-sandbox/sandboxer@v0.2.0=./sandboxer
go work edit -replace github.com/kuasar-sandbox/orchestrator@v0.2.0=./orchestrator
```

5. 内核源码走阿里云镜像（kernel.org 约 20 KB/s）：

```bash
mkdir -p guest-runtime/native-deps/build/tarball
curl -sSL -o guest-runtime/native-deps/build/tarball/linux-6.1.169.tar.gz \
  https://mirrors.aliyun.com/linux-kernel/v6.x/linux-6.1.169.tar.gz
```

6. 构建：

```bash
cd /home/lzh/kuasar/guest-runtime/native-deps && make build
cd /home/lzh/kuasar/guest-runtime && make sandbox-init flatten-ctl sandbox-runtime \
  BUILD_MKFS_EROFS=/usr/local/bin/mkfs.erofs
cd /home/lzh/kuasar/sandboxer/native-deps && make cloud-hypervisor
cd /home/lzh/kuasar && make -C kuasar-sandbox build
```

Cloud Hypervisor 可能出现 “build records are missing”，二进制仍会产出，可忽略记录校验。

## 9. 故障排查

| 现象 | 处理 |
|---|---|
| `kuasar-registry` 反复重启：`bind: address already in use` | 旧 `registry serve` 占用 5000：`ss -lntp \| grep :5000`，kill 旧 PID，`systemctl restart kuasar-registry` |
| `kuasar-cache` crash-loop：`RocksDB LOCK: Resource temporarily unavailable` | 旧 `cache-ctl` 残留：`pkill -x cache-ctl`，`systemctl restart kuasar-cache` |
| pause 固定 20 s | cgroup v1 + `Delegate=yes` + `TimeoutStopSec=20`；确认 unit 无 `Delegate`、`TimeoutStopSec=5`，且 pause 清理走 SIGKILL + `TasksCurrent` 等待 |
| 构建卡在 `manifest fetcher` | cache/store 未就绪（常见于 cache LOCK 冲突）；先修 cache，再重试 |
| `netns sandbox_switch: file does not exist` | `ip netns add sandbox_switch`，再重启 `kuasar-vswitch` |
| `kdm00` 不存在/无地址 | 等 vswitch 启动后执行 `vswitch-post.sh`：`ip link set kdm00 up`、`ip addr replace 169.254.1.0/31 dev kdm00`、`route_localnet=1` |
| 镜像架构不匹配 `linux/amd64` | 用 `crane copy --platform linux/arm64`，并 `crane config` 确认 `.architecture=arm64` |
| build 报 `invalid username: 'user'` | 模板镜像缺 E2B `user`；加 `useradd -m -u 1000 user` RUN step |
| GitHub 不通 | 使用 `gh-proxy.com`；Go 走 goproxy.cn，cargo 走 rsproxy.cn，内核走阿里云镜像 |

## 10. 已知限制与后续

- restore 的 `runtime_ready` 约 170–190 ms（Manifest + UFFD 逐页取），是 create/resume 的主要剩余开销；
- `artifact prepare` ~76 ms、`prepare` ~99 ms、runner 启动 ~65 ms 也有优化空间；
- 官方 perf harness 默认 1 vCPU / 512 MiB，且没有 API 层 pause/resume 专项；当前数据来自自定义用例；
- 与 CubeSandbox 的镜像/workload 尚未对齐，跨平台数字只能作趋势参考；
- 本环境是 privileged 容器 + cgroup v1，仅用于测试，不应暴露给不可信负载。

## 11. 速查

```bash
# 服务
docker exec lzh-sandbox-test systemctl restart kuasar-conductor kuasar-proxy
docker exec lzh-sandbox-test curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:3000/health

# 创建/执行/暂停/恢复/删除
source /home/lzh/kuasar/config/tenant.env; source /home/lzh/kuasar/config/template-2c2g.env
API=http://127.0.0.1:3000
SID=$(curl -s -X POST -H "X-API-KEY: $AK" -H 'Content-Type: application/json' \
  -d "{\"templateID\":\"$TID\",\"timeout\":600}" $API/sandboxes | jq -r .sandboxID)
sandbox-ctl exec --sandbox-id "$SID" --run-root /run/sandbox/sandboxes -- /bin/sh -c 'python3 -V'
curl -s -o /dev/null -X POST -H "X-API-KEY: $AK" -d '{"memory":true}' $API/sandboxes/$SID/pause
curl -s -o /dev/null -X POST -H "X-API-KEY: $AK" -d '{"timeout":600}' $API/sandboxes/$SID/connect
curl -s -o /dev/null -X DELETE -H "X-API-KEY: $AK" $API/sandboxes/$SID

# benchmark
cd /home/lzh/kuasar/kuasar-sandbox
E2B_API_KEY=$AK E2B_TEMPLATE_ID=$TID CONCURRENCY=1,5,10 ROUNDS=3 WARM=1 \
  bash test/perf/e2b-stage-bench.sh
```
