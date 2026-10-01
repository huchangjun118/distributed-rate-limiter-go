# distributed-rate-limiter-go

服务入口的限流与配额控制库：多算法本地限流 + 多实例一致配额 + 调用方/服务两级隔离 +
策略热更新 + 故障降级 + 单调时钟计量 + 快照重启恢复。共享计数默认以**同进程共享
`LocalCounter`** 提供，使多实例争用、超发、降级、重启等行为完全可在本地复现；
`Counter` / `Store` 均为接口，可替换为 Redis、数据库等分布式后端。

## 快速开始

```go
store := ratelimit.NewFileStore("data/snap.json") // 可选；nil 表示不落盘
eng, err := ratelimit.NewEngine(ratelimit.Config{
    Clock: ratelimit.DefaultClock(),
    Store: store,                       // 设置 Store 即启用定时快照 + 启动恢复
    CounterTimeout:  200 * time.Millisecond,
    PersistInterval: 5 * time.Second,
}, ratelimit.PolicyConfig{
    Version: 1,
    Rules: []ratelimit.Rule{
        {ID: "per-caller", Algo: ratelimit.AlgoTokenBucket, Scope: ratelimit.ScopeCaller,
         Key: "/api", Rate: 2, Burst: 3, Fail: ratelimit.FailClosed},
        {ID: "global",     Algo: ratelimit.AlgoSlidingWindow, Scope: ratelimit.ScopeServer,
         Key: "/api", Rate: 100, Window: time.Second, SubBuckets: 10, Fail: ratelimit.FailOpen},
    },
})
d := eng.Allow(ctx, &ratelimit.Request{Caller: "tenant-7", Key: "/api"})
// d.Allowed / d.RejectedBy(稳定排序) / d.Reasons(逐规则原因) / d.Version / d.Degraded
```

运行可执行示例：`go run ./cmd/ratelimit-example`。

## 限流算法与确定性行为（需求 1）

所有算法的状态推进都是给定 `(时钟读数, 参数, 状态)` 的**纯函数**，边界行为与调度无关。

### 令牌桶 `token_bucket`

- 容量 = `Burst`，速率 = `Rate` 令牌/秒，持续补充且上限为 `Burst`（不累积）。
- **冷启动满桶**：新键的第一波可立刻通过 `Burst` 个请求；同一时刻第 `Burst+1` 个必拒。
- 补充量只由**单调时钟**差值决定，差值被钳制为 `>= 0`。
- 拒绝不消耗令牌；补充时间戳仍推进到当前时刻，因此任何时间间隔既不会重复计入也不会漏掉。

### 漏桶 `leaky_bucket`（恒定出水的计量漏桶）

- 容量 = `Burst`（桶内排队数），出水 = `Rate` 请求/秒，持续漏水且不低于 0。
- **冷启动空桶**：最多突发 `Burst` 个通过，其后被整形成严格的 `Rate`/秒。
- 水位 + 请求量 `<= Burst` 才放行；拒绝不抬升水位（只有被放行的流量进水）。

### 滑动窗口 `sliding_window`（固定子桶滑动计数）

- 窗口跨度 `Window`，被等分为 `SubBuckets` 个与 epoch 对齐的子桶（按**墙上时钟**
  对齐，这是窗口状态能跨重启恢复的前提）。
- 对当前时刻 `now`，起始于 `b` 的桶当且仅当 `b <= now < b+Window`（半开区间
  `[b, b+Window)`）时计入；等价地保留 `now-Window < b <= now` 的桶。处在后沿
  `b <= now-Window` 的桶被删除，用量不会提前一个瞬间释放。
- 最旧的未过期桶与后沿有交叠，被**全额**计入——这是对平滑滑动日志的确定、保守
  近似，只会多计不会多放。
- 限额：`Rate` 被解释为“每窗口允许请求数”；`Burst > 0` 时以 `Burst` 覆盖该限额。
- 放行条件：`保留桶计数之和 + n <= 限额`。

边界示例（`Window=10s, limit=3`）：`t=0` 放满 3 个；`t=9.5s` 仍拒绝（`t=0` 桶仍在
半开窗口内）；`t=10s` 恰好放行（旧桶刚掉出后沿）。

## 多策略混合与优先级（需求 2）

- 一个请求可同时命中多条规则；规则是否命中由 `Match Matcher` 决定（`nil` 匹配
  全部）。可用 `MatchKeys(...)` / `MatchCallers(...)` 或自定义 `MatcherFunc`。
- 请求只有在**所有**命中规则都放行时才 `Allowed=true`。
- 判定结果与规则在配置切片中的遍历顺序**无关**：引擎按规则 ID 字典序评估，
  `Decision.RejectedBy` 始终排序，`Decision.Reasons` 给出每条规则各自独立的拒绝原因
  （`token_bucket_empty` / `leaky_bucket_full` / `sliding_window_exceeded` /
  各降级原因）。
- `Priority` 仅作人类可读的优先级标注；是否放行是集合判定，不依赖遍历顺序。
- 已通过规则的计数会保留（**不做全局回滚**）：这如实反映共享计数上已发生的提交，
  也避免跨实例回滚竞争；拒绝方规则清单仍完整返回，调用方可区分“被哪条规则挡下”。

## 多实例一致配额、绝不超发（需求 3）

- 所有实例在同一个 `Counter` 上争用。`Update` 采用乐观事务：

  1. 短临界区取出已提交状态的**深拷贝**；
  2. 不持锁执行确定性算法计算（慢调用不会锁死他人额度）；
  3. 短临界区通过指针同一性检查该键是否被其他事务抢先提交；无冲突则一次性整体
     提交，有冲突则**丢弃本次结果、基于最新已提交状态重试**。

- 每次判定都基于某个**已提交的完整计数**，读—算—提交之间对外不存在中间窗口；
  并发抢占下累计放行量严格 `<= 配额`，重试不会双计。测试以 64 goroutine × 100 请求
  争抢 500 配额、32 goroutine 争抢 10 配额等场景验证，`-race` 下通过。
- 本地复现：多个 `Engine` 传入同一个 `*LocalCounter` 即等价于多个服务实例共享配额
  （见示例与 `TestCounterTwoInstancesShareQuota`）。

## 调用方 / 服务两级作用域隔离（需求 4）

- `Scope=caller`：计数键为 `c:<ruleID>|<caller>|<key>`，按调用方独立；
  `Scope=server`：计数键为 `s:<ruleID>|<key>`，全局限额。
- 两级可分别使用不同的 `Counter` 实现（`Config.Counters.Caller/Server`）；即便共用
  同一物理存储，`c:`/`s:` 前缀与规则 ID 也保证不同调用方、不同规则、不同逻辑键
  互不串扰；单个键的突发不可能挤占其他键的额度（每键独立物理计数）。

## 运行期热更新（需求 5）

- `eng.Update(PolicyConfig)` 先做**全量校验**（算法/作用域/速率/突发/窗口/子桶/
  失败模式/ID 唯一），通过后原子替换不可变的已编译规则指针。
- 进行中的 `Allow` 在入口只 pin 一次版本指针，因此一次判定要么完整落在旧版本、
  要么完整落在新版本，不会读到半成品。
- 更新失败（非法算法、重复 ID、参数非法等）时**上一个可用版本继续服务**，
  `CurrentVersion()` 不变。测试：`TestHotUpdateFailureKeepsOldVersion`、
  `TestConcurrentUpdatesAndAllow`（更新与判定并发 + 持续插入非法更新）。
- 同一规则 ID 跨版本把算法换成另一种时，旧算法状态不会被按新算法误读：该键会以
  新算法的初始状态重新开始（限流配置变更的明确语义，文档化）。

## 共享计数不可用时的降级（需求 6）

- 每条规则独立配置 `Fail: open|closed`（默认 `closed`）。
- `open`：放行，`Decision.Degraded=true`，原因可区分：
  `degraded_allow`（存储错误）/ `degraded_timeout`（超时）/ `degraded_canceled`（取消）。
- `closed`：拒绝，原因为 `counter_unavailable` / `counter_timeout` /
  `counter_canceled`，与正常限流拒绝原因可区分。
- 单次事务受 `CounterTimeout` 约束。超时或调用方取消时：算法计算发生在无锁的私有
  拷贝上，提交是一次加锁赋值，因此取消发生在提交前即**完全不写状态**，不会留下
  半笔计数，也不持有任何跨调用锁，额度不会被永久锁死；故障注入恢复后立即继续服务
  （测试：`TestCounterCancelCommitsNothing`、`TestCounterDegradationRecoversNoLockout`、
  `TestDegradationModes`、`TestDegradedTimeoutReason`）。

## 单调时钟与时间调整（需求 7）

- 令牌桶/漏桶只消费 `Clock.Mono()`（Go 运行时单调钟，基于进程起点固定原点），
  墙上时钟被 NTP 步进、手动修改或回拨都**不会**凭空产生额度，也不会让限流失效或
  长期拒绝（单调钟继续前进即恢复）。测试：`TestTokenBucketWallClockRollbackNoQuota`。
- 滑动窗口桶以墙上纳秒时间戳为键：窗口语义对应绝对时间。
- **重启桥接**：单调钟原点是进程内概念，快照同时记录保存时的 `wall_nanos` 与
  `mono_nanos`；恢复时用“当前墙上时间 − 保存时墙上时间”（钳制 `>= 0`）计算停机
  间隔并平移连续算法的 `LastMono`。墙上时钟回拨 => 间隔 0 => 不会因重启补发令牌；
  停机 3 秒、速率 1/秒 => 恢复后恰好有 3 枚令牌（受桶容量约束）。

## 持久化与重启恢复（需求 7、8）

- 设置 `Config.Store` 后：启动时加载最近一次快照；之后按 `PersistInterval` 定时
  快照，`Close()` 时再尽力落一次，也可随时 `Persist(ctx)`。
- `FileStore` 采用“临时文件 + fsync + rename”原子替换：崩溃后只会看到上一份或
  新一份完整 JSON，不会出现半文件。
- 窗口内已用额度**不清零**：子桶以墙上时间为键，恢复后自然按窗口老化；未过期的
  用量继续生效（测试：`TestRestartWindowUsageNotZeroed`、
  `TestRestartTokenBucketShutdownGapRefill`、`TestRestartWallRollbackFabricatesNoQuota`、
  `TestFileStoreAtomicRoundTrip`）。
- 快照保存失败不影响在线服务：保留上一份好快照，下个周期自动重试。

## 配置项参考

| 字段 | 适用 | 说明 |
| --- | --- | --- |
| `Rules[].ID` | 全部 | 必填，版本内唯一；出现在判定结果中 |
| `Rules[].Algo` | 全部 | `token_bucket` / `leaky_bucket` / `sliding_window` |
| `Rules[].Scope` | 全部 | `caller`（默认）/ `server` |
| `Rules[].Key` | 全部 | 计数维度上的逻辑资源键（必填） |
| `Rules[].Match` | 全部 | 请求匹配器，`nil` 表示命中全部请求 |
| `Rules[].Rate` | 全部 | tb：令牌/秒；lb：漏水请求/秒；窗口：每窗口请求数（须 > 0） |
| `Rules[].Burst` | tb/lb 必填 | 桶容量，须 > 0；窗口算法中 > 0 时覆盖 Rate 限额 |
| `Rules[].Window` | 窗口必填 | 窗口跨度，须 > 0 |
| `Rules[].SubBuckets` | 窗口 | 子桶数，默认 10；决定边界分辨率 |
| `Rules[].Fail` | 全部 | `closed`（默认）/ `open` |
| `Rules[].Priority` | 全部 | 仅标注用，不改变放行集合 |
| `Config.Counters` | 全部 | 调用方/服务两级计数后端 |
| `Config.Clock` | 全部 | 默认 `DefaultClock()`；测试可用 `NewManualClock` |
| `Config.Store` | 可选 | 快照存储；非空即启用恢复与定时持久化 |
| `Config.CounterTimeout` | 全部 | 单次计数事务超时，默认 200ms |
| `Config.PersistInterval` | 可选 | 快照间隔，默认 5s |
| `PolicyConfig.Version` | 全部 | 单调版本号，会原样出现在每条 `Decision` 中 |

## 测试

```bash
go test ./...            # 全部用例
go test -race ./...      # 并发争抢 / 热更新竞态
```

覆盖：并发争抢不超发、窗口边界（半开区间与保守旧桶计数）、墙上时钟回拨、
热更新失败回滚与并发更新、fail-open/fail-closed 及超时取消、快照原子性、
重启后窗口用量保留与停机补额。
