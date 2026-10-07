# ratelimit — 服务入口限流与配额控制

一套可在本地复现的多算法限流 + 多实例一致配额库。支持令牌桶、漏桶、滑动窗口三种算法，多策略混合与优先级，多实例共享额度（CAS 无锁），调用方/服务两级隔离，策略热更新，降级，单调时钟计量与落盘恢复。

## 快速开始

```go
store := ratelimit.NewMemoryStore()          // 多实例共享同一个 Store 即共享额度
clock := ratelimit.NewRealClock()
mgr, err := ratelimit.NewPolicyManager(store, clock, []ratelimit.Policy{
    {
        ID: "per-caller",
        Scope: ratelimit.ScopeCaller,        // 调用方级隔离
        CallerID: "*", ServiceID: "*",
        Key: "api.*",                        // 资源键，支持 "*" 与 "prefix.*"
        Algorithm: ratelimit.AlgTokenBucket,
        Rate:      10,                      // 每秒补充 10 个令牌
        Burst:     20,                      // 突发上限 20
        Priority:  10,                       // 数值越大优先级越高
        Degrade:   ratelimit.DegradeReject, // 共享计数不可用时拒绝
    },
})

dec, err := mgr.Check(ctx, ratelimit.Request{
    CallerID:  "caller-1",
    ServiceID: "svc-a",
    Key:       "api.users",
})
if !dec.Allowed {
    // dec.Reason: "rate_limited" / "degraded_reject" / "degraded_allow"
    // dec.PolicyID: 命中的策略 ID
    // dec.Version:  判定所依据的策略版本
}
```

## 算法语义

所有算法都基于**单调时钟**计量（`Clock.Mono()`），状态持久化时携带计量时间。

### 令牌桶（AlgTokenBucket）

- 桶初始为满（`Burst` 个令牌），按 `Rate`（个/秒）匀速补充，补充上限为 `Burst`。
- 每次放行消耗 1 个令牌；桶空则拒绝。
- **突发行**：`Burst` 个瞬时请求可全部放行；长时间空闲后桶也不会超过 `Burst`。
- 时钟回拨（`now < last`）时把计量基准重定到当前时钟、不动余额：既不会凭空多出令牌，也不会长期拒绝（时钟继续前进即恢复补充）。

### 漏桶（AlgLeakyBucket）

- 桶容量 `Burst`，按 `Rate`（个/秒）匀速漏出。
- 请求加入 1 个单位；桶满（加入后超过容量）则拒绝（溢出）。
- 输出平滑：突发最多排队 `Burst` 个，稳态放行速率不超过 `Rate`。
- 时钟回拨处理与令牌桶一致。

### 滑动窗口（AlgSlidingWindow）

- 窗口跨度 `Window`，窗口内放行上限 = `min(Burst, ceil(Rate × Window秒数))`，且至少为 1。即：突发不超过 `Burst`，稳态吞吐不超过 `Rate` 个/秒；调小 `Rate` 会立即收紧窗口上限（热更新生效）。
- 窗口被等分为 10 个固定子桶；早于窗口边界（`计量时刻 - 子桶起点 >= Window`）的子桶整桶过期，因此边界是连续滑动的，不存在固定窗口整点突增。
- **边界行为确定**：计量时刻 `t`，子桶起点 `b` 满足 `t - b >= Window` 即过期；窗口内计数之和小于上限才放行。
- **计量时钟**：判定不使用进程内单调时钟的原始读数，而是使用随状态一起持久化的虚拟计量时钟（meter）。meter 在同一进程生命周期内随单调时钟前进（不受系统时间调整影响）；当检测到进程重启（`BootID` 变化）或单调时钟回拨时，按**真实物理时间**（墙钟差值）前进。因此：
  - 进程重启后，重启前计入的用量按真实经过的时间自然老化，停机超过 `Window` 后历史用量全部释放，不会长期占用额度；
  - 系统时间被回拨（墙钟差值为负）时老化量钳为 0：不凭空多出额度，也不会永久锁死——新进程自身运行时间累计超过 `Window` 后用量正常过期；
  - 多实例共享同一 `Store` 时，各实例的单调时钟本不可比，meter 机制同样保证跨实例按真实时间老化。
- 旧版本持久化的窗口状态（无计量锚点字段）无需迁移：首次判定时把 meter 重定到最新子桶，历史用量保留并在一个窗口跨度内自然过期，不会永久锁死。

## 配置项（Policy）

| 字段 | 说明 |
| --- | --- |
| `ID` | 策略标识，热更新时保持稳定；判定结果通过它区分命中的策略 |
| `Scope` | `ScopeCaller`（按调用方隔离）或 `ScopeService`（按服务共享） |
| `CallerID` | 匹配调用方，`"*"` 匹配所有调用方 |
| `ServiceID` | 匹配服务，`"*"` 匹配所有服务 |
| `Key` | 资源键：`"*"` 全匹配，`"prefix.*"` 前缀匹配，否则精确匹配 |
| `Algorithm` | `AlgTokenBucket` / `AlgLeakyBucket` / `AlgSlidingWindow` |
| `Rate` | 补充/漏出速率，单位：个/秒（必须 > 0）；滑动窗口中与 `Burst` 共同决定窗口上限 `min(Burst, ceil(Rate×Window))` |
| `Burst` | 令牌桶容量 / 漏桶队列容量 / 滑动窗口突发上限（必须 > 0） |
| `Window` | 滑动窗口跨度（仅滑动窗口，必须 > 0 且 ≥ 10ms） |
| `Priority` | 优先级，数值越大越高；相同时按 `ID` 字典序，与声明顺序无关 |
| `Degrade` | `DegradeReject`（失败关闭）或 `DegradeAllow`（失败放行） |

## 多策略混合与优先级

- 一次请求可能同时命中多条策略（如调用方级 + 服务级）。**所有命中策略都放行，最终才放行**；任一拒绝即拒绝。
- 命中策略按 `Priority` 降序、`ID` 升序排序后依次评估，第一个拒绝的策略决定结果。因此拒绝原因稳定，**不受规则遍历顺序影响**。
- 判定结果 `Decision` 携带 `PolicyID`、`Algorithm`、`Version`、`Reason`，可区分：
  - `allowed`：放行
  - `rate_limited`：被某策略限流拒绝
  - `degraded_allow`：共享计数不可用，按失败放行策略放行
  - `degraded_reject`：共享计数不可用，按失败关闭策略拒绝

## 多实例共享额度（并发不超发）

- 多个实例共享同一个 `Store` 即共享同一份额度。
- 每次判定都是一次 **CAS（Compare-And-Swap）** 循环：`Get` 读取某个**已提交的完整计数** → 计算新状态 → `CAS` 提交；冲突则基于最新已提交计数重试。
- 因此并发争抢下**累计放行量绝不超过额度**（有 `-race` 测试覆盖），重试不会超发。
- 全程无锁：超时或取消不会把计数停在中间状态，也不会把额度永久锁死（超时后计数为空，后续调用正常）。

## 作用域隔离

- 计数器键 = `(作用域, 策略ID, 调用方ID 或 服务ID, 资源键)`。
- 不同调用方的相同资源键各自独立计数，互不串扰。
- 单个资源键的突发不会挤占其他资源键的额度（计数器按 `策略+身份+资源键` 独立）。
- 服务级策略按 `ServiceID` 共享，跨调用方累计。

## 运行期热更新

- `Update(policies)` 先完整校验（参数合法性 + `ID` 唯一），通过后才原子替换快照；**校验失败时旧快照继续服务**。
- 每次判定在开始时**固定一个快照版本**（只加载一次），进行中的判定只可能落到某一个完整版本，不会读到新旧混合的策略。
- `Decision.Version` 标识判定所用版本。

## 降级

- 当 `Store` 不可用（`Get`/`CAS` 报错、超时或取消）时，按策略的 `Degrade` 选择放行或拒绝，原因分别为 `degraded_allow` / `degraded_reject`，与正常限流原因可区分。
- 存储恢复后自动回到正常判定。
- 超时/取消不写中间状态、不锁额度（有测试覆盖）。

## 时钟与持久化

- 计量使用单调时钟（`Clock.Mono()`），系统时间被调整/回拨时：
  - 不会凭空多出额度（回拨时重定计量基准，余额不动，补充受 `Burst` 上限约束）；
  - 不会让限流失效或长期拒绝（时钟继续前进即恢复）。
- 计数通过 `FileStore` 落盘：每次提交都写入临时文件、`fsync` 后原子 rename，崩溃不会留下半写计数。
- 重启后恢复：令牌桶/漏桶余额、滑动窗口窗口内用量都**不清零**。
  - 令牌桶/漏桶：重启后单调时钟重置表现为一次回拨，基准重定、余额保留，之后正常补充/漏出。
  - 滑动窗口：状态里持久化了虚拟计量时钟（meter）与上次提交的单调/墙钟读数、进程生命周期标识（`BootID`）。重启后 `BootID` 变化，meter 按**真实物理时间**（墙钟差值）前进，停机期间超窗的用量被正确释放；墙钟回拨时老化量钳为 0，不产生额外额度也不永久锁死。自定义 `Clock` 可实现可选接口 `BootClock`（`BootID() int64`）获得该行为；不实现则退化为仅按单调时钟老化（历史行为）。
- 验证方法：`ratelimit/sliding_restart_test.go` 中的测试用 `FileStore` + 更换时钟实例模拟重启，显式断言"墙钟确实推进、单调时钟确实归零、BootID 确实变化"后再验证用量老化与速率约束，杜绝因时间没有变化而侥幸通过。

## Store 扩展

`Store` 接口可替换为 Redis 等共享后端（多进程部署时）：

```go
type Store interface {
    Get(ctx, key) ([]byte, error)
    CAS(ctx, key, oldVal, newVal []byte) (bool, error) // 原子比较并交换
    Snapshot(ctx) (map[string][]byte, error)
    Restore(ctx, map[string][]byte) error
    Close() error
}
```

内置 `MemoryStore`（进程内共享）与 `FileStore`（落盘）。多进程部署时实现该接口即可，判定逻辑无需改动。

## 测试

```bash
go test ./... -race
```

覆盖：并发争抢（所有算法）、CAS 重试收敛、窗口边界、时钟回拨、热更新并发与失败保持、降级与超时不锁额度、作用域隔离、优先级稳定性、重启恢复、滑动窗口重启后按物理时间老化（含部分老化、墙钟回拨、旧格式状态迁移）、滑动窗口速率约束与速率热更新。
