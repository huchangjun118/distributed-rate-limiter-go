// Package ratelimit provides service-entry rate limiting and quota control.
//
// It implements three metering algorithms (token bucket, leaky bucket and
// sliding window) behind a compare-and-swap shared counter store, so multiple
// service instances can share one quota without over-issuing under contention.
// Policies are hot-updated atomically; decisions pin one complete policy
// version. When the shared store is unavailable, each policy can fail open or
// fail closed with a distinguishable decision reason. Metering uses a
// monotonic clock; counts persist to disk and recover after a restart without
// clearing in-use quota. The sliding window additionally caps per-window
// throughput at min(Burst, ceil(Rate*Window)) and ages recorded usage by real
// physical time across process restarts via a persisted metering clock, so
// expired usage is released after downtime and clock rollbacks never create
// quota.
package ratelimit
