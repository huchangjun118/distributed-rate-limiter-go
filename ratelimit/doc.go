// Package ratelimit provides service-entry rate limiting and quota control.
//
// It implements three metering algorithms (token bucket, leaky bucket and
// sliding window) behind a compare-and-swap shared counter store, so multiple
// service instances can share one quota without over-issuing under contention.
// Policies are hot-updated atomically; decisions pin one complete policy
// version. When the shared store is unavailable, each policy can fail open or
// fail closed with a distinguishable decision reason. Metering uses a
// monotonic clock; counts persist to disk and recover after a restart without
// clearing in-use quota.
package ratelimit
