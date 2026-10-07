// Package ratelimit provides service-entry rate limiting and quota control.
//
// It implements three metering algorithms (token bucket, leaky bucket and
// sliding window) behind a compare-and-swap shared counter store, so multiple
// service instances can share one quota without over-issuing under contention.
// Policies are hot-updated atomically; decisions pin one complete policy
// version. When the shared store is unavailable, each policy can fail open or
// fail closed with a distinguishable decision reason. Metering uses a
// monotonic clock together with a per-process lineage marker and a wall clock;
// counts persist to disk and recover after a restart without clearing in-use
// quota. The sliding window ages its persisted usage by the real physical
// elapsed time across a restart (usage older than the window is released on
// the first post-restart decision), is bounded by min(Burst, ceil(Rate*Window))
// and neither creates quota nor locks up when the system clock is adjusted.
package ratelimit
