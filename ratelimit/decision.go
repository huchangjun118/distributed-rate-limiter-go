package ratelimit

// Reason is a stable, order-independent identifier of *why* a request was
// rejected (or allowed under degradation). Values are part of the public API.
type Reason string

// Well-known reject reasons.
const (
	ReasonNone             Reason = "" // allowed
	ReasonTokenBucketEmpty Reason = "token_bucket_empty"
	ReasonLeakyBucketFull  Reason = "leaky_bucket_full"
	ReasonWindowExceeded   Reason = "sliding_window_exceeded"
	ReasonCounterUnavail   Reason = "counter_unavailable" // fail-closed deny: store error
	ReasonCounterTimeout   Reason = "counter_timeout"     // fail-closed deny: transaction deadline
	ReasonCounterCanceled  Reason = "counter_canceled"    // fail-closed deny: caller canceled
	ReasonDegradedAllow    Reason = "degraded_allow"      // fail-open allow
	ReasonDegradedTimeout  Reason = "degraded_timeout"    // fail-open allow after timeout
	ReasonDegradedCanceled Reason = "degraded_canceled"   // fail-open allow after cancel
)

// Decision is the result of evaluating one request against the whole
// effective policy set.
type Decision struct {
	Allowed bool
	// RejectedBy lists the policy IDs that denied the request, sorted
	// lexicographically, so the result is independent of rule iteration
	// order. Empty when Allowed (except DegradedAllow accounting).
	RejectedBy []string
	// Reasons maps policy ID -> reason. Sorted output via RejectedBy.
	Reasons map[string]Reason
	// Version is the immutable policy configuration version the decision was
	// made against.
	Version uint64
	// Degraded is true when the shared counter was unavailable.
	Degraded bool
}
