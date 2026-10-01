package ratelimit

import "time"

// Algorithm selects a limiter algorithm.
type Algorithm string

// Supported algorithms.
const (
	AlgoTokenBucket   Algorithm = "token_bucket"
	AlgoLeakyBucket   Algorithm = "leaky_bucket"
	AlgoSlidingWindow Algorithm = "sliding_window"
)

// FailMode decides what happens when the shared counter is unavailable.
type FailMode string

// Degradation modes.
const (
	FailOpen   FailMode = "open"   // allow when counter unavailable
	FailClosed FailMode = "closed" // deny when counter unavailable
)

// Scope of a policy.
type Scope string

const (
	ScopeCaller Scope = "caller" // per (caller,key) counters
	ScopeServer Scope = "server" // global per key counters
)

// Rule is one rate-limit policy entry.
type Rule struct {
	ID         string // unique, used in decision output and map keys
	Algo       Algorithm
	Scope      Scope
	Key        string        // logical resource key (e.g. route)
	Rate       float64       // tokens/s refill or leak/s drain; req/window for window
	Burst      int64         // bucket capacity / max in-flight
	Window     time.Duration // sliding-window span (window algo only)
	SubBuckets int           // sliding-window sub-bucket resolution (window algo)
	Fail       FailMode
	Priority   int     // tie-break only; never affects allow/deny set
	Match      Matcher // request selector; nil matches everything
}

// Matcher decides whether a request hits a rule.
type Matcher interface {
	Match(req *Request) bool
}

// Request is the evaluated object.
type Request struct {
	Caller string
	Key    string
	Tokens int64 // weight of the request, defaults to 1 when 0
}

// --- Matchers -------------------------------------------------------------

// MatcherFunc adapts a function to Matcher.
type MatcherFunc func(req *Request) bool

// Match implements Matcher.
func (f MatcherFunc) Match(req *Request) bool { return f(req) }

// MatchKeys matches requests whose Key is in the set.
func MatchKeys(keys ...string) Matcher {
	set := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		set[k] = struct{}{}
	}
	return MatcherFunc(func(req *Request) bool {
		_, ok := set[req.Key]
		return ok
	})
}

// MatchCallers matches requests whose Caller is in the set.
func MatchCallers(callers ...string) Matcher {
	set := make(map[string]struct{}, len(callers))
	for _, c := range callers {
		set[c] = struct{}{}
	}
	return MatcherFunc(func(req *Request) bool {
		_, ok := set[req.Caller]
		return ok
	})
}
