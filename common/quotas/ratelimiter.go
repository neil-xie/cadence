// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package quotas

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"github.com/uber/cadence/common/clock"
)

const (
	_defaultRPSTTL = 60 * time.Second
	_burstSize     = 1
)

// TODO: this is likely major overkill, especially now that clock.Ratelimiter exists.
// Just mutate the limit, it's concurrency-safe and efficient.  And consider a background
// debouncer only if that proves too costly (stevenl: I suspect it will not be).

// RateLimiter is a wrapper around the golang rate limiter handling dynamic
// configuration updates of the max dispatch per second. This has comparable
// performance to the token bucket rate limiter.
// BenchmarkSimpleRateLimiter-4   	10000000	       114 ns/op (tokenbucket)
// BenchmarkRateLimiter-4         	10000000	       148 ns/op (this)
type RateLimiter struct {
	sync.RWMutex
	maxDispatchPerSecond *float64
	goRateLimiter        atomic.Value
	// TTL is used to determine whether to update the limit. Until TTL, pick
	// lower(existing TTL, input TTL). After TTL, pick input TTL if different from existing TTL
	ttlTimer *time.Timer
	ttl      time.Duration
	minBurst int
}

// NewSimpleRateLimiter returns a new rate limiter backed by the golang rate
// limiter.  This is currently only used in tests.
func NewSimpleRateLimiter(t testing.TB, rps int) *RateLimiter {
	t.Helper() // ensure a T has been passed
	initialRps := float64(rps)
	return NewRateLimiter(&initialRps, _defaultRPSTTL, _burstSize)
}

// NewRateLimiter returns a new rate limiter that can handle dynamic
// configuration updates
func NewRateLimiter(maxDispatchPerSecond *float64, ttl time.Duration, minBurst int) *RateLimiter {
	rl := &RateLimiter{
		maxDispatchPerSecond: maxDispatchPerSecond,
		ttl:                  ttl,
		ttlTimer:             time.NewTimer(ttl),
		minBurst:             minBurst,
	}
	rl.storeLimiter(maxDispatchPerSecond)
	return rl
}

// UpdateMaxDispatch updates the max dispatch rate of the rate limiter
func (rl *RateLimiter) UpdateMaxDispatch(maxDispatchPerSecond *float64) {
	if rl.shouldUpdate(maxDispatchPerSecond) {
		rl.Lock()
		rl.maxDispatchPerSecond = maxDispatchPerSecond
		rl.storeLimiter(maxDispatchPerSecond)
		rl.Unlock()
	}
}

// Wait waits up till deadline for a rate limit token
func (rl *RateLimiter) Wait(ctx context.Context) error {
	limiter := rl.goRateLimiter.Load().(clock.Ratelimiter)
	return limiter.Wait(ctx)
}

// Reserve reserves a rate limit token
func (rl *RateLimiter) Reserve() clock.Reservation {
	limiter := rl.goRateLimiter.Load().(clock.Ratelimiter)
	return limiter.Reserve()
}

// Allow immediately returns with true or false indicating if a rate limit
// token is available or not
func (rl *RateLimiter) Allow() bool {
	limiter := rl.goRateLimiter.Load().(clock.Ratelimiter)
	return limiter.Allow()
}

// Limit returns the current rate per second limit for this ratelimiter
func (rl *RateLimiter) Limit() rate.Limit {
	rl.RLock()
	defer rl.RUnlock()
	if rl.maxDispatchPerSecond != nil {
		return rate.Limit(*rl.maxDispatchPerSecond)
	}
	return rate.Inf
}

func (rl *RateLimiter) storeLimiter(maxDispatchPerSecond *float64) {
	burst := int(*maxDispatchPerSecond)
	// If throttling is zero, burst also has to be 0
	if *maxDispatchPerSecond != 0 && burst <= rl.minBurst {
		burst = rl.minBurst
	}
	limiter := clock.NewRatelimiter(rate.Limit(*maxDispatchPerSecond), burst)
	rl.goRateLimiter.Store(limiter)
}

func (rl *RateLimiter) shouldUpdate(maxDispatchPerSecond *float64) bool {
	if maxDispatchPerSecond == nil {
		return false
	}
	select {
	case <-rl.ttlTimer.C:
		rl.ttlTimer.Reset(rl.ttl)
		rl.RLock()
		defer rl.RUnlock()
		return *maxDispatchPerSecond != *rl.maxDispatchPerSecond
	default:
		rl.RLock()
		defer rl.RUnlock()
		return *maxDispatchPerSecond < *rl.maxDispatchPerSecond
	}
}
