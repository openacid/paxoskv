// Copyright 2018 Huan Du. All rights reserved.
// Licensed under the MIT license that can be found in the LICENSE file.

// Package g exposes goroutine struct g to user space.
package goid

import (
	"fmt"
	"strconv"
	"time"
	"unsafe"

	"github.com/patrickmn/go-cache"
)

var defID string = "0x0000000000"

var idCache *cache.Cache
var expTime = 120 * time.Minute
var cleanTime = 1 * time.Minute
var cleanExpTime = 5 * time.Minute

func init() {
	idCache = cache.New(expTime, cleanTime)
}

func getg() unsafe.Pointer

// G returns current g (the goroutine struct) to user space.
func G() unsafe.Pointer {
	return getg()
}

func gP() (s string) {
	defer func() {
		if p := recover(); p != nil {
			s = defID
		}
	}()

	g := G()

	if g == nil {
		s = defID
	} else {
		s = fmt.Sprintf("%p", g)
	}

	return s
}

// ID returns goroutine g struct mem address with seq number as the id
// note it's not the only one to guarantee that
func ID() (s string) {
	id := gP()

	if v, ok := idCache.Get(id); ok {
		return id + strconv.FormatInt(v.(int64), 10)
	}

	return id + strconv.FormatInt(0, 10)
}

// SetID set goroutine id
func SetID() (s string) {
	id := gP()

	// try to incr 1, may be error when id not exist
	if v, err := idCache.IncrementInt64(id, int64(1)); err == nil {
		idCache.Set(id, v, expTime)
		return id + strconv.FormatInt(v, 10)
	}

	// add id in cache, when it not exist, or it just deleted
	if err := idCache.Add(id, int64(0), expTime); err == nil {
		return id + strconv.FormatInt(0, 10)
	}

	//maybe someone else has already added it, so incr it again
	if v, err := idCache.IncrementInt64(id, int64(1)); err == nil {
		idCache.Set(id, v, expTime)
		return id + strconv.FormatInt(v, 10)
	}

	return id
}

// ExpireID expire goroutine id
func ExpireID() {
	id := gP()

	v, ok := idCache.Get(id)
	if !ok {
		return
	}

	idCache.Set(id, v, cleanExpTime)
}
