// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"math"
	"time"
)

func Backoff(attempt int, max time.Duration) time.Duration {
	if attempt == 0 {
		return 0
	}
	backoff := float64(500*time.Millisecond) * math.Pow(2, float64(attempt))
	d := time.Duration(backoff)
	if d > max {
		d = max
	}
	return d
}
