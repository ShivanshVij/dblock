// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"context"
	"time"
)

func Wait(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}
