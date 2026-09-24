package ccache

import "time"

// timeNow is the single clock behind every expiry decision in the cache:
// TTL stamping on write (bucket.set/setnx/setnx2), Item.Expired, Item.TTL
// and Item.Extend all go through it. It is a package-level variable so that
// tests can install a fake clock and still exercise the exact production
// expiration path rather than a test-only branch. Production code always
// uses time.Now. It must not be swapped concurrently with cache use.
var timeNow = time.Now
