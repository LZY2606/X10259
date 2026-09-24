package ccache

import "time"

// now is the single clock behind every expiry decision in the cache:
// setting items (bucket.set/setnx/setnx2), extending them, and the
// Item.Expired/TTL readers all go through it.
//
// Production always uses time.Now. Tests replace it with a fake clock so
// that expiry can be advanced deterministically; because the fake clock is
// installed at this one seam, tests exercise exactly the same expiry code
// paths that run in production (no test-only algorithm is compiled in).
var now = time.Now
