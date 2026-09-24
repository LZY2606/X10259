package ccache

// itemState is the lifecycle state of an Item as it moves between a bucket,
// the promotables queue, the LRU list and the deletables queue. It is the
// single source of truth for promotion, expiry and OnDelete-callback
// ownership:
//
//	unlisted --(worker promotes)--> listed --(worker deletes/gc)--> deleted
//	unlisted --(delete overtakes promotion / Clear)--> deleted
//
// deleted is terminal: a deleted item must never be promoted (back) into the
// list, so a late promotion arriving through the promotables queue is dropped
// and can neither resurrect the item nor fire its callback a second time.
//
// The state is only mutated by the cache's worker goroutine, or while every
// bucket is write-locked (Clear), so plain loads and stores are sufficient.
type itemState int32

const (
	// stateUnlisted: the item lives in a bucket (and possibly still sits in
	// the promotables queue) but has not been inserted into the LRU list.
	stateUnlisted itemState = iota
	// stateListed: the item is inserted into the LRU list and its size is
	// accounted in the cache's size counter.
	stateListed
	// stateDeleted: terminal. The item has been retired and the ownership of
	// its OnDelete callback has been consumed (exactly once, and only for
	// items that reached stateListed).
	stateDeleted
)

func (s itemState) String() string {
	switch s {
	case stateUnlisted:
		return "unlisted"
	case stateListed:
		return "listed"
	case stateDeleted:
		return "deleted"
	}
	return "unknown"
}

// isDeleted reports whether the item has reached the terminal deleted state.
func (i *Item[T]) isDeleted() bool {
	return i.state == stateDeleted
}

// markDeleted moves the item to the terminal deleted state and reports
// whether the caller acquired ownership of the OnDelete callback. Exactly one
// caller ever receives true: the first deletion of a listed item. Deleting an
// unlisted item retires it without a callback (it never accounted for size),
// and deleting an already-deleted item is a no-op.
func (i *Item[T]) markDeleted() (ownsCallback bool) {
	if i.state == stateDeleted {
		return false
	}
	ownsCallback = i.state == stateListed
	i.state = stateDeleted
	return ownsCallback
}
