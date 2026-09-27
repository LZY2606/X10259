package ccache

import (
	"strconv"
	"testing"
	"time"
)

func TestAllocBaselineGet(t *testing.T) {
	c := New(Configure[int]())
	defer c.Stop()
	for i := 0; i < 500; i++ {
		c.Set(strconv.Itoa(i), i, time.Minute)
	}
	c.SyncUpdates()
	n := testing.AllocsPerRun(1000, func() { c.Get("123") })
	t.Logf("GET_ALLOCS=%v", n)
	n = testing.AllocsPerRun(1000, func() { c.GetWithoutPromote("123") })
	t.Logf("GETNP_ALLOCS=%v", n)
}

func TestAllocBaselineSet(t *testing.T) {
	c := New(Configure[int]())
	defer c.Stop()
	n := testing.AllocsPerRun(1000, func() {
		c.Set("k", 1, time.Minute)
	})
	t.Logf("SET_ALLOCS=%v", n)
	n = testing.AllocsPerRun(1000, func() {
		c.Delete("k")
	})
	t.Logf("DEL_ALLOCS=%v", n)
}
