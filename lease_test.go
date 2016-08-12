package lease

import (
	"reflect"
	"testing"
	"time"
)

func TestLeaseMetaData(t *testing.T) {
	l := NewLease("foo")

	// Set and Get
	key, val := "bar", "baz"
	l.Set(key, val)
	v, ok := l.Get(key)
	if !ok || v != val {
		t.Errorf("\ngot: (%v, %v)\nexpected: (%v, %v)", v, ok, val, true)
	}
	v, ok = l.Get("foo")
	if ok || v != nil {
		t.Errorf("\ngot: (%v, %v)\nexpected: (%v, %v)", v, ok, nil, false)
	}

	// SetAs and Get
	ss := []string{"foo", "baz"}
	l.SetAs(key, ss, StringSet)
	v, ok = l.Get(key)
	if !ok || !reflect.DeepEqual(v, ss) {
		t.Errorf("\ngot: (%v, %v)\nexpected: (%v, %v)", v, ok, ss, true)
	}

	// Del and Get
	l.Del(key)
	v, ok = l.Get(key)
	if ok || v != nil {
		t.Errorf("\ngot: (%v, %v)\nexpected: (%v, %v)", v, ok, nil, false)
	}

	// hasNoOwner
	if !l.hasNoOwner() {
		t.Error("expect lease to has no owner")
	}

	// isExpired
	l.lastRenewal = time.Now().Add(-time.Minute)
	if !l.isExpired(time.Second * 15) {
		t.Error("expect lease to be expired")
	}

	l.lastRenewal = time.Now().Add(+time.Minute)
	if l.isExpired(time.Second * 15) {
		t.Error("expect lease not to be expired")
	}
}
