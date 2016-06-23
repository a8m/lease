package leases

import (
	"errors"
)

type (
	method int
	args   []interface{}
)

const (
	methodCreate = iota
	methodRenew
	methodEvict
	methodTake
	methodList
)

func (m method) String() string {
	return methodNames[m]
}

var methodNames = map[method]string{
	methodCreate: "CreateLeaseTable",
	methodRenew:  "RenewLease",
	methodEvict:  "EvictLease",
	methodTake:   "TakeLease",
	methodList:   "ListLeases",
}

type managerMock struct {
	calls  map[method]int  // method name: call times
	result map[method]args // expected behavior
}

func newManagerMock(behavior map[method]args) *managerMock {
	return &managerMock{
		calls:  make(map[method]int),
		result: behavior,
	}
}

func (m *managerMock) mcalled(name method) int {
	if _, ok := m.calls[name]; !ok {
		m.calls[name] = 1
	} else {
		m.calls[name]++
	}
	return m.calls[name]
}

// record all method calls and return the stubed behavior
// for all functions that returns "error" as a result
func (m *managerMock) errOnly(name method) (err error) {
	i := m.mcalled(name)
	if v := m.result[name][i-1]; v != nil {
		err = v.(error)
	}
	return
}

func (m *managerMock) CreateLeaseTable() error {
	return m.errOnly(methodCreate)
}

func (m *managerMock) RenewLease(*Lease) error {
	return m.errOnly(methodRenew)
}

func (m *managerMock) TakeLease(*Lease) error {
	return m.errOnly(methodTake)
}

func (m *managerMock) EvictLease(*Lease) error {
	return m.errOnly(methodEvict)
}

func (m *managerMock) ListLeases() (leases []*Lease, err error) {
	i := m.mcalled(methodList)
	if v := m.result[methodList][i-1]; v != nil {
		leases = v.([]*Lease)
	} else {
		err = errors.New("list leases failed")
	}
	return
}
