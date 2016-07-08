package lease

import (
	"errors"
)

// Test cases:
// 1. create lease table
//    - getting "already exists error"
//    - getting error, should retry until maxCreateRetries
//    - error = nil, should success
// 2. listLeases
//    - gettign error from db, should return an err
//    - when success, test unmarshalig
// 3. renewLease
//    - while success, should increment the counter
//    - while failed. should not increment the counter
// 4. takeLease
//    - while success. should set the owner and inrement the counter
//    - when failed. should not
// 5. evictLease
//    - when success. should set the owner to NULL
//    - when failed. should not.

type (
	method int
	args   []interface{}
)

const (
	methodCreate = iota
	methodLCreate
	methodDelete
	methodRenew
	methodEvict
	methodTake
	methodList
)

func (m method) String() string {
	return methodNames[m]
}

var methodNames = map[method]string{
	methodCreate:  "CreateLeaseTable",
	methodLCreate: "CreateLease",
	methodDelete:  "DeleteLease",
	methodRenew:   "RenewLease",
	methodEvict:   "EvictLease",
	methodTake:    "TakeLease",
	methodList:    "ListLeases",
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

func (m *managerMock) DeleteLease(*Lease) error {
	return m.errOnly(methodDelete)
}

func (m *managerMock) CreateLease(l *Lease) (*Lease, error) {
	return l, m.errOnly(methodLCreate)
}

func (m *managerMock) RenewLease(*Lease) error {
	return m.errOnly(methodRenew)
}

func (m *managerMock) TakeLease(*Lease) error {
	return m.errOnly(methodTake)
}

func (m *managerMock) EvictLease(l *Lease) error {
	l.Owner = "NULL"
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
