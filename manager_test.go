package lease

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/jpillora/backoff"
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

func TestCreateTable(t *testing.T) {
	client := newClientMock(map[method]args{
		methodCreateTable: {
			// getting "already exists error"
			awserr.New("ResourceInUseException", "", errors.New("")),
			// getting error, should retry until maxCreateRetries
			nil, nil, nil,
			// error = nil, should success
			new(dynamodb.CreateTableOutput),
		},
	})
	manager := newTestManager(client)

	err := manager.CreateLeaseTable()
	assert(t, err == nil, "expecting not to fail while getting 'table already exist' error")
	assert(t, client.calls[methodCreateTable] == 1, "number of calls should be 1")

	err = manager.CreateLeaseTable()
	assert(t, client.calls[methodCreateTable] == 4, "should retry 4 times")
	assert(t, err != nil, "expecting to return the error")

	err = manager.CreateLeaseTable()
	assert(t, err == nil, "expecting not to fail when the request success")
	assert(t, client.calls[methodCreateTable] == 5, "number of calls should be 5")
}

type (
	method int
	args   []interface{}
)

const (
	// Manager methods
	methodCreate = iota
	methodLCreate
	methodDelete
	methodRenew
	methodEvict
	methodTake
	methodList

	// Clientface methods
	methodScan
	methodPutItem
	methodUpdateItem
	methodDeleteItem
	methodCreateTable
)

func (m method) String() string {
	inter := "Manager"
	if m > methodList {
		inter = "Clientface"
	}
	return fmt.Sprintf("%s.%s", inter, methodNames[m])
}

var methodNames = map[method]string{
	methodCreate:      "CreateLeaseTable",
	methodLCreate:     "CreateLease",
	methodDelete:      "DeleteLease",
	methodRenew:       "RenewLease",
	methodEvict:       "EvictLease",
	methodTake:        "TakeLease",
	methodList:        "ListLeases",
	methodScan:        "Scan",
	methodPutItem:     "PutItem",
	methodUpdateItem:  "UpdateItem",
	methodDeleteItem:  "DeleteItem",
	methodCreateTable: "CreateTable",
}

type clientMock struct {
	calls  map[method]int  // method name: call times
	result map[method]args // expected behavior
}

func newClientMock(behavior map[method]args) *clientMock {
	return &clientMock{
		calls:  make(map[method]int),
		result: behavior,
	}
}

func (c *clientMock) mcalled(name method) int {
	if _, ok := c.calls[name]; !ok {
		c.calls[name] = 1
	} else {
		c.calls[name]++
	}
	return c.calls[name]
}

func (c *clientMock) Scan(*dynamodb.ScanInput) (out *dynamodb.ScanOutput, err error) {
	i := c.mcalled(methodScan)
	if v := c.result[methodScan][i-1]; v != nil {
		out = v.(*dynamodb.ScanOutput)
	} else {
		err = errors.New("scan failed")
	}
	return
}

func (c *clientMock) PutItem(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	i := c.mcalled(methodPutItem)
	result := c.result[methodPutItem][i-1]
	if result != nil {
		out, ok := result.(*dynamodb.PutItemOutput)
		if ok {
			return out, nil
		}
		// allows custom errors. for example: 'ConditionalFailed'
		err, ok := result.(awserr.RequestFailure)
		return nil, err
	}
	return nil, errors.New("put item failed")
}

func (c *clientMock) UpdateItem(*dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	i := c.mcalled(methodUpdateItem)
	result := c.result[methodUpdateItem][i-1]
	if result != nil {
		out, ok := result.(*dynamodb.UpdateItemOutput)
		if ok {
			return out, nil
		}
		// allows custom errors. for example: 'ConditionalFailed'
		err, ok := result.(awserr.RequestFailure)
		return nil, err
	}
	return nil, errors.New("update item failed")
}

func (c *clientMock) DeleteItem(*dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	i := c.mcalled(methodDeleteItem)
	result := c.result[methodDeleteItem][i-1]
	if result != nil {
		out, ok := result.(*dynamodb.DeleteItemOutput)
		if ok {
			return out, nil
		}
		// allows custom errors. for example: 'ConditionalFailed'
		err, ok := result.(awserr.RequestFailure)
		return nil, err
	}
	return nil, errors.New("delete item failed")
}

func (c *clientMock) CreateTable(*dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
	i := c.mcalled(methodCreateTable)
	result := c.result[methodCreateTable][i-1]
	if result != nil {
		out, ok := result.(*dynamodb.CreateTableOutput)
		if ok {
			return out, nil
		}
		// allows custom errors. for example: 'ConditionalFailed'
		err, ok := result.(awserr.RequestFailure)
		return nil, err
	}
	return nil, errors.New("create table failed")
}

func newTestManager(client Clientface) *LeaseManager {
	logger := logrus.New()
	logger.Level = logrus.PanicLevel
	config := &Config{
		WorkerId:   "1",
		LeaseTable: "test",
		Logger:     logger,
		Client:     client,
		Backoff:    &Backoff{b: &backoff.Backoff{Min: 0, Max: 0}},
	}
	config.defaults()
	return &LeaseManager{config}
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

func assert(t *testing.T, cond bool, reason string) {
	if !cond {
		t.Error(reason)
	}
}
