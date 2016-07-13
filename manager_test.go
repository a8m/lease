package lease

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/jpillora/backoff"
)

func TestCreateTable(t *testing.T) {
	client := newClientMock(map[method]args{
		methodCreateTable: {
			// getting "already exists error"
			awserr.New("ResourceInUseException", "", errors.New("")),
			// getting error, should retry until maxCreateRetries
			nil, nil, nil,
			// create table finished successfully
			new(dynamodb.CreateTableOutput),
		},
	})
	manager := newTestManager(client)

	err := manager.CreateLeaseTable()
	assert(t, err == nil, "expect not to fail while getting 'table already exist' error")
	assert(t, client.calls[methodCreateTable] == 1, "number of calls should be 1")

	err = manager.CreateLeaseTable()
	assert(t, client.calls[methodCreateTable] == 4, "should retry 4 times")
	assert(t, err != nil, "expect to returns the error")

	err = manager.CreateLeaseTable()
	assert(t, err == nil, "expect not to fail when the request success")
	assert(t, client.calls[methodCreateTable] == 5, "number of calls should be 5")
}

func TestListLeases(t *testing.T) {
	client := newClientMock(map[method]args{
		methodScan: {
			// getting error from dynamodb
			nil, nil, nil,
			// scan table finished successfully
			&dynamodb.ScanOutput{
				Items: []map[string]*dynamodb.AttributeValue{
					{"leaseKey": {S: aws.String("foo")}},
					{"leaseKey": {S: aws.String("bar")}},
					{"leaseKey": {S: aws.String("baz")}},
				},
			},
		},
	})
	manager := newTestManager(client)

	leases, err := manager.ListLeases()
	assert(t, err != nil, "expect to returns the error")
	assert(t, client.calls[methodScan] == 3, "number of calls should be 3")

	leases, err = manager.ListLeases()
	assert(t, err == nil, "expect not to fail when the request success")
	assert(t, client.calls[methodScan] == 4, "number of calls should be 4")

	expectedLeases := []string{"foo", "bar", "baz"}
	for i := range leases {
		k1, k2 := leases[i].Key, expectedLeases[i]
		assert(t, k1 == k2, fmt.Sprintf("expect %s to equal %s", k1, k1))
	}
}

func TestRenewLease(t *testing.T) {
	client := newClientMock(map[method]args{
		methodUpdateItem: {
			// update item finsihed successfully
			new(dynamodb.UpdateItemOutput),
			// getting error from dynamodb
			nil, nil,
		},
	})
	manager := newTestManager(client)

	leaseToRenew := &Lease{Key: "foo", Counter: 10, Owner: "o1"}
	err := manager.RenewLease(leaseToRenew)
	assert(t, err == nil, "expect not to fail")
	assert(t, leaseToRenew.Counter == 11, "expect leaseCounter to be 11")

	err = manager.RenewLease(leaseToRenew)
	assert(t, err != nil, "expect to returns the error")
	assert(t, leaseToRenew.Counter == 11, "expect leaseCounter to be 11")
	assert(t, client.calls[methodUpdateItem] == 3, "number of calls should be 3")
}

func TestEvictLease(t *testing.T) {
	client := newClientMock(map[method]args{
		methodUpdateItem: {
			// getting error from dynamodb
			nil, nil,
			// update item finsihed successfully
			new(dynamodb.UpdateItemOutput),
		},
	})
	manager := newTestManager(client)

	leaseToEvict := &Lease{Key: "foo", Counter: 10, Owner: "o1"}
	err := manager.EvictLease(leaseToEvict)
	assert(t, err != nil, "expect to returns the error")
	assert(t, leaseToEvict.Owner == "o1", "expect leaseOwner to be the same")
	assert(t, client.calls[methodUpdateItem] == 2, "number of calls should be 2")

	err = manager.EvictLease(leaseToEvict)
	assert(t, err == nil, "expect not to fail")
	assert(t, leaseToEvict.Counter == 10, "expect leaseCounter to be the same")
	assert(t, leaseToEvict.Owner == "NULL", "expect leaseOwner to be the 'NULL'")
}

func TestTakeLease(t *testing.T) {
	client := newClientMock(map[method]args{
		methodUpdateItem: {
			// getting error from dynamodb
			nil, nil,
			// update item finsihed successfully
			new(dynamodb.UpdateItemOutput),
		},
	})
	manager := newTestManager(client)

	leaseToTake := &Lease{Key: "foo", Counter: 10, Owner: "o1"}
	err := manager.TakeLease(leaseToTake)
	assert(t, err != nil, "expect to returns the error")
	assert(t, leaseToTake.Owner == "o1" && leaseToTake.Counter == 10, "expect leaseOwner and leaseCounter to be the same")

	err = manager.TakeLease(leaseToTake)
	assert(t, err == nil, "expect not to fail")
	assert(t, leaseToTake.Owner == manager.WorkerId, "expect owner to equal workerId")
	assert(t, leaseToTake.Counter == 11, "expect counter to be increment by 1")
}

func TestDeleteLease(t *testing.T) {
	client := newClientMock(map[method]args{
		methodDeleteItem: {
			// delete item finished successfully
			new(dynamodb.DeleteItemOutput),
			// getting "conditional error"
			awserr.New("ConditionalCheckFailedException", "", errors.New("")),
			// getting error from dynamodb
			nil, nil,
		},
	})
	manager := newTestManager(client)

	leaseToDelete := &Lease{Key: "foo"}
	err := manager.DeleteLease(leaseToDelete)
	assert(t, err == nil, "expect not to fail")
	assert(t, client.calls[methodDeleteItem] == 1, "expect number of calls to equal 1")

	err = manager.DeleteLease(leaseToDelete)
	assert(t, err != nil, "expect returns the conditional error")
	assert(t, client.calls[methodDeleteItem] == 2, "expect number of calls to equal 2")
}

func TestCreateLease(t *testing.T) {
	client := newClientMock(map[method]args{
		methodPutItem: {
			// delete item finished successfully
			new(dynamodb.PutItemOutput),
			// getting "conditional error"
			awserr.New("ConditionalCheckFailedException", "", errors.New("")),
			// getting error from dynamodb
			nil, nil, nil,
		},
	})
	manager := newTestManager(client)

	leaseToCreate := &Lease{Key: "bar"}
	lease, err := manager.CreateLease(leaseToCreate)
	assert(t, err == nil, "expect CreateLease not to fail")
	assert(t, client.calls[methodPutItem] == 1, "expect number of calls to equal 1")
	assert(t, lease.Owner == manager.WorkerId && lease.Counter == 1, "expect taking the lease")

	_, err = manager.CreateLease(leaseToCreate)
	assert(t, err != nil, "expect CreateLease to fail")
	assert(t, client.calls[methodPutItem] == 2, "expect not retry on conditional failure")

	_, err = manager.CreateLease(leaseToCreate)
	assert(t, err != nil, "expect CreateLease to fail")
	assert(t, client.calls[methodPutItem] == 5, "expect CreateLease to retry 3 times")
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
		err, ok := result.(awserr.Error)
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
		err, ok := result.(awserr.Error)
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
		err, ok := result.(awserr.Error)
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
		err, ok := result.(awserr.Error)
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
