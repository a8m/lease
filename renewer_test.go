package lease

import (
	"testing"

	"github.com/Sirupsen/logrus"
)

type renewerTest struct {
	name               string
	prevState          map[string]*Lease // leases we holds from the previous loop
	managerBehavior    map[method]args   // manager expected behavior
	expectedCalls      map[method]int    // manager expected calls
	expectedHeldLeases []Lease           // expected result while calling GetHeldLeases
}

var (
	renewerId = "1"
	lease1    = &Lease{Key: "foo", Owner: "2"}
	lease2    = &Lease{Key: "bar", Owner: renewerId}
	lease3    = &Lease{Key: "baz", Owner: renewerId}
)

var renewerTestCases = []renewerTest{
	{
		"we holds 0 leases. gets 2 leases from the manager belongs to this worker. expect to renew 2",
		make(map[string]*Lease),
		map[method]args{
			methodList:  {[]*Lease{lease2, lease3}},
			methodRenew: {nil, nil},
		},
		map[method]int{
			methodList:  1,
			methodRenew: 2,
		},
		[]Lease{*lease2, *lease3},
	},
	{
		"we holds 2 leases. gets 2 leases from the manager belongs to this worker. expect to renew 2",
		map[string]*Lease{
			lease2.Key: lease2,
			lease3.Key: lease3,
		},
		map[method]args{
			methodList:  {[]*Lease{lease2, lease3}},
			methodRenew: {nil, nil},
		},
		map[method]int{
			methodList:  1,
			methodRenew: 2,
		},
		[]Lease{*lease2, *lease3},
	},
	{
		"we holds 1 lease. gets 2 leases from the manager. 1 belongs to this worker. expect to renew 1",
		map[string]*Lease{
			lease2.Key: lease2,
		},
		map[method]args{
			methodList:  {[]*Lease{lease1, lease2}},
			methodRenew: {nil},
		},
		map[method]int{
			methodList:  1,
			methodRenew: 1,
		},
		[]Lease{*lease2},
	},
	{
		"we holds 2 leases, but someone delete them from the db. expect to renew 0",
		map[string]*Lease{
			lease2.Key: lease2,
			lease3.Key: lease3,
		},
		map[method]args{
			methodList: {[]*Lease{}},
		},
		map[method]int{
			methodList:  1,
			methodRenew: 0,
		},
		[]Lease{},
	},
	{
		"we holds 2 leases, but someone stoled 1 from us. expect to renew 1",
		map[string]*Lease{
			lease2.Key: lease2,
			lease3.Key: lease3,
		},
		map[method]args{
			methodList: {[]*Lease{
				&Lease{Key: lease2.Key, Owner: "3"},
				lease3,
			}},
			methodRenew: {nil},
		},
		map[method]int{
			methodList:  1,
			methodRenew: 1,
		},
		[]Lease{*lease3},
	},
	{
		"we holds 2 leases, but someone stoled them from us. expect to renew 0",
		map[string]*Lease{
			lease2.Key: lease2,
			lease3.Key: lease3,
		},
		map[method]args{
			methodList: {[]*Lease{
				&Lease{Key: lease2.Key, Owner: "3"},
				&Lease{Key: lease3.Key, Owner: "4"},
			}},
		},
		map[method]int{
			methodList:  1,
			methodRenew: 0,
		},
		[]Lease{},
	},
}

func TestRenewerCases(t *testing.T) {
	for _, test := range renewerTestCases {
		logger := logrus.New()
		logger.Level = logrus.PanicLevel
		manager := newManagerMock(test.managerBehavior)
		holder := &leaseHolder{
			Config:     &Config{WorkerId: renewerId, Logger: logger},
			manager:    manager,
			heldLeases: test.prevState,
		}
		holder.Renew()
		// test method calls expectations
		for method, calls := range test.expectedCalls {
			if n := manager.calls[method]; n != calls {
				t.Errorf("%s: got\n\t%+v\nexpected\n\t%v", test.name, n, calls)
			}
		}
		// test GetHeldLeases equality
		leases := holder.GetHeldLeases()
		if len(leases) != len(test.expectedHeldLeases) {
			t.Errorf("%s\nGetHeldLeases - length: got\n\t%+v\nexpected\n\t%v",
				test.name, len(leases), len(test.expectedHeldLeases))
		}
		for _, l1 := range test.expectedHeldLeases {
			found := false
			for _, l2 := range leases {
				if l1.Key == l2.Key {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("%s: expected lease to be exists in result:\n\t%+v\n", l1)
			}
		}
	}
}
