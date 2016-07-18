package lease

import (
	"testing"
	"time"

	"github.com/Sirupsen/logrus"
)

type takerTest struct {
	name            string
	prevState       map[string]*Lease // leases we holds from the previous loop
	managerBehavior map[method]args   // manager expected behavior
	expectedCalls   map[method]int    // manager expected calls
}

var (
	takerId = "3"
)

var takerTestCases = []takerTest{
	{
		`2 workers(incloding me). 2 leases.
		and I does not hold any lease. expect to stole 1`,
		make(map[string]*Lease),
		map[method]args{
			methodList: {[]*Lease{
				&Lease{Key: "foo", Owner: "1", lastRenewal: time.Now()},
				&Lease{Key: "bar", Owner: "1", lastRenewal: time.Now()},
			}},
			methodTake: {nil},
		},
		map[method]int{
			methodList: 1,
			methodTake: 1,
		},
	},
	{
		`2 workers(incloding me). 1 leases.
		and I does not hold any lease. expect to do nothing`,
		make(map[string]*Lease),
		map[method]args{
			methodList: {[]*Lease{
				&Lease{Key: "foo", Owner: "1", lastRenewal: time.Now()},
			}},
			methodTake: {nil},
		},
		map[method]int{
			methodList: 1,
			methodTake: 0,
		},
	},
	{
		`2 workers, 3 leases, and all of them expired.
		I does not hold any leases, and this is my first scanning.
		expect to take two leases.`,
		make(map[string]*Lease),
		map[method]args{
			methodList: {[]*Lease{
				&Lease{Key: "foo", Owner: "1", lastRenewal: time.Now().Add(-time.Hour)},
				&Lease{Key: "bar", Owner: "1", lastRenewal: time.Now().Add(-time.Hour)},
				&Lease{Key: "baz", Owner: "1", lastRenewal: time.Now().Add(-time.Hour)},
			}},
			methodTake: {nil, nil},
		},
		map[method]int{
			methodList: 1,
			methodTake: 2,
		},
	},
	{
		`threre are 2 workers(including me), and 3 leases.
		worker "1" helds them.
		but now, he stopped and not hold them any more.
		expect to evict all these leases and take them.`,
		map[string]*Lease{
			"foo": &Lease{Key: "foo", Owner: "1", lastRenewal: time.Now().Add(-time.Hour), Counter: 10},
			"bar": &Lease{Key: "bar", Owner: "1", lastRenewal: time.Now().Add(-time.Hour), Counter: 10},
			"baz": &Lease{Key: "baz", Owner: "1", lastRenewal: time.Now().Add(-time.Hour), Counter: 10},
		},
		map[method]args{
			methodList: {[]*Lease{
				&Lease{Key: "foo", Owner: "1", Counter: 10},
				&Lease{Key: "bar", Owner: "1", Counter: 10},
				&Lease{Key: "baz", Owner: "1", Counter: 10},
			}},
			methodTake:  {nil, nil, nil},
			methodEvict: {nil, nil, nil},
		},
		map[method]int{
			methodList:  1,
			methodTake:  3,
			methodEvict: 3,
		},
	},
}

func TestTakerCases(t *testing.T) {
	for _, test := range takerTestCases {
		logger := logrus.New()
		logger.Level = logrus.PanicLevel
		manager := newManagerMock(test.managerBehavior)
		taker := &leaseTaker{
			Config: &Config{WorkerId: takerId,
				Logger:                    logger,
				ExpireAfter:               time.Minute,
				MaxLeasesToStealAtOneTime: 1,
			},
			manager:   manager,
			allLeases: test.prevState,
		}
		taker.Take()
		// test method calls expectations
		for method, calls := range test.expectedCalls {
			if n := manager.calls[method]; n != calls {
				t.Errorf("%s: got\n\t%+v\nexpected\n\t%v", test.name, n, calls)
			}
		}
	}
}
