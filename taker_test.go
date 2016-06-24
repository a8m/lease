package leases

// Test cases:
// 1. there are 2 workers(incloding me), two leases
//    and I does not hold any lease. expect to stole 1
// 2. there are 2 workers, 1 leases
//    and I does not hold any lease. expect do nothing.
// 3. threre are 2 workers, 3 leases, and of them expired.
//    expect to evict and take it
// 4. threre are 3 workers(including me), 3 leases
//    and I holding 1 lease. expect do nothing.
// 5. there are 3 workers(including me), 3 leases
//    and I does not any lease. expect to stole from the most loaded worker
// 6

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
	/*{
		`2 workers(incloding me). 1 leases.
		and I does not hold any lease. expect to do nothing`,
		make(map[string]*Lease),
		map[method]args{
			methodList: {[]*Lease{
				&Lease{Key: "foo", Owner: "1", lastRenewal: time.Now(), Counter: 10},
			}},
			methodTake: {nil},
		},
		map[method]int{
			methodList: 1,
			methodTake: 0,
		},
	},*/
}

func TestTakerCases(t *testing.T) {
	for _, test := range takerTestCases {
		logger := logrus.New()
		logger.Level = logrus.DebugLevel
		manager := newManagerMock(test.managerBehavior)
		taker := &LeaseTaker{
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
