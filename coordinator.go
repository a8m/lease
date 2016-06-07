package leases

import (
	"math/rand"
	"time"
)

type Coordinator struct {
	*Config
	rand    *rand.Rand
	renewer Renewer
	taker   Taker
}

// NewCoordinator create new Coordinator with the given config.
func NewCoordinator(config *Config) *Coordinator {
	manager := &LeaseManager{config}
	c := &Coordinator{
		Config: config,
		renewer: &LeaseHolder{
			Config:     config,
			manager:    manager,
			heldLeases: make(map[string]*Lease),
		},
		taker: &LeaseTaker{
			Config:    config,
			manager:   manager,
			allLeases: make(map[string]*Lease),
		},
		rand: rand.New(rand.NewSource(time.Now().UTC().UnixNano())),
	}
	// start background LeaseHolder and LeaseTaker handling
	go c.loop()
	return c
}

// Returns copy of the current held leases.
func (c *Coordinator) GetLeases() []*Lease {
	return c.renewer.GetHeldLeases()
}

func (c *Coordinator) loop() {
	for {
		// Take(or steal leases)
		if err := c.taker.Take(); err != nil {
			c.Logger.Infof("Worker %s failed to take leases", c.OwnerId)
		} else {
			c.Logger.Infof("Worker %s finish to take leases successfully", c.OwnerId)
		}

		// Renew old leases
		if err := c.renewer.Renew(); err != nil {
			c.Logger.Infof("Worker %s failed to renew its leases", c.OwnerId)
		} else {
			c.Logger.Infof("Worker %s finish to renew leases successfully", c.OwnerId)
		}

		// wait for a while...
		<-c.ticker()
	}
}

// ticker return timer chan between 1-2 minutes
// TODO: ticker should be configurable with lower and higher threshold.
func (c *Coordinator) ticker() <-chan time.Time {
	sleepTime := time.Duration(c.rand.Int63n(c.ExpireAfter.Nanoseconds() / 3))
	c.Logger.Infof("Worker %s sleep for: %s", c.OwnerId, sleepTime)
	return time.After(sleepTime)
}
