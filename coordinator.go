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
	done    chan struct{}
}

// NewCoordinator create new Coordinator with the given config.
func NewCoordinator(config *Config) *Coordinator {
	manager := &LeaseManager{config}
	c := &Coordinator{
		Config: config,
		done:   make(chan struct{}),
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

// Stop the coordinator gracefully. wait for background tasks to complete.
func (c *Coordinator) Stop() {
	c.Logger.Info("stopping coordinator")

	// notify loop
	c.done <- struct{}{}

	// wait
	<-c.done

	c.Logger.Info("stopped coordinator")
}

// loop run forever and upadte leases periodically.
func (c *Coordinator) loop() {
	defer close(c.done)

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

		select {
		// wait for a while and loop again.
		case <-c.ticker():
			continue
		// or someone called stop and we need to exit.
		case <-c.done:
			return
		}
	}
}

func (c *Coordinator) ticker() <-chan time.Time {
	sleepTime := time.Duration(c.rand.Int63n(c.ExpireAfter.Nanoseconds() / 3))
	c.Logger.Infof("Worker %s sleep for: %s", c.OwnerId, sleepTime)
	return time.After(sleepTime)
}
