package leases

import "time"

// Coordinator abstracts away LeaseTaker and LeaseRenewer from the
// application code that's using leasing and it owns the scheduling of
// the two previously mentioned components.
type Coordinator struct {
	*Config

	// Tick called inside the loop method and it resposible to
	// set the "break" between iterations.
	// for example: in our test cases, we don't want to sleep.
	Tick func() <-chan time.Time

	// coordinator state
	manager Manager
	renewer Renewer
	taker   Taker
	done    chan struct{}
}

// New Coordinator with the given config.
func NewCoordinator(config *Config) *Coordinator {
	config.defaults()
	manager := &LeaseManager{config}
	c := &Coordinator{
		Config:  config,
		manager: manager,
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
		done: make(chan struct{}),
	}
	c.Tick = defaultTick(c)
	return c
}

// Start create the leases table if it's not exist and
// then start background LeaseHolder and LeaseTaker handling.
func (c *Coordinator) Start() error {
	if err := c.manager.CreateLeaseTable(); err != nil {
		return err
	}
	go c.loop()
	return nil
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

// Returns copy of the current held leases.
func (c *Coordinator) GetLeases() []*Lease {
	return c.renewer.GetHeldLeases()
}

// loop run forever and upadte leases periodically.
func (c *Coordinator) loop() {
	defer close(c.done)
	for {
		select {
		case <-c.Tick():
			// Take(or steal leases)
			if err := c.taker.Take(); err != nil {
				c.Logger.WithError(err).Infof("Worker %s failed to take leases", c.WorkerId)
			} else {
				c.Logger.Infof("Worker %s finish to take leases successfully", c.WorkerId)
			}

			// Renew old leases
			if err := c.renewer.Renew(); err != nil {
				c.Logger.WithError(err).Infof("Worker %s failed to renew its leases", c.WorkerId)
			} else {
				c.Logger.Infof("Worker %s finish to renew leases successfully", c.WorkerId)
			}
		// or someone called stop and we need to exit.
		case <-c.done:
			return
		}
	}
}

func defaultTick(c *Coordinator) func() <-chan time.Time {
	firstTime := true
	return func() <-chan time.Time {
		var sleepTime time.Duration
		if firstTime {
			firstTime = false
		} else {
			sleepTime = time.Duration(c.ExpireAfter.Nanoseconds() / 3)
			c.Logger.Infof("Worker %s sleep for: %s", c.WorkerId, sleepTime)
		}
		return time.After(sleepTime)
	}
}
