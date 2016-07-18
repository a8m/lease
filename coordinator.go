package lease

import "time"

// Coordinator abstracts away LeaseTaker and LeaseRenewer from the
// application code that's using leasing and it owns the scheduling of
// the two previously mentioned components.
type Coordinator struct {
	*Config
	Manager Manager
	Renewer Renewer
	Taker   Taker
	// coordinator state
	stopTaker  chan struct{}
	stopRenwer chan struct{}
}

// Taker or Renewer loop function
type loopFunc func() error

// New create new Coordinator with the given config.
func New(config *Config) Leaser {
	config.defaults()
	manager := &LeaseManager{config}
	return &Coordinator{
		Config:  config,
		Manager: manager,
		Renewer: &leaseHolder{
			Config:     config,
			manager:    manager,
			heldLeases: make(map[string]*Lease),
		},
		Taker: &leaseTaker{
			Config:    config,
			manager:   manager,
			allLeases: make(map[string]*Lease),
		},
	}
}

// Start create the leases table if it's not exist and
// then start background leaseHolder and leaseTaker handling.
func (c *Coordinator) Start() error {
	if err := c.Manager.CreateLeaseTable(); err != nil {
		return err
	}

	takerIntervalMills := (c.ExpireAfter + c.epsilonMills) * 2
	renewerIntervalMills := c.ExpireAfter/3 - c.epsilonMills

	c.stopTaker = c.loop(c.Taker.Take, takerIntervalMills, "take leases")
	c.stopRenwer = c.loop(c.Renewer.Renew, renewerIntervalMills, "renew leases")

	c.Logger.Infof("Start coordinator with failover time %s, and epsilon %s. "+
		"LeaseCoordinator will renew leases every %s, take leases every %s "+
		"and steal %d lease(s) at a time.",
		c.ExpireAfter,
		c.epsilonMills,
		renewerIntervalMills,
		takerIntervalMills,
		c.MaxLeasesToStealAtOneTime)

	return nil
}

// Stop the coordinator gracefully. wait for background tasks to complete.
func (c *Coordinator) Stop() {
	c.Logger.Info("stopping coordinator")

	// stop taker loop
	c.stopTaker <- struct{}{}

	// wait for close
	<-c.stopTaker

	// stop renewer loop
	c.stopRenwer <- struct{}{}

	// wait for close
	<-c.stopRenwer

	c.Logger.Info("stopped coordinator")
}

// Returns copy of the current held leases.
func (c *Coordinator) GetLeases() []Lease {
	return c.Renewer.GetHeldLeases()
}

// Delete the given lease from DB. does nothing when passed alease that does
// not exist in the DB.
func (c *Coordinator) Delete(l Lease) error {
	return c.Manager.DeleteLease(&l)
}

// Create a new lease. conditional on a lease not already existing with different
// owner and counter.
func (c *Coordinator) Create(l Lease) (Lease, error) {
	lease, err := c.Manager.CreateLease(&l)
	if err != nil {
		return l, err
	}
	return *lease, nil
}

// loop spawn a goroutine and returns a "done" channel that linked to this goroutine.
// the interval used to create a ticker to run the given loopFunc each x time and
// the reason string used for logging.
func (c *Coordinator) loop(fn loopFunc, interval time.Duration, reason string) chan struct{} {
	done := make(chan struct{})
	go func() {
		ticker := c.ticker(interval)
		defer close(done)

		for {
			select {
			// taker or renew old leases
			case <-ticker():
				if err := fn(); err != nil {
					c.Logger.WithError(err).Errorf("Worker %s failed to %s", c.WorkerId, reason)
				}
			// someone called stop and we need to exit.
			case <-done:
				return
			}
		}
	}()

	return done
}

// ticker returns time.Time channel that called with zero value in the first call.
// used to start 'taking'(or 'renewing') leases immediately.
func (c *Coordinator) ticker(d time.Duration) func() <-chan time.Time {
	firstTime := true
	return func() <-chan time.Time {
		sleepTime := d
		if firstTime {
			firstTime = false
			sleepTime = 0
		}
		return time.After(sleepTime)
	}
}
