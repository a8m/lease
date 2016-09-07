package lease

import "time"

// Coordinator is the implemtation of the Leaser interface.
// It's abstracts away LeaseTaker and LeaseRenewer from the application
// code that using leasing and it owns the scheduling of two previously
// mentioned components.
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
	manager := &LeaseManager{config, newSerializer()}
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

// GetHeldLeases returns the currently held leases.
// A lease is currently held if we successfully renewed it on the last run of Renewer.Renew().
// Lease objects returned are copies and their counters will not tick.
func (c *Coordinator) GetHeldLeases() []Lease {
	return c.Renewer.GetHeldLeases()
}

// Delete the given lease from DB. does nothing when passed a lease that does
// not exist in the DB.
// The deletion is conditional on the fact that the lease is being held by this worker.
func (c *Coordinator) Delete(l Lease) error {
	return c.Manager.DeleteLease(&l)
}

// Create a new lease.
// Conditional on a lease not already existing with different owner and counter.
func (c *Coordinator) Create(lease Lease) (Lease, error) {
	clease, err := c.Manager.CreateLease(&lease)
	if err != nil {
		return lease, err
	}
	return *clease, nil
}

// Update used to update only the extra fields on the Lease object and
// it cannot be used to update internal fields such as leaseCounter, leaseOwner.
//
// Fails if we do not hold the lease, or if the concurrency token does not match
// the concurrency token on the internal authoritative copy of the lease
// (ie, if we lost and re-acquired the lease).
//
// With this method you will be able to update the task status, or any
// other fields.
// for example: {"status": "done", "last_update": "unix seconds"}
// To add extra fields on a Lease, use Lease.Set(key, val)
func (c *Coordinator) Update(lease Lease) (Lease, error) {
	var heldLease Lease
	for _, hlease := range c.Renewer.GetHeldLeases() {
		if lease.Key == hlease.Key {
			heldLease = hlease
			break
		}
	}

	// fails if we don't hold the passed-in lease object
	if heldLease.hasNoOwner() {
		return lease, ErrLeaseNotHeld
	}

	// or if the concurrency token does not match
	if heldLease.concurrencyToken != lease.concurrencyToken {
		return lease, ErrTokenNotMatch
	}

	ulease, err := c.Manager.UpdateLease(&lease)
	if err != nil {
		return lease, err
	}
	return *ulease, nil
}

// ForceUpdate used to update the lease object without checking if the concurrency
// token is valid or if we already lost this lease.
//
// Unlike Update, this method allows you to update the task status,
// or any other fields even if you lost the lease.
//
// for example: {"status": "done", "last_update": "unix seconds"}
// To add extra fields on a Lease, use Lease.Set(key, val)
func (c *Coordinator) ForceUpdate(lease Lease) (Lease, error) {
	ulease, err := c.Manager.UpdateLease(&lease)
	if err != nil {
		return lease, err
	}
	return *ulease, nil
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
