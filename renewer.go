package leases

import (
	"strings"
	"sync"
)

// LeaseRenewer used by the LeaseCoordinator to renew leases held by the system.
// Each LeaseCoordinator instance corresponds to one worker and uses exactly one LeaseRenewer
// to manage lease renewal for that worker.
type Renewer interface {
	// TODO:
	// Proposal: Renew method will return (int, error) -
	// the int will represent the number of leases that this worker holds
	Renew() error
	GetHeldLeases() []*Lease
}

// LeaseHolder is the default implementation of Renewer that uses DynamoDB
// via LeaseManager
type LeaseHolder struct {
	sync.Mutex
	*Config
	manager    Manager
	heldLeases map[string]*Lease
}

// Attempt to renew all currently held leases.
func (l *LeaseHolder) Renew() error {
	leases, err := l.manager.ListLeases()
	if err != nil {
		return err
	}

	// remove leases that deleted from the DynamoDB table.
	lostLeases := make([]string)
	for _, hlease := range l.heldLeases {
		exist := false
		for lease := range leases {
			if lease.Key == hlease.Key {
				exist = true
			}
		}
		if !exist {
			l.Lock()
			delete(l.heldLeases, hlease.Key)
			l.Unlock()
			lostLeases = append(lostLeases, hlease.Key)
		}
	}
	if n := len(lostLeases); n > 0 {
		l.Logger.Debugf("Worker %s lost %d leases due deprecation: %s",
			l.WorkerId,
			n,
			strings.Join(lostLeases, ", "))
	}

	// remove all the leases that stoled from this worker, or renew the leases
	// that we still hold.
	for _, lease := range leases {
		if lease.Owner == l.WorkerId {
			l.Lock()
			l.heldLeases[lease.Key] = lease
			l.Unlock()
			if err := l.renewLease(lease); err != nil {
				l.Logger.Debug("Worker %s could not renew lease with key %s", l.WorkerId, lease.Key)
			}
		} else {
			if _, ok := l.heldLeases[lease.Key]; ok {
				l.Logger.Debugf("Worker %s lost lease with key %s", l.WorkerId, lease.Key)
				l.Lock()
				delete(l.heldLeases, lease.Key)
				l.Unlock()
			}
		}
	}

	// print the currently held leases belongs to this worker.
	if keys := l.keys(); len(keys) > 0 {
		l.Logger.Debugf("Worker %s hold leases: %s", l.WorkerId, strings.Join(keys, ", "))
	}
	return nil
}

// Renew a lease by incrementing the lease counter.
// TODO: Add Conditional on the leaseCounter in DynamoDB matching the leaseCounter of the input
func (l *LeaseHolder) renewLease(lease *Lease) (err error) {
	lease.Counter++
	return l.manager.UpdateLease(lease)
}

// Returns currently held leases.
// A lease is currently held if we successfully renewed it on the last
// run of Renew()
// Lease objects returned are copies and their lease counters will not tick.
func (l *LeaseHolder) GetHeldLeases() (leases []*Lease) {
	l.Lock()
	defer l.Unlock()
	for _, lease := range l.heldLeases {
		copy := *lease
		leases = append(leases, &copy)
	}
	return
}

// keys return all worker's leases
func (l *LeaseHolder) keys() (keys []string) {
	for k, _ := range l.heldLeases {
		keys = append(keys, k)
	}
	return keys
}
