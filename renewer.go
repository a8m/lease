package lease

import (
	"strings"
	"sync"
)

// LeaseRenewer used by the LeaseCoordinator to renew leases held by the system.
// Each LeaseCoordinator instance corresponds to one worker and uses exactly one LeaseRenewer
// to manage lease renewal for that worker.
type Renewer interface {
	Renew() error
	GetHeldLeases() []Lease
}

// leaseHolder is the default implementation of Renewer that uses DynamoDB
// via LeaseManager
type leaseHolder struct {
	sync.RWMutex
	*Config
	manager    Manager
	heldLeases map[string]*Lease
}

// Attempt to renew all currently held leases.
func (l *leaseHolder) Renew() error {
	leases, err := l.manager.ListLeases()
	if err != nil {
		return err
	}

	// remove leases that deleted from the DynamoDB table.
	lostLeases := make([]string, 0)
	for key, _ := range l.heldLeases {
		exist := false
		for _, lease := range leases {
			if lease.Key == key {
				exist = true
			}
		}
		if !exist {
			l.Lock()
			delete(l.heldLeases, key)
			l.Unlock()
			lostLeases = append(lostLeases, key)
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
			// if we took this lease and it's not holds by this renewer
			if _, ok := l.heldLeases[lease.Key]; !ok {
				l.Lock()
				l.heldLeases[lease.Key] = lease
				l.Unlock()
			}
			if err := l.manager.RenewLease(lease); err != nil {
				l.Logger.Debugf("Worker %s could not renew lease with key %s", l.WorkerId, lease.Key)
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

// Returns currently held leases.
// A lease is currently held if we successfully renewed it on the last
// run of Renew()
// Lease objects returned are copies and their lease counters will not tick.
func (l *leaseHolder) GetHeldLeases() (leases []Lease) {
	l.RLock()
	defer l.RUnlock()
	for _, lease := range l.heldLeases {
		leases = append(leases, *lease)
	}
	return
}

// keys return all worker's leases
func (l *leaseHolder) keys() (keys []string) {
	for k, _ := range l.heldLeases {
		keys = append(keys, k)
	}
	return keys
}
