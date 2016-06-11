package leases

import (
	"strings"
	"sync"
)

// Renewer in the interface that wraps the Renew and GetHeldLeases methods.
type Renewer interface {
	Renew() error
	GetHeldLeases() []*Lease
}

// LeaseHolder used by the LeaseCoordinator to renew held leases.
type LeaseHolder struct {
	sync.Mutex
	*Config
	manager Manager

	heldLeases map[string]*Lease
}

// Attempt to renew all currently held leases.
func (l *LeaseHolder) Renew() error {
	list, err := l.manager.ListLeases()
	if err != nil {
		return err
	}

	heldLeases := make(map[string]*Lease)
	for _, lease := range list {
		if lease.Owner == l.WorkerId {
			heldLeases[lease.Key] = lease
			if err := l.renewLease(lease); err != nil {
				l.Logger.Debug("Worker %s could not renew lease with key %s", l.WorkerId, lease.Key)
			}
		} else {
			if _, ok := l.heldLeases[lease.Key]; ok {
				l.Logger.Debugf("Worker %s lost lease with key %s", l.WorkerId, lease.Key)
			}
		}
	}
	l.Lock()
	l.heldLeases = heldLeases
	l.Unlock()
	// print held leases belongs to our worker.
	l.Logger.Debugf("Worker %s hold leases: %s", l.WorkerId, strings.Join(l.keys(), ", "))
	return nil
}

// Renew a lease by incrementing the lease counter.
// TODO: Add Conditional on the leaseCounter in DynamoDB matching the leaseCounter of the input
func (l *LeaseHolder) renewLease(lease *Lease) (err error) {
	lease.Counter++
	return l.manager.UpdateLease(lease)
}

// Returns copy of the current held leases.
func (l *LeaseHolder) GetHeldLeases() (list []*Lease) {
	l.Lock()
	defer l.Unlock()
	for _, lease := range l.heldLeases {
		copy := *lease
		list = append(list, &copy)
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
