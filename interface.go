package lease

import "time"

// Lease type contains data pertianing to a Lease.
// Distributed systems may use leases to partition work across a fleet of workers.
// Each unit of work/task identified by a leaseKey and has a corresponding Lease.
// Every worker will contend for all leases - only one worker will successfully take each one.
// The worker should hold the lease until it is ready to stop processing the corresponding unit of work,
// or until it fails.
// When the worker stops holding the lease, another worker will take and hold the lease.
type Lease struct {
	Key     string `dynamodbav:"leaseKey"`
	Owner   string `dynamodbav:"leaseOwner"`
	Counter int    `dynamodbav:"leaseCounter"`

	lastRenewal time.Time
}

// isExpired test if the lease renewal is expired from the given time.
func (l *Lease) isExpired(t time.Duration) bool {
	return time.Since(l.lastRenewal) > t
}

// hasNoOwner return true if the current owner is null.
func (l *Lease) hasNoOwner() bool {
	return l.Owner == "NULL" || l.Owner == ""
}

// Leaser is the interface that wraps the Coordinator methods.
type Leaser interface {
	Stop()
	Start() error
	GetLeases() []Lease
	Delete(Lease) error
	Create(Lease) (Lease, error)
}
