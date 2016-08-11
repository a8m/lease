package lease

import (
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// AttributeType used to explicitly set the DynamoDB data type
// when setting an extra field on a lease object.
type AttributeType int

const (
	// StringSet is a string set data type
	StringSet AttributeType = iota
	// NumberSet is a  number set data type
	NumberSet
	// BinarySet is a binary set data type
	BinarySet
)

var (
	// ErrTokenNotMatch and ErrLeaseNotHeld could be returns only on the Update() call.
	//
	// If the concurrency token of the passed-in lease doesn't match the
	// concurrency token of the authoritative lease, it means the lease was
	// lost and regained between when the caller acquired his concurrency
	// token and when the caller called update.
	ErrTokenNotMatch = errors.New("leaser: concurrency token doesn't match the authoritative lease")
	// ErrLeaseNotHeld error will be returns only if the passed-in lease object
	// does not held be this  worker.
	ErrLeaseNotHeld = errors.New("leaser: worker does not hold the passed-in lease object")
	// ErrValueNotMatch error will be returns only if you tring to set an extra field on
	// a lease object using the SetAs method and the field value does not match the field
	// type.
	// for example: StringSet type excepts only []string{...}
	ErrValueNotMatch = errors.New("leaser: field value does not match the field type")
)

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

	// lastRenewal is used by LeaseTaker to track the last time a lease counter was incremented.
	// It is deliberately not persisted in DynamoDB.
	lastRenewal time.Time
	// concurrencyToken is used to prevent updates to leases that we have lost and re-acquired.
	// It is deliberately not persisted in DynamoDB.
	concurrencyToken string
	// extrafields holds all the fields that not belong to this package.
	extrafields map[string]interface{}
	// explicitfields holds all the fields that set using SetAs method
	explicitfields map[string]*dynamodb.AttributeValue
	// removed attributes; used to create the update expression.
	removedfields []string
}

// NewLease gets a key(represents the lease key/name) and returns a new Lease object.
func NewLease(key string) Lease {
	return Lease{Key: key}
}

// Set extra field to the Lease object before you create or update it
// using the Leaser.
//
// Use this method to add meta-data on the lease. for example:
//
//    lease.Set("success", true)
//    lease.Set("checkpoint", 35465786912)
func (l *Lease) Set(key string, val interface{}) {
	if l.extrafields == nil {
		l.extrafields = make(map[string]interface{})
	}
	l.extrafields[key] = val
	// make sure that this key does not exists in the explicit fields map
	delete(l.explicitfields, key)
}

// SetAs is like the Set method, but with another argument "typ" that explicitly
// sets the DynamoDB data type.
//
// For example:
//
//    Set("key", []string{"foo", "bar"})               // add this field as a list
//    SetAs("key", []string{"foo", "bar"}, StringSet)  // add this field as a string set
//
// Error will be returns only if the field value does not match the field type.
func (l *Lease) SetAs(key string, val interface{}, typ AttributeType) error {
	if l.explicitfields == nil {
		l.explicitfields = make(map[string]*dynamodb.AttributeValue)
	}
	ok := false
	switch typ {
	case StringSet, NumberSet:
		var ss []string
		if ss, ok = val.([]string); ok {
			v := &dynamodb.AttributeValue{
				SS: aws.StringSlice(ss),
			}
			if typ == NumberSet {
				v = &dynamodb.AttributeValue{
					NS: aws.StringSlice(ss),
				}
			}
			l.explicitfields[key] = v
		}
	case BinarySet:
		var bs [][]byte
		if bs, ok = val.([][]byte); ok {
			l.explicitfields[key] = &dynamodb.AttributeValue{
				BS: bs,
			}
		}
	}
	if !ok {
		return ErrValueNotMatch
	}
	// make sure that this key does not exists in the extra fields map
	delete(l.extrafields, key)
	return nil
}

// Get extra field from the Lease object that not belongs to this package.
func (l *Lease) Get(key string) (interface{}, bool) {
	if val, ok := l.extrafields[key]; ok {
		return val, ok
	}
	if val, ok := l.explicitfields[key]; ok {
		var ret interface{}
		if val.NS != nil {
			ret = aws.StringValueSlice(val.NS)
		} else if val.SS != nil {
			ret = aws.StringValueSlice(val.SS)
		} else {
			ret = val.BS
		}
		return ret, ok
	}
	return nil, false
}

// Del deletes extra field of the lease object.
func (l *Lease) Del(key string) {
	var ok bool
	if _, ok = l.extrafields[key]; ok {
		delete(l.extrafields, key)
	} else if _, ok = l.explicitfields[key]; ok {
		delete(l.explicitfields, key)
	}
	if ok {
		l.removedfields = append(l.removedfields, key)
	}
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
	Update(Lease) (Lease, error)
}
