package lease

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	// Table schema
	LeaseKeyKey     = "leaseKey"
	LeaseOwnerKey   = "leaseOwner"
	LeaseCounterKey = "leaseCounter"

	// AWS exception
	AlreadyExist      = "ResourceInUseException"
	ConditionalFailed = "ConditionalCheckFailedException"

	// Max number of retries
	maxScanRetries   = 3
	maxCreateRetries = 3
	maxUpdateRetries = 2
	maxDeleteRetries = 2
)

// Manager wrap the basic operations for leases.
type Manager interface {
	// Creates the table that will store leases if it's not already exists.
	CreateLeaseTable() error

	// List all leases(objects) in table.
	ListLeases() ([]*Lease, error)

	// Renew a lease
	RenewLease(*Lease) error

	// Take a lease
	TakeLease(*Lease) error

	// Evict a lease
	EvictLease(*Lease) error

	// Delete a lease
	DeleteLease(*Lease) error

	// Create a lease
	CreateLease(*Lease) (*Lease, error)

	// Update a lease
	UpdateLease(*Lease) (*Lease, error)
}

// LeaseManager is the default implemntation of Manager
// that uses DynamoDB.
type LeaseManager struct {
	*Config
	Serializer Serializer
}

// CreateLeaseTable creates the table that will store the leases. succeeds
// if it's  already exists.
func (l *LeaseManager) CreateLeaseTable() (err error) {
	for l.Backoff.Attempt() < maxCreateRetries {
		_, err = l.Client.CreateTable(&dynamodb.CreateTableInput{
			TableName: aws.String(l.LeaseTable),
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String(LeaseKeyKey),
					AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
				},
			},
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String(LeaseKeyKey),
					KeyType:       aws.String("HASH"),
				},
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(int64(l.LeaseTableReadCap)),
				WriteCapacityUnits: aws.Int64(int64(l.LeaseTableWriteCap)),
			},
		})

		if err == nil {
			break
		}

		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == AlreadyExist {
			err = nil
			break
		}

		backoff := l.Backoff.Duration()

		l.Logger.WithFields(logrus.Fields{
			"backoff": backoff,
			"attempt": int(l.Backoff.Attempt()),
		}).Warnf("Worker %s failed to create table", l.WorkerId)

		time.Sleep(backoff)
	}
	l.Backoff.Reset()
	return
}

// Renew a lease by incrementing the lease counter.
// Conditional on the leaseCounter in DynamoDB matching the leaseCounter of the input
// Mutates the leaseCounter of the passed-in lease object after updating the record in DynamoDB.
func (l *LeaseManager) RenewLease(lease *Lease) (err error) {
	clease := *lease
	clease.Counter++
	if err = l.condUpdate(clease, *lease); err == nil {
		lease.Counter = clease.Counter
	}
	return
}

// Evict the current owner of lease by setting owner to null
// Conditional on the owner in DynamoDB matching the owner of the input.
// Mutates the lease owner of the passed-in lease object after updating the record in DynamoDB.
func (l *LeaseManager) EvictLease(lease *Lease) (err error) {
	clease := *lease
	clease.Owner = "NULL"
	if err = l.condUpdate(clease, *lease); err == nil {
		lease.Owner = clease.Owner
	}
	return
}

// Take a lease by incrementing its leaseCounter and setting its owner field.
// Conditional on the leaseCounter in DynamoDB matching the leaseCounter of the input
// Mutates the lease counter and owner of the passed-in lease object after updating the record in DynamoDB.
func (l *LeaseManager) TakeLease(lease *Lease) (err error) {
	clease := *lease
	clease.Counter++
	clease.Owner = l.WorkerId
	if err = l.condUpdate(clease, *lease); err == nil {
		lease.Owner = clease.Owner
		lease.Counter = clease.Counter
	}
	return
}

// ListLeasses returns all the lease units stored in the table.
func (l *LeaseManager) ListLeases() (list []*Lease, err error) {
	var res *dynamodb.ScanOutput
	for l.Backoff.Attempt() < maxScanRetries {
		res, err = l.Client.Scan(&dynamodb.ScanInput{
			TableName: aws.String(l.LeaseTable),
		})
		if err != nil {
			backoff := l.Backoff.Duration()

			l.Logger.WithFields(logrus.Fields{
				"backoff": backoff,
				"attempt": int(l.Backoff.Attempt()),
			}).Warnf("Worker %s failed to scan leases table", l.WorkerId)

			time.Sleep(backoff)
			continue
		}
		for _, item := range res.Items {
			if lease, err := l.Serializer.Decode(item); err != nil {
				l.Logger.WithError(err).Error("decode lease")
			} else {
				list = append(list, lease)
			}
		}
		break
	}
	l.Backoff.Reset()
	return
}

// Delete the given lease from DynamoDB. does nothing when passed a
// lease that does not exist in DynamoDB.
func (l *LeaseManager) DeleteLease(lease *Lease) (err error) {
	for l.Backoff.Attempt() < maxDeleteRetries {
		_, err = l.Client.DeleteItem(&dynamodb.DeleteItemInput{
			TableName: aws.String(l.LeaseTable),
			Key: map[string]*dynamodb.AttributeValue{
				LeaseKeyKey: {
					S: aws.String(lease.Key),
				},
			},
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":condOwner": {
					S: aws.String(lease.Owner),
				},
			},
			ExpressionAttributeNames: map[string]*string{
				"#owner": aws.String(LeaseOwnerKey),
				"#key":   aws.String(LeaseKeyKey),
			},
			ConditionExpression: aws.String("attribute_not_exists(#key) OR #owner = :condOwner"),
		})

		if err == nil {
			break
		}

		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == ConditionalFailed {
			break
		}

		backoff := l.Backoff.Duration()

		l.Logger.WithFields(logrus.Fields{
			"backoff": backoff,
			"attempt": int(l.Backoff.Attempt()),
		}).Warnf("Worker %s failed to delete lease", l.WorkerId)

		time.Sleep(backoff)
	}
	l.Backoff.Reset()
	return
}

// Create a new lease. conditional on a lease not already existing with different
// owner and counter.
func (l *LeaseManager) CreateLease(lease *Lease) (*Lease, error) {
	if lease.Owner == "" {
		lease.Owner = l.WorkerId
	}
	if lease.Counter == 0 {
		lease.Counter++
	}
	item, err := l.Serializer.Encode(lease)
	if err != nil {
		return lease, err
	}
	for l.Backoff.Attempt() < maxCreateRetries {
		_, err = l.Client.PutItem(&dynamodb.PutItemInput{
			TableName: aws.String(l.LeaseTable),
			Item:      item,
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":condOwner": {
					S: aws.String(lease.Owner),
				},
				":condCounter": {
					N: aws.String(strconv.Itoa(lease.Counter)),
				},
			},
			ExpressionAttributeNames: map[string]*string{
				"#counter": aws.String(LeaseCounterKey),
				"#owner":   aws.String(LeaseOwnerKey),
				"#key":     aws.String(LeaseKeyKey),
			},
			ConditionExpression: aws.String("attribute_not_exists(#key) OR #counter = :condCounter AND #owner = :condOwner"),
		})

		if err == nil {
			break
		}

		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == ConditionalFailed {
			break
		}

		backoff := l.Backoff.Duration()

		l.Logger.WithFields(logrus.Fields{
			"backoff": backoff,
			"attempt": int(l.Backoff.Attempt()),
		}).Warnf("Worker %s failed to create lease", l.WorkerId)

		time.Sleep(backoff)
	}

	l.Backoff.Reset()

	if err != nil {
		return nil, err
	}

	// the ReturnValues argument can only be ALL_OLD or NONE, it means that
	// our lease object is the most updated.
	return lease, nil
}

// UpdateLease used to update only the extra fields on the Lease object.
// With this method you will be able to update the task status, or any
// other fields.
// for example: {"status": "done", "last_update": "unix seconds"}
// To add extra fields on a Lease, use Lease.Set(key, val)
func (l *LeaseManager) UpdateLease(lease *Lease) (*Lease, error) {
	var (
		attExp     string
		attVal     map[string]*dynamodb.AttributeValue
		isReserved = func(w string) bool { return w == LeaseKeyKey || w == LeaseOwnerKey || w == LeaseCounterKey }
	)

	// set fields
	if len(lease.extrafields) > 0 || len(lease.explicitfields) > 0 {
		item, err := l.Serializer.Encode(lease)
		if err != nil {
			return lease, err
		}
		setExp := make([]string, 0)
		for k, v := range item {
			if !isReserved(k) {
				// if it's the first time we add entry to the map
				if attVal == nil {
					attVal = make(map[string]*dynamodb.AttributeValue)
				}
				setExp = append(setExp, fmt.Sprintf("%s = :%s", k, k))
				attVal[":"+k] = v
			}
		}
		if len(setExp) > 0 {
			attExp += "SET " + strings.Join(setExp, ", ")
		}
	}

	// remove fields
	if len(lease.removedfields) > 0 {
		rmExp := make([]string, 0)
		for _, f := range lease.removedfields {
			if !isReserved(f) {
				rmExp = append(rmExp, f)
			}
		}
		if len(rmExp) > 0 {
			attExp += " REMOVE " + strings.Join(rmExp, ", ")
		}
	}

	// if there's nothing to update
	if attExp == "" {
		return lease, nil
	}

	return l.updateLease(&dynamodb.UpdateItemInput{
		TableName: aws.String(l.LeaseTable),
		Key: map[string]*dynamodb.AttributeValue{
			LeaseKeyKey: {
				S: aws.String(lease.Key),
			},
		},
		UpdateExpression:          aws.String(attExp),
		ExpressionAttributeValues: attVal,
		ReturnValues:              aws.String(dynamodb.ReturnValueAllNew),
	})
}

// condLease gets a 2 Lease objects. the first one is for the update attributes
// and the second used to construct the condition expression.
func (l *LeaseManager) condUpdate(updateLease, condLease Lease) (err error) {
	updateInput := &dynamodb.UpdateItemInput{
		TableName: aws.String(l.LeaseTable),
		Key: map[string]*dynamodb.AttributeValue{
			LeaseKeyKey: {
				S: aws.String(updateLease.Key),
			},
		},
		ReturnValues: aws.String(dynamodb.ReturnValueAllNew),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":owner": {
				S: aws.String(updateLease.Owner),
			},
			":count": {
				N: aws.String(strconv.Itoa(updateLease.Counter)),
			},
		},
		UpdateExpression: aws.String(fmt.Sprintf(
			"SET %s = :owner, %s = :count",
			LeaseOwnerKey,
			LeaseCounterKey,
		)),
	}

	// add conditions only to veteran leases
	var (
		condExp string
		attrExp = make(map[string]*string)
	)
	if condLease.Counter > 0 {
		updateInput.ExpressionAttributeValues[":condCounter"] = &dynamodb.AttributeValue{
			N: aws.String(strconv.Itoa(condLease.Counter)),
		}
		attrExp["#counter"] = aws.String(LeaseCounterKey)
		condExp = ":condCounter = #counter"
	}
	if condLease.Owner != "" {
		updateInput.ExpressionAttributeValues[":condOwner"] = &dynamodb.AttributeValue{
			S: aws.String(condLease.Owner),
		}
		attrExp["#owner"] = aws.String(LeaseOwnerKey)
		if condExp != "" {
			condExp += " AND "
		}
		condExp += ":condOwner = #owner"
	}
	if condExp != "" {
		updateInput.ExpressionAttributeNames = attrExp
		updateInput.ConditionExpression = aws.String(condExp)
	}

	_, err = l.updateLease(updateInput)

	return
}

// updateLease gets updateInput and call Client.Update with the retries logic.
// use this method to reduce duplicate code.
// if the operation success we serialize the response and return the result.
func (l *LeaseManager) updateLease(input *dynamodb.UpdateItemInput) (*Lease, error) {
	var (
		err error
		out *dynamodb.UpdateItemOutput
	)
	for l.Backoff.Attempt() < maxUpdateRetries {
		out, err = l.Client.UpdateItem(input)

		if err == nil {
			break
		}

		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == ConditionalFailed {
			break
		}

		backoff := l.Backoff.Duration()

		l.Logger.WithFields(logrus.Fields{
			"backoff": backoff,
			"attempt": int(l.Backoff.Attempt()),
		}).Warnf("Worker %s failed to update lease", l.WorkerId)

		time.Sleep(backoff)
	}

	l.Backoff.Reset()

	if err != nil {
		return nil, err
	}

	return l.Serializer.Decode(out.Attributes)
}
