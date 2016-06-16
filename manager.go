package leases

import (
	"fmt"
	"strconv"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

const (
	// Table schema
	LeaseKeyKey     = "leaseKey"
	LeaseOwnerKey   = "leaseOwner"
	LeaseCounterKey = "leaseCounter"

	// AWS exception
	AlreadyExist = "ResourceInUseException"

	// Max number of retries
	maxScanRetries   = 3
	maxCreateRetries = 3
	maxUpdateRetries = 2
)

// Manager wrap the basic operations for leases.
type Manager interface {
	// Creates the table that will store leases if it's not already exists.
	CreateLeaseTable() error

	// Update lease object state.
	UpdateLease(*Lease) error

	// List all leases(objects) in table.
	ListLeases() ([]*Lease, error)
}

// LeaseManager is the default implemntation of Manager
// that uses DynamoDB.
type LeaseManager struct {
	*Config
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

		if awsErr, ok := err.(awserr.RequestFailure); ok && awsErr.Code() == AlreadyExist {
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

// UpdateLease gets a lease and update it in the leasing table.
func (l *LeaseManager) UpdateLease(lease *Lease) (err error) {
	for l.Backoff.Attempt() < maxUpdateRetries {
		_, err = l.Client.UpdateItem(&dynamodb.UpdateItemInput{
			TableName: aws.String(l.LeaseTable),
			Key: map[string]*dynamodb.AttributeValue{
				LeaseKeyKey: {
					S: aws.String(lease.Key),
				},
			},
			ReturnValues: aws.String(dynamodb.ReturnValueAllNew),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":owner": {
					S: aws.String(lease.Owner),
				},
				":count": {
					N: aws.String(strconv.Itoa(lease.Counter)),
				},
			},
			UpdateExpression: aws.String(fmt.Sprintf(
				"SET %s = :owner, %s = :count",
				LeaseOwnerKey,
				LeaseCounterKey,
			)),
		})

		if err == nil {
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
			lease := new(Lease)
			if err := dynamodbattribute.UnmarshalMap(item, lease); err == nil {
				list = append(list, lease)
				lease.lastRenewal = time.Now()
			}
		}
		break
	}
	l.Backoff.Reset()
	return
}
