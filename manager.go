package leases

import (
	"fmt"
	"strconv"
	"time"

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

	// aws exceptions
	AlreadyExist = "ResourceInUseException"

	// number of retries on update operation.
	NumOfRetries = 3

	// time to sleep between retries
	RetrySleep = time.Second * 5
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

// LeaseManager is the default implemntation of Manager.
type LeaseManager struct {
	*Config
}

// CreateLeaseTable creates the tables if it's not already exists.
func (l *LeaseManager) CreateLeaseTable() error {
	_, err := l.Client.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(l.Table),
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
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
	})
	if err != nil {
		return nil
	}
	if awsErr, ok := err.(awserr.RequestFailure); ok && awsErr.Code() == AlreadyExist {
		return nil
	}
	return err
}

// UpdateLease gets a lease and update it in the leasing table.
func (l *LeaseManager) UpdateLease(lease *Lease) (err error) {
	for i := 0; i < NumOfRetries; i++ {
		_, err := l.Client.UpdateItem(&dynamodb.UpdateItemInput{
			TableName: aws.String(l.Table),
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
		} else if i-1 < NumOfRetries {
			l.Logger.WithError(err).Infof("Worker %s failed to update lease. sleep for 5s", l.OwnerId)
			time.Sleep(time.Second * 5)
		}
	}
	return err
}

// ListLeasses returns all the lease units stored in the table.
func (l *LeaseManager) ListLeases() (list []*Lease, err error) {
	for i := 0; i < NumOfRetries; i++ {
		res, err := l.Client.Scan(&dynamodb.ScanInput{
			TableName: aws.String(l.Table),
		})
		if err != nil {
			if i-1 < NumOfRetries {
				l.Logger.WithError(err).Infof("Worker %s failed to scan leases table. sleep for 5s", l.OwnerId)
				time.Sleep(RetrySleep)
				continue
			}
		} else {
			for _, item := range res.Items {
				lease := new(Lease)
				if err := dynamodbattribute.UnmarshalMap(item, lease); err == nil {
					list = append(list, lease)
					lease.lastRenewal = time.Now()
				}
			}
			break
		}
	}
	return list, err
}
