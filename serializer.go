package lease

import (
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type Serializer interface {
	// Decode convert the provided dynamodb item to Lease object.
	Decode(map[string]*dynamodb.AttributeValue) (*Lease, error)
	// Encode serializes the provided Lease object to dynamodb item.
	Encode(*Lease) (map[string]*dynamodb.AttributeValue, error)
}

// serializer implement the Serializer interface
type serializer struct {
	schemakeys []string
}

func newSerializer() Serializer {
	return &serializer{
		schemakeys: []string{LeaseKeyKey, LeaseOwnerKey, LeaseCounterKey},
	}
}

func (s *serializer) Decode(item map[string]*dynamodb.AttributeValue) (*Lease, error) {
	lease := new(Lease)
	if err := dynamodbattribute.UnmarshalMap(item, lease); err != nil {
		return nil, err
	}

	lease.lastRenewal = time.Now()
	lease.concurrencyToken, _ = uuid()

	// delete all the keys that belong to this package
	for _, k := range s.schemakeys {
		delete(item, k)
	}
	if len(item) > 0 {
		var (
			extrafields    = make(map[string]interface{})
			explicitfields = make(map[string]*dynamodb.AttributeValue)
		)
		// set the explicit fields
		for k, v := range item {
			if v.SS != nil || v.BS != nil || v.NS != nil {
				explicitfields[k] = v
				delete(item, k)
			}
		}
		// set the extra fields
		dynamodbattribute.ConvertFromMap(item, &extrafields)
		lease.extrafields = extrafields
		lease.explicitfields = explicitfields
	}
	return lease, nil
}

func (s *serializer) Encode(lease *Lease) (map[string]*dynamodb.AttributeValue, error) {
	item := map[string]*dynamodb.AttributeValue{
		LeaseKeyKey: {
			S: aws.String(lease.Key),
		},
		LeaseOwnerKey: {
			S: aws.String(lease.Owner),
		},
		LeaseCounterKey: {
			N: aws.String(strconv.Itoa(lease.Counter)),
		},
	}

	// make sure we remove the keys that belog to this package
	// and avoid unwanted behavior
	for _, k := range s.schemakeys {
		delete(lease.extrafields, k)
	}

	if len(lease.explicitfields) > 0 {
		for k, v := range lease.explicitfields {
			item[k] = v
		}
	}

	if len(lease.extrafields) > 0 {
		if fields, err := dynamodbattribute.MarshalMap(lease.extrafields); err != nil {
			return nil, err
		} else {
			for k, v := range fields {
				item[k] = v
			}
		}
	}

	return item, nil
}
