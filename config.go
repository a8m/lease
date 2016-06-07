package leases

import (
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Clientface is a thin methods set of DynamoDB.
type Clientface interface {
	Scan(*dynamodb.ScanInput) (*dynamodb.ScanOutput, error)
	UpdateItem(*dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error)
	CreateTable(*dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error)
}

type Config struct {
	// Client is a Clientface implemetation.
	Client Clientface

	// Working leasing table.
	Table string

	// WorkerId used as a lease-owner.
	OwnerId string

	// Logger is the logger used. defaults to logrus.Log
	Logger *logrus.Logger

	// ExpireAfter indicate how long lease unit can live without renovation
	// before expiration.
	// A worker which does not renew it's lease, will be regarded as having problems
	// and it's shards will be assigned to other workers. default to 5m.
	ExpireAfter time.Duration

	// Max leases to steal from another worker at one time (for load balancing).
	// Setting this to a higher number allow faster load convergence (e.g. during deployments, cold starts),
	// but can cause higher churn in the system
	MaxLeasesToStealAtOneTime int
}

func (c *Config) defaults() {
	if c.Logger == nil {
		c.Logger = logrus.New()
	}
	if c.Client == nil {
		c.Client = dynamodb.New(session.New(aws.NewConfig()))
	}
	if c.ExpireAfter == 0 {
		c.ExpireAfter = time.Minute * 5
	}
	if c.MaxLeasesToStealAtOneTime == 0 {
		c.MaxLeasesToStealAtOneTime = 1
	}
	falseOrPanic(c.MaxLeasesToStealAtOneTime < 0, "leases: MaxLeasesToStealAtOneTime should be greater or equal to 1")
	falseOrPanic(c.ExpireAfter < time.Minute, "leases: ExpireAfter must be greater or equal to 1m")
	falseOrPanic(c.OwnerId == "", "leases: OwnerId is required field")
	falseOrPanic(c.Table == "", "leases: Table is required field")
}

func falseOrPanic(p bool, msg string) {
	if p {
		panic(msg)
	}
}
