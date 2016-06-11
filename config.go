package leases

import (
	"crypto/rand"
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/jpillora/backoff"
)

// Clientface is a thin methods set of DynamoDB.
type Clientface interface {
	Scan(*dynamodb.ScanInput) (*dynamodb.ScanOutput, error)
	UpdateItem(*dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error)
	CreateTable(*dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error)
}

// Backoff is interface to hold Backoff strategy
type Backoff interface {
	Reset()
	Attempt() float64
	Duration() time.Duration
}

// Logger represents the API of both Logger and Entry.
type Logger interface {
	WithFields(logrus.Fields) *logrus.Entry
	WithField(string, interface{}) *logrus.Entry
	WithError(error) *logrus.Entry
	Debug(...interface{})
	Info(...interface{})
	Error(...interface{})
	Fatal(...interface{})
	Debugf(string, ...interface{})
	Infof(string, ...interface{})
	Warnf(string, ...interface{})
}

type Config struct {
	// Client is a Clientface implemetation.
	Client Clientface

	// Logger is the logger used. defaults to log.Log
	Logger Logger

	// Backoff determines the backoff strategy for http failures.
	// Defaults to backoff.Backoff with min value of time.Second and jitter
	// set to true.
	Backoff Backoff

	// The Amazon DynamoDB table name used for tracking leases.
	LeaseTable string

	// WorkerId used as a lease-owner.
	WorkerId string

	// ExpireAfter indicate how long lease unit can live without renovation
	// before expiration.
	// A worker which does not renew it's lease, will be regarded as having problems
	// and it's shards will be assigned to other workers. defaults to 5m.
	ExpireAfter time.Duration

	// Max leases to steal from another worker at one time (for load balancing).
	// Setting this to a higher number allow faster load convergence (e.g. during deployments, cold starts),
	// but can cause higher churn in the system. defaults to 1.
	MaxLeasesToStealAtOneTime int

	// The Amazon DynamoDB table used for tracking leases will be provisioned with this read capacity.
	// Defaults to 10.
	LeaseTableReadCap int

	// The Amazon DynamoDB table used for tracking leases will be provisioned with this write capacity.
	// Defaults to 10.
	LeaseTableWriteCap int
}

// defaults for configuration.
func (c *Config) defaults() {
	if c.Logger == nil {
		c.Logger = logrus.New()
	}
	c.Logger = c.Logger.WithField("package", "leases")

	if c.Client == nil {
		c.Client = dynamodb.New(session.New(aws.NewConfig()))
	}

	if c.Backoff == nil {
		c.Backoff = &backoff.Backoff{
			Min:    time.Second,
			Jitter: true,
		}
	}

	if c.LeaseTable == "" {
		c.Logger.Fatal("LeaseTable is required field")
	}

	if c.ExpireAfter == 0 {
		c.ExpireAfter = time.Minute * 5
	}
	if c.ExpireAfter < time.Minute {
		c.Logger.Fatal("ExpireAfter must be greater or equal to 1m")
	}

	if c.MaxLeasesToStealAtOneTime == 0 {
		c.MaxLeasesToStealAtOneTime = 1
	}
	if c.MaxLeasesToStealAtOneTime < 0 {
		c.Logger.Fatal("MaxLeasesToStealAtOneTime should be greater than 0")
	}

	if c.LeaseTableReadCap == 0 {
		c.LeaseTableReadCap = 10
	}
	if c.LeaseTableReadCap < 0 {
		c.Logger.Fatal("LeaseTableReadCap must be greater than 0")
	}

	if c.LeaseTableWriteCap == 0 {
		c.LeaseTableWriteCap = 10
	}
	if c.LeaseTableWriteCap < 0 {
		c.Logger.Fatal("LeaseTableWriteCap must be greater than 0")
	}

	if c.WorkerId == "" {
		wid, err := uuid()
		if err != nil {
			c.Logger.Fatal("Failed to generate uuid. WorkerId is required field")
		}
		c.Logger.Infof("WorkerId does not provided in config. WorkerId is automatically assigned as: %s", wid)
		c.WorkerId = wid
	}
}

func uuid() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
