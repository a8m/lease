package lease

import (
	"crypto/rand"
	"fmt"
	"sync"
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
	PutItem(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	UpdateItem(*dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(*dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error)
	CreateTable(*dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error)
}

// Backofface is the interface that holds the backoff strategy
type Backofface interface {
	Reset()
	Attempt() float64
	Duration() time.Duration
}

// Logger represents the desired API of both Logger and Entry.
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
	// Defaults to lease.Backoff with min value of time.Second and jitter
	// set to true.
	Backoff Backofface

	// The Amazon DynamoDB table name used for tracking leases.
	LeaseTable string

	// WorkerId used as a lease-owner.
	WorkerId string

	// ExpireAfter indicate how long lease unit can live without renovation
	// before expiration.
	// A worker which does not renew it's lease, will be regarded as having problems
	// and it's shards will be assigned to other workers. defaults to 10s.
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

	// Allow for some variance when calculating lease expirations. set to 25ms.
	epsilonMills time.Duration
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
		c.Backoff = &Backoff{
			b: &backoff.Backoff{
				Min:    time.Second,
				Jitter: true,
			}}
	}

	if c.LeaseTable == "" {
		c.Logger.Fatal("LeaseTable is required field")
	}

	c.epsilonMills = time.Millisecond * 25

	if c.ExpireAfter == 0 {
		c.ExpireAfter = time.Second * 10
	}
	if c.ExpireAfter < time.Second*10 {
		c.Logger.Fatal("ExpireAfter must be greater or equal to 10s")
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

// Backoff is the default thread-safe implemtation for Backofface
type Backoff struct {
	sync.Mutex
	b *backoff.Backoff
}

func (b *Backoff) Duration() time.Duration {
	b.Lock()
	defer b.Unlock()
	return b.b.Duration()
}

func (b *Backoff) Attempt() float64 {
	b.Lock()
	defer b.Unlock()
	return b.b.Attempt()
}

func (b *Backoff) Reset() {
	b.Lock()
	b.b.Reset()
	b.Unlock()
}
