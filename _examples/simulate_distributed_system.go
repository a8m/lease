// simulate distributed system that uses leases to partition work across a
// fleet of workers.
//
// Work tasks should distribute evenly between workers.
//
// When worker stops holding a lease, another worker will take and hold it.
package main

import (
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/a8m/lease"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func main() {
	log := logrus.New()
	log.Level = logrus.DebugLevel

	sess := session.New(&aws.Config{
		Region: aws.String("us-east-1"),
	})

	// one dynamodb client
	client := dynamodb.New(sess)

	// leases creator
	leaseCreator := lease.New(&lease.Config{
		Logger:     log,
		Client:     client,
		LeaseTable: "lease-table-test",
	})

	// create four leases
	tasks := []string{"foo", "bar", "baz", "qux"}
	for _, task := range tasks {
		l, err := leaseCreator.Create(lease.Lease{Key: task})
		if err != nil {
			log.WithError(err).Error("create lease")
		} else {
			log.WithField("name", l.Key).Info("lease created")
		}
	}

	done1 := worker(client, log)
	done2 := worker(client, log)

	time.Sleep(time.Minute * 2)

	done1 <- struct{}{}
	<-done1
	log.Info("worker 1 stopped")

	time.Sleep(time.Minute * 2)

	done2 <- struct{}{}
	<-done2
	log.Info("worker 2 stopped2. exit...")
}

func worker(client *dynamodb.DynamoDB, log *logrus.Logger) chan struct{} {
	// exit/done channel
	done := make(chan struct{})

	// leases creator
	leaser := lease.New(&lease.Config{
		Logger:     log,
		Client:     client,
		LeaseTable: "lease-table-test",
	})

	// start taking leases
	err := leaser.Start()

	if err != nil {
		log.WithError(err).Fatal("start leaser")
	}

	go func() {
		tick := time.NewTicker(time.Second * 5)

		defer tick.Stop()
		defer close(done)

		for {
			select {
			case <-tick.C:
				// take tasks to handle,
				// or sleep for a while if there are no tasks to handle
				if tasks := leaser.GetLeases(); len(tasks) > 0 {
					for _, task := range tasks {
						log.WithField("task name", task.Key).Info("start handling")
						// HANDLE YOUR TASK/JOB HERE
						time.Sleep(time.Second * 30)
						log.WithField("task name", task.Key).Info("handling finished")
					}
				} else {
					log.WithField("sleeping", "5s").Info("there are no tasks to handle")
				}
			case <-done:
				leaser.Stop()
				return
			}
		}
		// stop taking leases
		leaser.Stop()
	}()

	return done
}
