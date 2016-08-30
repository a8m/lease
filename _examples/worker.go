// simple worker implementation that loop "forever" and ask for
// jobs/tasks to handle
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

	leaser := lease.New(&lease.Config{
		Logger:     log,
		Client:     dynamodb.New(sess),
		LeaseTable: "lease-table-test",
	})

	// start taking leases
	err := leaser.Start()

	if err != nil {
		log.WithError(err).Fatal("start leaser")
	}

	go func() {
		for {
			// take tasks to handle,
			// or sleep for a while if there are no tasks to handle
			if tasks := leaser.GetHeldLeases(); len(tasks) > 0 {
				for _, task := range tasks {
					log.WithField("task name", task.Key).Info("start handling")
					// HANDLE YOUR TASK/JOB HERE
					time.Sleep(time.Second * 5)
					log.WithField("task name", task.Key).Info("handling finished")
				}
			} else {
				duration := time.Second * 10
				log.WithField("sleeping", duration).Info("there are no tasks to handle")
				time.Sleep(duration)
			}
		}
	}()

	time.Sleep(time.Minute * 5)
	// stop taking leases
	leaser.Stop()
}
