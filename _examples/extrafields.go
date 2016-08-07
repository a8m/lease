// simple worker that loops forever and ask for a tasks
// each time it finish a task, it updates 4 extra fields:
// runtime     - time taken to finish the task
// success     - indicates if the last task finished successfully
// last_update - last time this task updated; timestamp in unix seconds
// results     - set or list contains the last task result.
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
			if tasks := leaser.GetLeases(); len(tasks) > 0 {
				for _, task := range tasks {
					start := time.Now()
					log.WithField("task name", task.Key).Info("start handling")
					// HANDLE YOUR TASK/JOB HERE
					time.Sleep(time.Second * 5)
					log.WithField("task name", task.Key).Info("handling finished, update lease")
					// update different extra fields
					task.Set("runtime", time.Since(start))
					task.Set("success", true)
					// using SetAs will create this attribute as a string set;
					// use Set() if you want this attribute to be a list.
					task.SetAs("results", []string{"200", "500", "404"}, lease.StringSet)
					task.Set("last_update", time.Now().Unix())
					if _, err := leaser.Update(task); err != nil {
						log.WithError(err).Error("updating lease")
					}
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
