// simulate distributed system that uses leases to partition work across a
// fleet of workers.
//
// Each worker resonsible to 'create', 'delete' and 'handle' leases.
package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/a8m/lease"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Task status
const (
	TASK_STATUS = "taskStatus"
	CREATED     = iota
	PROGRESS_X
	PROGRESS_Y
	PROGRESS_Z
	DONE
)

func main() {
	log := logrus.New()
	log.Level = logrus.DebugLevel

	sess := session.New(&aws.Config{
		Region: aws.String("us-east-1"),
	})

	// create one dynamodb client
	client := dynamodb.New(sess)

	done1 := newWorker(client, log.WithField("instance", 1))
	done2 := newWorker(client, log.WithField("instance", 2))

	time.Sleep(time.Minute * 3)

	// termiate instance 1
	done1 <- struct{}{}
	// wait for graceful exit
	<-done1

	time.Sleep(time.Minute * 3)

	// termiate instance 2
	done2 <- struct{}{}
	<-done2

	log.Info("exit example")
}

// worker should represent a single ec2 instance in the system.
// each worker responsible to 'create' random leases, 'handle' and 'delete' the expired
func newWorker(client *dynamodb.DynamoDB, log lease.Logger) chan struct{} {
	// exit/done channel
	done := make(chan struct{})

	// lease
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
		tickHandle := time.NewTicker(time.Second * 10)
		// we want it immediately in the first iteration
		tickCreate := time.After(0)

		defer tickHandle.Stop()
		defer close(done)

		for {
			select {
			case <-tickHandle.C:
				// take tasks to handle,
				// or sleep for a while if there are no tasks to handle
				if tasks := leaser.GetHeldLeases(); len(tasks) > 0 {
					for _, task := range tasks {
						status, ok := task.Get(TASK_STATUS)
						// if this task handled successfully, remove it from the lease table
						if ok && status.(int) == DONE {
							log.WithField("expired task", task.Key).Info("deleting")
							if err := leaser.Delete(task); err != nil {
								log.WithField("task name", task.Key).WithError(err).Error("deleting failed")
							} else {
								log.WithField("task name", task.Key).Info("deleted successfully")
							}
							continue
						}
						log.WithField("task name", task.Key).Info("start handling")
						// -------------------------
						// HANDLE YOUR TASK/JOB HERE
						// -------------------------
						time.Sleep(time.Second)
						log.WithField("task name", task.Key).Info("handling finished")

						// set the status of the task
						taskStatus := PROGRESS_X
						// 'ok' means that the 'status' key exists on the map
						if ok {
							taskStatus = status.(int)
							// incrementing it by one may set it to DONE
							taskStatus++
						}

						// add metadata on the lease
						task.Set(TASK_STATUS, taskStatus)
						task.Set("last_update", time.Now().Unix())
						task.SetAs("results", []string{"200", "500", "404"}, lease.StringSet)
						if _, err := leaser.Update(task); err != nil {
							log.WithField("task name", task.Key).WithError(err).Error("update failed")
						} else {
							log.WithField("task name", task.Key).Info("updated successfully")
						}
					}
				}
			case <-tickCreate:
				// create 5 random tasks each tick
				for i := 1; i <= 5; i++ {
					task := lease.Lease{Key: fmt.Sprintf("task-%d-%d", i, rand.Intn(1e3))}
					task.Set("created_at", time.Now().Unix())
					task.Set(TASK_STATUS, CREATED)
					l, err := leaser.Create(task)
					if err != nil {
						log.WithError(err).Error("create lease")
					} else {
						log.WithField("name", l.Key).Info("lease created")
					}
				}
				tickCreate = time.After(time.Minute * 2)
			case <-done:
				leaser.Stop()
				return
			}
		}
	}()

	return done
}
