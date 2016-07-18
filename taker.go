package lease

import "math/rand"

// Taker is the interface that wraps the Take method.
// It  used by Coordinator to take new leases, or leases that other workers fail to renew.
// Each Coordinator instance corresponds to one worker and uses exactly one Taker to take
// leases for that worker.
type Taker interface {
	Take() error
}

// An implementation of Taker that uses DynamoDB via LeaseManager
type leaseTaker struct {
	*Config
	manager Manager

	// leaseTaker state
	allLeases map[string]*Lease
}

// Compute the set of leases available to be taken and attempt to take them. Lease taking process is:
//
// 1) If a lease's counter hasn't changed in long enough(i.e: "expired") set its owner to null.
// 2) Compute the "leases per worker" and the number we should take.
// 3) If we need to take leases, try to take expired leases. if there are no expired leases, consider stealing.
func (l *leaseTaker) Take() error {
	list, err := l.manager.ListLeases()
	if err != nil {
		return err
	}

	l.updateLeases(list)

	leaseCounts := l.computeLeaseCounts()
	numWorkers := len(leaseCounts)
	// assuming numLeases <= numWorkers
	target := 1
	// our target for each worker is numLeases / numWorkers (+1 if numWorkers doesn't evenly divide numLeases)
	if len(l.allLeases) > numWorkers {
		target = len(l.allLeases) / numWorkers
		if len(list)%numWorkers != 0 {
			target += 1
		}
	}

	myCount := leaseCounts[l.WorkerId]
	numToReachTarget := target - myCount

	if numToReachTarget <= 0 {
		l.Logger.Debugf("Worker %s does not need to take leases. we have %d, and the target is: %d",
			l.WorkerId,
			myCount,
			target)
		return nil
	}

	var leasesToTake []*Lease
	expiredLeases := l.getExpiredLeases()

	if len(expiredLeases) > 0 {
		// shuffle expiredLeases so workers don't all try to contend for the same leases.
		shuffle(expiredLeases)
		if numExpired := len(expiredLeases); numToReachTarget > numExpired {
			numToReachTarget = numExpired
		}
		leasesToTake = expiredLeases[:numToReachTarget]
	} else {
		l.Logger.Debugf("Worker %s needed %d leases but none were expired. consider stealing",
			l.WorkerId,
			numToReachTarget)
		leasesToTake = l.chooseLeasesToSteal(leaseCounts, numToReachTarget, target)
	}

	for _, lease := range leasesToTake {
		if err := l.manager.TakeLease(lease); err != nil {
			l.Logger.WithError(err).Debugf("Worker %s could not take lease with key %s.",
				l.WorkerId,
				lease.Key)
		} else {
			l.Logger.Debugf("Worker %s taked lease: %s successfully.", l.WorkerId, lease.Key)
		}
	}

	if len(leasesToTake) > 0 {
		l.Logger.Debugf("Worker %s saw %d total leases, %d available leases, %d workers.\n"+
			"Target is %d leases, I have %d leases, I plan to take %d leases, I will take %d leases",
			l.WorkerId,
			len(l.allLeases),
			len(expiredLeases),
			numWorkers,
			target,
			myCount,
			numToReachTarget,
			len(leasesToTake))
	}

	return nil
}

// Choose leases to steal by randomly selecting one or more (up to max) from the most loaded worker.
//
// Steal up to maxLeasesToStealAtOneTime leases from the most loaded worker if
// 1. he has > target leases and I need >= 1 leases : steal min(leases needed, maxLeasesToStealAtOneTime)
// 2. he has == target leases and I need > 1 leases : steal 1
func (l *leaseTaker) chooseLeasesToSteal(leaseCounts map[string]int, needed, target int) []*Lease {
	var mostLoadedWorker string
	// find the most loaded worker
	for worker, count := range leaseCounts {
		if mostLoadedWorker == "" || leaseCounts[mostLoadedWorker] < count {
			mostLoadedWorker = worker
		}
	}

	numLeasesToSteal := 0
	if count := leaseCounts[mostLoadedWorker]; count >= target {
		overTarget := count - target
		numLeasesToSteal = min(needed, overTarget)
		// steal 1 if we need > 1 and max loaded worker has target leases.
		if needed > 1 && numLeasesToSteal == 0 {
			numLeasesToSteal = 1
		}
		numLeasesToSteal = min(numLeasesToSteal, l.MaxLeasesToStealAtOneTime)
	}

	if numLeasesToSteal <= 0 {
		l.Logger.Debugf("Worker %s not stealing from most loaded worker %s.\n"+
			"He has %d, target is %d, and I need %d.",
			l.WorkerId,
			mostLoadedWorker,
			leaseCounts[mostLoadedWorker],
			target,
			needed)
		return nil
	} else {
		l.Logger.Debugf("Worker %s will attempt to steal %d leases from most loaded worker %s.\n"+
			"He has %d leases, target is %d, and I need %d.",
			l.WorkerId,
			numLeasesToSteal,
			mostLoadedWorker,
			leaseCounts[mostLoadedWorker],
			target,
			needed)
	}

	var candidates []*Lease
	for _, lease := range l.allLeases {
		if lease.Owner == mostLoadedWorker {
			candidates = append(candidates, lease)
		}
	}
	shuffle(candidates)

	return candidates[:numLeasesToSteal]
}

// Scan all leases and update lastRenewalTime. Add new leases and delete old leases.
func (l *leaseTaker) updateLeases(list []*Lease) {
	allLeases := make(map[string]*Lease)
	for _, newLease := range list {
		// if we've seen this lease before.
		if oldLease, ok := l.allLeases[newLease.Key]; ok {
			// and the counter has changed, set lastRenewal to the time of the scan.
			if oldLease.Counter != newLease.Counter {
				allLeases[oldLease.Key] = newLease
			} else {
				if oldLease.isExpired(l.ExpireAfter) {
					// in some cases that "other" worker evict this lease
					// and set his owner to NULL
					oldLease.Owner = newLease.Owner
					if err := l.manager.EvictLease(oldLease); err != nil {
						l.Logger.WithError(err).Warnf("Worker %s failed to evict lease with key %s",
							l.WorkerId,
							newLease.Key)
					}
				}
				allLeases[oldLease.Key] = oldLease
			}
		} else {
			allLeases[newLease.Key] = newLease
		}
	}
	l.allLeases = allLeases
}

// Get list of leases that were expired as of our last scan.
func (l *leaseTaker) getExpiredLeases() (list []*Lease) {
	for _, lease := range l.allLeases {
		if lease.isExpired(l.ExpireAfter) || lease.hasNoOwner() {
			list = append(list, lease)
		}
	}
	return
}

// Compute the number of leases I should try to take based on the state of the system.
func (l *leaseTaker) computeLeaseCounts() map[string]int {
	m := make(map[string]int)
	for _, lease := range l.allLeases {
		if lease.hasNoOwner() {
			continue
		}
		if _, ok := m[lease.Owner]; ok {
			m[lease.Owner]++
		} else {
			m[lease.Owner] = 1
		}
	}

	// If I have no leases, I wasn't represented in leaseCounts. Let's fix that.
	if _, ok := m[l.WorkerId]; !ok {
		m[l.WorkerId] = 0
	}

	return m
}

// shuffle list of leases
func shuffle(list []*Lease) {
	for i := range list {
		j := rand.Intn(i + 1)
		list[i], list[j] = list[j], list[i]
	}
}

// simple min function implemetation.
// the standard library accept float64. I want to ignore casting + reduce binary size.
func min(i, j int) int {
	if i > j {
		return j
	}
	return i
}
