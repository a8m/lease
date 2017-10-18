# Lease [![Build status][travis-image]][travis-url] [![License][license-image]][license-url] [![GoDoc][godoc-img]][godoc-url]
A generic leasing implementation of the [Amazon-KCL.lease][kcl.lease] package. ideal for manage
and partition work across a fleet of workers.

![Screenshot](https://github.com/a8m/lease/blob/master/assets/lease.jpg)

### What is a Lease ?
Lease type contains data pertianing to a lease.  
Distributed systems may use leases to partition work across a fleet of workers.  
Each unit of work (identified by a leaseKey) has a corresponding Lease.  
Every worker will contend for all leases - only one worker will successfully take each one.  
The worker should hold the lease until it is ready to stop processing the corresponding unit of work,
or until it fails.  
When the worker stops holding the lease, another worker will take and hold the lease.

To get started, see the [examples][examples]


### License
MIT

[examples]:      https://github.com/a8m/lease/tree/master/_examples
[kcl.lease]:     https://github.com/awslabs/amazon-kinesis-client/tree/master/src/main/java/com/amazonaws/services/kinesis/leases
[godoc-url]:     https://godoc.org/github.com/a8m/lease
[godoc-img]:     https://img.shields.io/badge/godoc-reference-blue.svg?style=flat-square
[travis-url]:    https://travis-ci.org/a8m/lease
[travis-image]:  https://img.shields.io/travis/a8m/lease.svg?style=flat-square
[license-url]:   LICENSE
[license-image]: https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square
