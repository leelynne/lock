# lock
Package lock implements a distributed lock on top of dynamodb.
A lock can be acquired by a node/worker and with a set expiration time.

The nodes using this package should be running clocks that are mostly in-sync, e.g. running NTP for the reasons listed below.

Usage:
```go
db := dynamodb.New(session.New(), &aws.Config{})
locker := lock.Locker{
   TableName: "locks",
   TableKey: "lock_key",
   NodeID: "worker84",
}

locked, err := locker.Lock("event123", time.Now().Add(60 * time.Second))
// do stuff
node.Unlock("event123")
```

Split-brain possibilities:

Because dynamodb does not provide any time functions in its query language all times
originate from the nodes performing the locking. This can lead to issues if a node's notion
of time is out-of-sync with the others. For example for nodes a and b with node b's time set far ahead
of node a:

```
 a.lock("event123", 250) a time:200, b time:255 - a locks 'event123' and thinks is has exclusive rights until time 255 (55 ticks)

 b.lock("event123", 350) a time:210, b time:260 - b locks 'event123' because for node b the lock as expired.  b now thinks it has exclusive until 350
```

To avoid split-brain issues:
 * only use this package on servers you control running NTP.
 * Don't rely on lock expirations granularity less than few a seconds.
 * Pad lock expiration times.
