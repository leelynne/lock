// +build integration

package lock_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/leelynne/lock"
)

var lockTable = "prod.locks"

func TestLockBasics(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	conf := &aws.Config{}
	db := dynamodb.New(session.New(), conf.WithRegion("us-west-2"))
	lk := lock.NewLock("testNode", lockTable, db)

	lockKey := fmt.Sprintf("test:key-%d", rand.Int63())

	locked, err := lk.Lock(lockKey, time.Now().Add(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if !locked {
		t.Error("failed to lock first lock")
	}

	// Lock again
	locked, err = lk.Lock(lockKey, time.Now().Add(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if !locked {
		t.Fatal("failed to lock second lock")
	}

	// Attempt lock from another node
	otherLk := lock.NewLock("testNode2", lockTable, db)
	olock, err := otherLk.Lock(lockKey, time.Now().Add(10*time.Minute))
	if err != nil {
		t.Fatalf("Err attempting to lock from another node - %s", err.Error())
	}
	if olock {
		t.Fatal("Other node was able to aquire a locked key.")
	}

	err = lk.Unlock(lockKey)
	if err != nil {
		t.Error(err)
	}
	// Unlock again
	err = lk.Unlock(lockKey)
	if err != nil {
		t.Error(err)
	}
}

func TestLockExpiration(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	conf := &aws.Config{}
	db := dynamodb.New(session.New(), conf.WithRegion("us-west-2"))
	lk := lock.NewLock("testNode", lockTable, db)

	lockKey := fmt.Sprintf("test:key-%d", rand.Int63())

	// Lock with expiration in the past
	locked, err := lk.Lock(lockKey, time.Now().Add(-10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !locked {
		t.Fatal("failed to lock")
	}

	// Attempt lock from another node
	otherLk := lock.NewLock("testNode2", lockTable, db)
	olock, err := otherLk.Lock(lockKey, time.Now().Add(10*time.Minute))
	if err != nil {
		t.Errorf("Err attempting to lock from another node - %s", err.Error())
	}
	if !olock {
		t.Fatal("Unable to aquire lock even after it expired.")
	}

	// cleanup
	err = otherLk.Unlock(lockKey)
	if err != nil {
		t.Error(err)
	}
}
