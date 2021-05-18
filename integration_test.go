package lock

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var lockTable = "prod.locks"

func TestLockBasics(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	rand.Seed(time.Now().UnixNano())
	conf := &aws.Config{}
	db := dynamodb.New(session.New(), conf.WithRegion("us-west-2"))
	lk := &Locker{
		NodeID:    "testNode",
		TableName: lockTable,
		DB:        db,
	}

	lockKey := fmt.Sprintf("test:key-%d", rand.Int63())
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
	defer cancel()

	locked, err := lk.Lock(ctx, lockKey, time.Now().Add(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if !locked {
		t.Error("failed to lock first lock")
	}

	// Lock again
	locked, err = lk.Lock(ctx, lockKey, time.Now().Add(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if !locked {
		t.Fatal("failed to lock second lock")
	}

	// Attempt lock from another node
	otherLk := &Locker{
		NodeID:    "testNode2",
		TableName: lockTable,
		DB:        db,
	}
	olock, err := otherLk.Lock(ctx, lockKey, time.Now().Add(10*time.Minute))
	if err != nil {
		t.Fatalf("Err attempting to lock from another node - %s", err.Error())
	}
	if olock {
		t.Fatal("Other node was able to aquire a locked key.")
	}

	err = lk.Unlock(ctx, lockKey)
	if err != nil {
		t.Error(err)
	}
	// Unlock again
	err = lk.Unlock(ctx, lockKey)
	if err != nil {
		t.Error(err)
	}
}

func TestLockExpiration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	rand.Seed(time.Now().UnixNano())
	conf := &aws.Config{}
	db := dynamodb.New(session.New(), conf.WithRegion("us-west-2"))
	lk := &Locker{
		NodeID:    "testNode",
		TableName: lockTable,
		DB:        db,
	}

	lockKey := fmt.Sprintf("test:key-%d", rand.Int63())
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
	defer cancel()
	// Lock with expiration in the past
	locked, err := lk.Lock(ctx, lockKey, time.Now().Add(-10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !locked {
		t.Fatal("failed to lock")
	}

	// Attempt lock from another node
	otherLk := &Locker{
		NodeID:    "testNode2",
		TableName: lockTable,
		DB:        db,
	}
	olock, err := otherLk.Lock(ctx, lockKey, time.Now().Add(10*time.Minute))
	if err != nil {
		t.Errorf("Err attempting to lock from another node - %s", err.Error())
	}
	if !olock {
		t.Fatal("Unable to aquire lock even after it expired.")
	}

	// cleanup
	err = otherLk.Unlock(ctx, lockKey)
	if err != nil {
		t.Error(err)
	}
}
