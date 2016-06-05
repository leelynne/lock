/* Package lock implements a distributed lock on top of dynamodb.
A lock can be acquired for a given node with a set expiration time.

The nodes using this package should be running clocks that are mostly in-sync, e.g. running NTP for the reasons listed below.

Usage:

	conf := &aws.Config{}
	db := dynamodb.New(session.New(), conf.WithRegion("us-west-2"))
	lk := lock.NewLock("testNode", lockTable, db)

	lockKey := fmt.Sprintf("test:key-%d", rand.Int63())

Split-brain possibilities:

Because dynamodb does not provide any time functions in its query language all times
originate from the nodes performing the locking. This can lead to issues if a node's notion
of time is out-of-sync with the others. For example for nodes a and b with node b's time set far ahead
of node a:

 - a.lock("event123", 250) a:200, b:250 - a locks 'event123' and thinks is has exclusive rights until time 250
 - b.lock("event123", 350) a:210, b:260 - b locks 'event'123 because for node b the lock as expired.  b now thinks it has exclusive until 350

To avoid split-brain issues:
 - only use this package on servers you control running NTP.
 - Don't rely on lock expirations granularity less than few a seconds.
 - Pad lock expiration times
*/
package lock

import (
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	tableKey      = "lock_key"
	expColumnName = "lease_expiration"
)

type Lock struct {
	tableName string
	nodeId    string // ID to use for locking
	db        *dynamodb.DynamoDB
}

func NewLock(nodeId, tableName string, db *dynamodb.DynamoDB) *Lock {
	return &Lock{
		tableName: tableName,
		nodeId:    nodeId,
		db:        db,
	}
}

// Lock attempts to grant exclusive access to the given key until the expiration.
// Lock will return false if the lock is currently held by another node otherwise true.
// A node can re-lock the same. A non-nil error means the lock was not granted.
func (l *Lock) Lock(key string, expiration time.Time) (locked bool, e error) {
	// Conditional put on item not present
	now := time.Now().UnixNano() / 1000
	nowString := strconv.FormatInt(now, 10)
	expString := strconv.FormatInt(expiration.UnixNano()/1000, 10)
	entryNotExist := fmt.Sprintf("attribute_not_exists(%s)", tableKey)
	owned := "nodeId = :nodeId"
	alreadyExpired := fmt.Sprintf(":now > %s", expColumnName)

	item := map[string]*dynamodb.AttributeValue{}
	item[tableKey] = &dynamodb.AttributeValue{S: aws.String(key)}
	item["nodeId"] = &dynamodb.AttributeValue{S: aws.String(l.nodeId)}
	item[expColumnName] = &dynamodb.AttributeValue{N: aws.String(expString)}
	req := &dynamodb.PutItemInput{
		Item:                item,
		ConditionExpression: aws.String(fmt.Sprintf("(%s) OR (%s) OR (%s)", entryNotExist, owned, alreadyExpired)),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":now":    &dynamodb.AttributeValue{N: aws.String(nowString)},
			":nodeId": &dynamodb.AttributeValue{S: aws.String(l.nodeId)},
		},
		TableName: aws.String(l.tableName),
	}
	_, err := l.db.PutItem(req)
	if err != nil {
		if awserr, ok := err.(awserr.Error); ok {
			if awserr.Code() == "ConditionalCheckFailedException" {
				// Locked is owned by someone else
				return false, nil
			}
		}
		return false, err
	}
	return true, nil
}

// Unlock removes the exclusive lock on this key.
func (l *Lock) Unlock(key string) error {
	entryNotExist := fmt.Sprintf("attribute_not_exists(%s)", tableKey)
	owned := "nodeId = :nodeId"

	dynamoKey := map[string]*dynamodb.AttributeValue{}
	dynamoKey[tableKey] = &dynamodb.AttributeValue{S: aws.String(key)}
	req := &dynamodb.DeleteItemInput{
		Key:                 dynamoKey,
		ConditionExpression: aws.String(fmt.Sprintf("(%s) OR (%s)", entryNotExist, owned)),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":nodeId": &dynamodb.AttributeValue{S: aws.String(l.nodeId)},
		},
		TableName: aws.String(l.tableName),
	}
	_, err := l.db.DeleteItem(req)
	if err != nil {
		if awserr, ok := err.(awserr.Error); ok {
			if awserr.Code() == "ConditionalCheckFailedException" {
				// Either the lock didn't exist or it's owned by someone else
				return fmt.Errorf("Key '%s' does not exist or is locked by another node.", key)
			} else {
				return err
			}
		} else {
			return err
		}
	}
	return nil
}
