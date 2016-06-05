package lock

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var (
	ErrTimeout = errors.New("timeout")
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

// Lock attempts to grant exclusive access to the given key.
//
func (l *Lock) Lock(key string, leaseExpiration time.Time) (locked bool, e error) {
	// Conditional put on item not present
	now := time.Now().Unix()
	nowString := strconv.FormatInt(now, 10)
	leaseExpString := strconv.FormatInt(leaseExpiration.Unix(), 10)
	entryNotExist := fmt.Sprintf("attribute_not_exists(%s)", tableKey)
	owned := "nodeId = :nodeId"
	alreadyExpired := fmt.Sprintf(":now > %s", expColumnName)

	item := map[string]*dynamodb.AttributeValue{}
	item[tableKey] = &dynamodb.AttributeValue{S: aws.String(key)}
	item["nodeId"] = &dynamodb.AttributeValue{S: aws.String(l.nodeId)}
	item[expColumnName] = &dynamodb.AttributeValue{N: aws.String(leaseExpString)}
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

// Unlock
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
