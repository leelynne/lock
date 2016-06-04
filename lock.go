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
func (l *Lock) Lock(key string, leaseExpiration time.Time) (bool, error) {
	// Conditional put on item not present
	now := time.Now().Unix()
	nowString := strconv.FormatInt(now, 10)
	entryNotExist := fmt.Sprintf("attribute_not_exists(%s)", tableKey)
	alreadyExpired := fmt.Sprintf(":now > %s", expColumnName)

	item := map[string]*dynamodb.AttributeValue{}
	item[tableKey] = &dynamodb.AttributeValue{S: aws.String(key)}
	item["nodeId"] = &dynamodb.AttributeValue{S: aws.String(l.nodeId)}
	item[expColumnName] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(leaseExpiration.Unix(), 10))}
	req := &dynamodb.PutItemInput{
		Item:                item,
		ConditionExpression: aws.String(fmt.Sprintf("(%s) OR (%s)", entryNotExist, alreadyExpired)),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":now": &dynamodb.AttributeValue{N: aws.String(nowString)},
		},
		TableName: aws.String(l.tableName),
	}
	_, err := l.db.PutItem(req)
	if err != nil {
		if awserr, ok := err.(awserr.Error); ok {
			if awserr.Code() == "ConditionalCheckFailedException" {
				// Someone else has the lock
				return false, nil
			}
		}
		return false, err
	}
	return true, nil
}

func (l *Lock) Unlock(key string) error {
	return nil
}
