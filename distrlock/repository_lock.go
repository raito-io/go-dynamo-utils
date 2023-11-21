package distrlock

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/raito-io/go-dynamo-utils/utils"
)

var SkString = &types.AttributeValueMemberS{Value: "##LOCK##"}

const attributeNameLockId = "lockId"
const attributeNameTimeout = "timeout"

// Interface validation check
var _ DynamodbClient = (*dynamodb.Client)(nil)

//go:generate go run github.com/vektra/mockery/v2 --name=DynamodbClient --with-expecter
type DynamodbClient interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(options *dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(options *dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(options *dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

//go:generate go run github.com/vektra/mockery/v2 --name=IdGenerator --with-expecter
type IdGenerator interface {
	ID() string
}

// RepositoryLockHandler will handle locks distributed locks within a single dynamodb table
// The RepositoryLockHandler can create locks on hash tables and hash+range tables
type RepositoryLockHandler struct {
	Client           DynamodbClient
	TableName        string
	PartitionKeyName string
	SortKeyName      *string
	SortKeyValue     types.AttributeValue
	Timeout          time.Duration
	RefreshInterval  time.Duration
	RefreshVariance  time.Duration
	IdGenerator      IdGenerator
}

type Options struct {
	// SortKeyName if table has a sort key this value represent the sort key value
	SortKeyName *string

	// SortKeyDefault value
	SortKeyValue types.AttributeValue

	// Timeout stored in distributed lock
	Timeout *time.Duration

	// RefreshInterval average time between two lock poll request
	RefreshInterval *time.Duration

	// RefreshVariance variance of the refresh interval between two lock poll request
	RefreshVariance *time.Duration

	// IdGenerator a generator of unique IDs
	IdGenerator IdGenerator
}

// New create a new initialized distributed lock.
func New(client DynamodbClient, tableName string, partitionKeyName string, optFns ...func(options *Options)) *RepositoryLockHandler {
	options := Options{}
	for _, fn := range optFns {
		fn(&options)
	}

	repositoryLock := &RepositoryLockHandler{
		Client:           client,
		TableName:        tableName,
		PartitionKeyName: partitionKeyName,
		SortKeyName:      nil,
		SortKeyValue:     SkString,
		Timeout:          time.Second,
		RefreshInterval:  time.Millisecond * 200,
		RefreshVariance:  time.Millisecond * 20,
	}

	if options.SortKeyName != nil {
		repositoryLock.SortKeyName = options.SortKeyName
	}

	if options.SortKeyValue != nil {
		repositoryLock.SortKeyValue = options.SortKeyValue
	}

	if options.Timeout != nil {
		repositoryLock.Timeout = *options.Timeout
	}

	if options.RefreshInterval != nil {
		repositoryLock.RefreshInterval = *options.RefreshInterval
	}

	if options.RefreshVariance != nil {
		repositoryLock.RefreshVariance = *options.RefreshVariance
	}

	if options.IdGenerator != nil {
		repositoryLock.IdGenerator = options.IdGenerator
	} else {
		repositoryLock.IdGenerator = &utils.IdGenerator{}
	}

	return repositoryLock
}

// WithSortKey Specifies the sort key column if exists.
func WithSortKey(sortKeyColumnName string) func(options *Options) {
	return func(options *Options) {
		options.SortKeyName = &sortKeyColumnName
	}
}

// WithSortKeyValue Specifies the sort key value to use for a lock. This is required if the column hash a sort key that is not of they string.
// Note that the partition key + sort key must be unique within the table.
func WithSortKeyValue(sortKeyValue types.AttributeValue) func(options *Options) {
	return func(options *Options) {
		options.SortKeyValue = sortKeyValue
	}
}

// WithTimeout Specifies a timeout value set in the distributed lock. Default value is 1 second.
func WithTimeout(timeout time.Duration) func(options *Options) {
	return func(options *Options) {
		options.Timeout = &timeout
	}
}

// WithRefreshInterval Specifies a refresh interval that is used by the lock handler to poll if the lock is still active.
// Default value is 200ms
func WithRefreshInterval(refreshInterval time.Duration) func(options *Options) {
	return func(options *Options) {
		options.RefreshInterval = &refreshInterval
	}
}

// WithRefreshVariance Specifies a variance of the refresh interval that is used by the lock handler.
// Default value is 20ms.
func WithRefreshVariance(refreshVariance time.Duration) func(options *Options) {
	return func(options *Options) {
		options.RefreshVariance = &refreshVariance
	}
}

type Lock struct {
	repository *RepositoryLockHandler
	partition  types.AttributeValue
	lockId     string
}

// TryLock tries to lock a specified partition.
// If the handler was able to lock the partition a new lock will be returned. Additionally, the second return argument will be true
// If the handler was unable to lock the partition nil and false is returned as first arguments.
func (h *RepositoryLockHandler) TryLock(ctx context.Context, partition types.AttributeValue) (*Lock, bool, error) {
	lock, success, err := h.lock(ctx, partition, "")
	return lock, success, err
}

// Lock tries to lock a specified partition.
// The method will return a new lock once it is able to create a lock.
// If it was unable to create a new lock it will try after a RefreshInterval.
// Polling will stop if the context is Done.
func (h *RepositoryLockHandler) Lock(ctx context.Context, partition types.AttributeValue) (*Lock, error) {
	currentLockId := ""
	timeoutLock := ""
	var currentLocktimeout time.Time

	for {
		select {
		case <-ctx.Done():
			return nil, ErrTimeout
		default:
			if currentLocktimeout.Before(time.Now()) {
				timeoutLock = currentLockId
			}

			lock, success, err := h.lock(ctx, partition, timeoutLock)
			if err != nil {
				return nil, err
			}

			if success {
				return lock, nil
			}

			existingLockId, leaseDuration, err := h.lockLookup(ctx, partition)
			if err != nil {
				return nil, err
			}

			if *existingLockId != currentLockId {
				currentLockId = *existingLockId
				currentLocktimeout = time.Now().Add(*leaseDuration)
			}

			sleepContext(ctx, h.RefreshInterval, h.RefreshVariance)
		}
	}
}

func (h *RepositoryLockHandler) lock(ctx context.Context, partition types.AttributeValue, existingLockId string) (*Lock, bool, error) {
	generatedId := h.IdGenerator.ID()

	item := h.key(partition)
	item[attributeNameLockId] = &types.AttributeValueMemberS{Value: generatedId}
	item[attributeNameTimeout] = &types.AttributeValueMemberN{Value: strconv.FormatInt(h.Timeout.Nanoseconds(), 10)}

	var conditionExpression string
	expressionAttributeNames := map[string]string{"#LockID": attributeNameLockId}
	expressionAttributeValues := map[string]types.AttributeValue{":lockid": &types.AttributeValueMemberS{Value: existingLockId}}

	if h.hasSortKey() {
		conditionExpression = "attribute_not_exists(#SK) OR #LockID = :lockid"
		expressionAttributeNames["#SK"] = *h.SortKeyName
	} else {
		conditionExpression = "attribute_not_exists(#PK) OR #LockID = :lockid"
		expressionAttributeNames["#PK"] = h.PartitionKeyName
	}

	_, err := h.Client.PutItem(ctx, &dynamodb.PutItemInput{
		Item:                      item,
		TableName:                 &h.TableName,
		ConditionExpression:       &conditionExpression,
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
	})

	if err != nil {
		var conditionalCheckFailedException *types.ConditionalCheckFailedException
		if errors.As(err, &conditionalCheckFailedException) {
			return nil, false, nil
		}

		var transactionConflictException *types.TransactionConflictException
		if errors.As(err, &transactionConflictException) {
			return nil, false, nil
		}

		return nil, false, err
	}

	return &Lock{lockId: generatedId, partition: partition, repository: h}, true, nil
}

func (h *RepositoryLockHandler) lockLookup(ctx context.Context, partition types.AttributeValue) (*string, *time.Duration, error) {
	getItemResult, err := h.Client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      &h.TableName,
		Key:            h.key(partition),
		ConsistentRead: aws.Bool(true),
	})

	if err != nil {
		return nil, nil, err
	}

	if getItemResult != nil {
		lockKeyAttribute, found := getItemResult.Item[attributeNameLockId]
		if !found {
			// If no lockId is found, we assume that the lock is not active anymore
			return nil, nil, nil
		}

		lockKey, ok := lockKeyAttribute.(*types.AttributeValueMemberS)
		if !ok {
			return nil, nil, NewDistrLockError(fmt.Sprintf("attribute %s not of expected type AttributeValueMemberS but was %T", attributeNameLockId, lockKeyAttribute), nil)
		}

		timeoutNs, err := strconv.ParseInt(getItemResult.Item[attributeNameTimeout].(*types.AttributeValueMemberN).Value, 10, 64)
		if err != nil {
			return nil, nil, err
		}

		timeout := time.Duration(timeoutNs) * time.Nanosecond

		return &lockKey.Value, &timeout, nil
	}

	return nil, nil, nil
}

func (h *RepositoryLockHandler) key(partition types.AttributeValue) map[string]types.AttributeValue {
	key := map[string]types.AttributeValue{
		h.PartitionKeyName: partition,
	}

	if h.hasSortKey() {
		key[*h.SortKeyName] = h.SortKeyValue
	}

	return key
}

func (h *RepositoryLockHandler) hasSortKey() bool {
	return h.SortKeyName != nil && h.SortKeyValue != nil
}

// LockId returns the current id used by the lock
func (l *Lock) LockId() string {
	return l.lockId
}

// Release remove the lock in the database
func (l *Lock) Release(ctx context.Context) error {
	for {
		_, err := l.repository.Client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName:                 &l.repository.TableName,
			Key:                       l.key(),
			ConditionExpression:       aws.String("#LockId = :lockId"),
			ExpressionAttributeNames:  map[string]string{"#LockId": attributeNameLockId},
			ExpressionAttributeValues: map[string]types.AttributeValue{":lockId": &types.AttributeValueMemberS{Value: l.lockId}},
		})

		if err != nil {
			if errors.Is(err, &types.TransactionConflictException{}) {
				sleepContext(ctx, time.Millisecond*15, time.Millisecond*10)

				continue
			}

			return err
		}

		return nil
	}
}

// TransactionCondition returns a TransactWriteItem to validate if the lock is still active
func (l *Lock) TransactionCondition() types.TransactWriteItem {
	return types.TransactWriteItem{
		ConditionCheck: &types.ConditionCheck{
			TableName:                           &l.repository.TableName,
			Key:                                 l.key(),
			ConditionExpression:                 aws.String("#LockId = :lockId"),
			ExpressionAttributeNames:            map[string]string{"#LockId": attributeNameLockId},
			ExpressionAttributeValues:           map[string]types.AttributeValue{":lockId": &types.AttributeValueMemberS{Value: l.lockId}},
			ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureNone,
		},
	}
}

// TransactionWithRefresh returns a TransactWriteItem to validate if the lock is still active and refresh the lock if successful
// Note the callback function returned as second argument should be called with the return types of the TransactWriteItems call
func (l *Lock) TransactionWithRefresh() (types.TransactWriteItem, func(*dynamodb.TransactWriteItemsOutput, error) (*dynamodb.TransactWriteItemsOutput, error)) {
	generatedId := l.repository.IdGenerator.ID()

	return types.TransactWriteItem{
			Update: &types.Update{
				TableName:                           &l.repository.TableName,
				Key:                                 l.key(),
				ConditionExpression:                 aws.String("#LockId = :lockId"),
				UpdateExpression:                    aws.String("SET #LockId = :newLockId"),
				ExpressionAttributeNames:            map[string]string{"#LockId": attributeNameLockId},
				ExpressionAttributeValues:           map[string]types.AttributeValue{":lockId": &types.AttributeValueMemberS{Value: l.lockId}, ":newLockId": &types.AttributeValueMemberS{Value: generatedId}},
				ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureNone,
			},
		}, func(output *dynamodb.TransactWriteItemsOutput, err error) (*dynamodb.TransactWriteItemsOutput, error) {
			if err == nil {
				l.lockId = generatedId
			}

			return output, err
		}
}

// Refresh updates the timeout of the current active lock
func (l *Lock) Refresh(ctx context.Context) error {
	newLock, success, err := l.repository.lock(ctx, l.partition, l.lockId)
	if err != nil {
		return err
	}

	if !success {
		return ErrLockUpdate
	}

	l.lockId = newLock.lockId

	return nil
}

func (l *Lock) key() map[string]types.AttributeValue {
	return l.repository.key(l.partition)
}

func sleepContext(ctx context.Context, delay time.Duration, delayVariance time.Duration) {
	varianceMilliSecs := rand.Int63n(delayVariance.Milliseconds()*2) - delayVariance.Milliseconds()
	variance := time.Millisecond * time.Duration(varianceMilliSecs)

	select {
	case <-ctx.Done():
	case <-time.After(delay + variance):
	}
}
