package distrlock

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/ptr"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/raito-io/go-dynamo-utils/distrlock/mocks"
)

func TestLock_TryLock_Success(t *testing.T) {
	// Given

	ctx := context.Background()

	tableName := "tableName"
	pkName := "pkName"
	pk := &types.AttributeValueMemberS{Value: "PK"}

	dynamodbClient := mocks.NewDynamodbClient(t)
	dynamodbClient.EXPECT().PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &tableName,
		Item: map[string]types.AttributeValue{
			pkName:               pk,
			attributeNameLockId:  &types.AttributeValueMemberS{Value: "UniqueID"},
			attributeNameTimeout: &types.AttributeValueMemberN{Value: "100000000"},
		},
		ConditionExpression:       aws.String("attribute_not_exists(#PK) OR #LockID = :lockid"),
		ExpressionAttributeNames:  map[string]string{"#LockID": attributeNameLockId, "#PK": pkName},
		ExpressionAttributeValues: map[string]types.AttributeValue{":lockid": &types.AttributeValueMemberS{Value: ""}},
	}).Return(nil, nil)

	handler := New(dynamodbClient, tableName, pkName, WithTimeout(time.Millisecond*100), MockIdGenerator(t, "UniqueID"))

	// When
	lock, success, err := handler.TryLock(ctx, pk)

	// Then
	require.Nil(t, err)
	require.True(t, success)
	require.NotNil(t, lock)

	require.Equal(t, &Lock{
		lockId:     "UniqueID",
		partition:  pk,
		repository: handler,
	}, lock)
}

func TestLock_TryLock_Failed(t *testing.T) {
	// Given

	ctx := context.Background()

	tableName := "tableName"
	pkName := "pkName"
	pk := &types.AttributeValueMemberS{Value: "PK"}

	dynamodbClient := mocks.NewDynamodbClient(t)
	dynamodbClient.EXPECT().PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &tableName,
		Item: map[string]types.AttributeValue{
			pkName:               pk,
			attributeNameLockId:  &types.AttributeValueMemberS{Value: "UniqueID"},
			attributeNameTimeout: &types.AttributeValueMemberN{Value: "100000000"},
		},
		ConditionExpression:       aws.String("attribute_not_exists(#PK) OR #LockID = :lockid"),
		ExpressionAttributeNames:  map[string]string{"#LockID": attributeNameLockId, "#PK": pkName},
		ExpressionAttributeValues: map[string]types.AttributeValue{":lockid": &types.AttributeValueMemberS{Value: ""}},
	}).Return(nil, fmt.Errorf("context of error: %w", &types.ConditionalCheckFailedException{Message: ptr.String("condition failed")}))

	handler := New(dynamodbClient, tableName, pkName, WithTimeout(time.Millisecond*100), MockIdGenerator(t, "UniqueID"))

	// When
	lock, success, err := handler.TryLock(ctx, pk)

	// Then
	require.Nil(t, err)
	require.False(t, success)
	require.Nil(t, lock)
}

func TestLock_TryLock_Error(t *testing.T) {
	// Given

	ctx := context.Background()

	tableName := "tableName"
	pkName := "pkName"
	pk := &types.AttributeValueMemberS{Value: "PK"}

	dynamodbClient := mocks.NewDynamodbClient(t)
	dynamodbClient.EXPECT().PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &tableName,
		Item: map[string]types.AttributeValue{
			pkName:               pk,
			attributeNameLockId:  &types.AttributeValueMemberS{Value: "UniqueID"},
			attributeNameTimeout: &types.AttributeValueMemberN{Value: "100000000"},
		},
		ConditionExpression:       aws.String("attribute_not_exists(#PK) OR #LockID = :lockid"),
		ExpressionAttributeNames:  map[string]string{"#LockID": attributeNameLockId, "#PK": pkName},
		ExpressionAttributeValues: map[string]types.AttributeValue{":lockid": &types.AttributeValueMemberS{Value: ""}},
	}).Return(nil, errors.New("boom"))

	handler := New(dynamodbClient, tableName, pkName, WithTimeout(time.Millisecond*100), MockIdGenerator(t, "UniqueID"))

	// When
	lock, success, err := handler.TryLock(ctx, pk)

	// Then
	require.Error(t, err)
	require.False(t, success)
	require.Nil(t, lock)
}

func TestLock_TryLock_WithSortKey(t *testing.T) {
	// Given

	ctx := context.Background()

	tableName := "tableName"
	pkName := "pkName"
	pk := &types.AttributeValueMemberS{Value: "PK"}

	dynamodbClient := mocks.NewDynamodbClient(t)
	dynamodbClient.EXPECT().PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &tableName,
		Item: map[string]types.AttributeValue{
			pkName:               pk,
			"SK":                 &types.AttributeValueMemberS{Value: "SortKeyId"},
			attributeNameLockId:  &types.AttributeValueMemberS{Value: "UniqueID"},
			attributeNameTimeout: &types.AttributeValueMemberN{Value: "100000000"},
		},
		ConditionExpression:       aws.String("attribute_not_exists(#SK) OR #LockID = :lockid"),
		ExpressionAttributeNames:  map[string]string{"#LockID": attributeNameLockId, "#SK": "SK"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":lockid": &types.AttributeValueMemberS{Value: ""}},
	}).Return(nil, nil)

	handler := New(dynamodbClient, tableName, pkName, WithTimeout(time.Millisecond*100),
		WithSortKey("SK"),
		WithSortKeyValue(&types.AttributeValueMemberS{Value: "SortKeyId"}),
		MockIdGenerator(t, "UniqueID"),
	)

	// When
	lock, success, err := handler.TryLock(ctx, pk)

	// Then
	require.Nil(t, err)
	require.True(t, success)
	require.NotNil(t, lock)

	require.Equal(t, &Lock{
		lockId:     "UniqueID",
		partition:  pk,
		repository: handler,
	}, lock)
}

func TestLock_Lock_Success(t *testing.T) {
	// Given

	ctx := context.Background()

	tableName := "tableName"
	pkName := "pkName"
	pk := &types.AttributeValueMemberS{Value: "PK"}

	dynamodbClient := mocks.NewDynamodbClient(t)
	dynamodbClient.EXPECT().PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &tableName,
		Item: map[string]types.AttributeValue{
			pkName:               pk,
			attributeNameLockId:  &types.AttributeValueMemberS{Value: "UniqueID"},
			attributeNameTimeout: &types.AttributeValueMemberN{Value: "100000000"},
		},
		ConditionExpression:       aws.String("attribute_not_exists(#PK) OR #LockID = :lockid"),
		ExpressionAttributeNames:  map[string]string{"#LockID": attributeNameLockId, "#PK": pkName},
		ExpressionAttributeValues: map[string]types.AttributeValue{":lockid": &types.AttributeValueMemberS{Value: ""}},
	}).Return(nil, fmt.Errorf("context of error: %w", &types.ConditionalCheckFailedException{Message: ptr.String("condition failed")})).Times(3)

	dynamodbClient.EXPECT().GetItem(mock.Anything, &dynamodb.GetItemInput{
		TableName:      &tableName,
		Key:            map[string]types.AttributeValue{pkName: pk},
		ConsistentRead: aws.Bool(true),
	}).Return(&dynamodb.GetItemOutput{
		Item: map[string]types.AttributeValue{
			pkName:               pk,
			attributeNameLockId:  &types.AttributeValueMemberS{Value: "AnotherLock"},
			attributeNameTimeout: &types.AttributeValueMemberN{Value: "30000000"},
		},
	}, nil).Times(3)

	dynamodbClient.EXPECT().PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &tableName,
		Item: map[string]types.AttributeValue{
			pkName:               pk,
			attributeNameLockId:  &types.AttributeValueMemberS{Value: "UniqueID"},
			attributeNameTimeout: &types.AttributeValueMemberN{Value: "100000000"},
		},
		ConditionExpression:       aws.String("attribute_not_exists(#PK) OR #LockID = :lockid"),
		ExpressionAttributeNames:  map[string]string{"#LockID": attributeNameLockId, "#PK": pkName},
		ExpressionAttributeValues: map[string]types.AttributeValue{":lockid": &types.AttributeValueMemberS{Value: "AnotherLock"}},
	}).Return(nil, nil).Once()

	handler := New(dynamodbClient, tableName, pkName, WithTimeout(time.Millisecond*100),
		WithRefreshInterval(time.Millisecond*10),
		MockIdGenerator(t, "UniqueID"),
	)

	// When
	lock, err := handler.Lock(ctx, pk)

	//Then
	require.NoError(t, err)
	require.Equal(t, &Lock{
		lockId:     "UniqueID",
		partition:  pk,
		repository: handler,
	}, lock)
}

func TestLock_Lock_Timeout(t *testing.T) {
	// Given

	ctx := context.Background()
	ctx, cancelFn := context.WithTimeout(ctx, time.Millisecond*100)
	defer cancelFn()

	tableName := "tableName"
	pkName := "pkName"
	pk := &types.AttributeValueMemberS{Value: "PK"}

	dynamodbClient := mocks.NewDynamodbClient(t)
	dynamodbClient.EXPECT().PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &tableName,
		Item: map[string]types.AttributeValue{
			pkName:               pk,
			attributeNameLockId:  &types.AttributeValueMemberS{Value: "UniqueID"},
			attributeNameTimeout: &types.AttributeValueMemberN{Value: "100000000"},
		},
		ConditionExpression:       aws.String("attribute_not_exists(#PK) OR #LockID = :lockid"),
		ExpressionAttributeNames:  map[string]string{"#LockID": attributeNameLockId, "#PK": pkName},
		ExpressionAttributeValues: map[string]types.AttributeValue{":lockid": &types.AttributeValueMemberS{Value: ""}},
	}).Return(nil, fmt.Errorf("context of error: %w", &types.ConditionalCheckFailedException{Message: ptr.String("condition failed")}))

	dynamodbClient.EXPECT().GetItem(mock.Anything, &dynamodb.GetItemInput{
		TableName:      &tableName,
		Key:            map[string]types.AttributeValue{pkName: pk},
		ConsistentRead: aws.Bool(true),
	}).Return(&dynamodb.GetItemOutput{
		Item: map[string]types.AttributeValue{
			pkName:               pk,
			attributeNameLockId:  &types.AttributeValueMemberS{Value: "AnotherLock"},
			attributeNameTimeout: &types.AttributeValueMemberN{Value: "300000000"},
		},
	}, nil)

	handler := New(dynamodbClient, tableName, pkName, WithTimeout(time.Millisecond*100),
		WithRefreshInterval(time.Millisecond*10),
		MockIdGenerator(t, "UniqueID"),
	)

	// When
	lock, err := handler.Lock(ctx, pk)

	//Then
	require.ErrorAs(t, err, &ErrTimeout)
	require.Nil(t, lock)
}

func TestLock_LockId(t *testing.T) {
	// Given

	l := Lock{
		lockId: "UniqueID",
	}

	// When
	id := l.LockId()

	// Then
	require.Equal(t, "UniqueID", id)
}

func TestLock_Unlock(t *testing.T) {
	// Given

	ctx := context.Background()

	tableName := "tableName"
	pkName := "pkName"
	pk := &types.AttributeValueMemberS{Value: "PK"}

	dynamodbClient := mocks.NewDynamodbClient(t)
	dynamodbClient.EXPECT().DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &tableName,
		Key: map[string]types.AttributeValue{
			pkName: pk,
		},
		ConditionExpression:       aws.String("#LockId = :lockId"),
		ExpressionAttributeNames:  map[string]string{"#LockId": attributeNameLockId},
		ExpressionAttributeValues: map[string]types.AttributeValue{":lockId": &types.AttributeValueMemberS{Value: "UniqueID"}},
	}).Return(nil, nil).Once()

	handler := New(dynamodbClient, tableName, pkName, WithTimeout(time.Millisecond*100))

	l := Lock{
		lockId:     "UniqueID",
		partition:  pk,
		repository: handler,
	}

	// When
	err := l.Release(ctx)

	// Then
	require.NoError(t, err)
}

func TestLock_TransactionCondition_Hash(t *testing.T) {
	//Given

	rh := RepositoryLockHandler{
		TableName:        "DynamoDbTable",
		PartitionKeyName: "PK",
	}

	lock := Lock{
		lockId:     "someLockId",
		partition:  &types.AttributeValueMemberS{Value: "Some PK"},
		repository: &rh,
	}

	//When
	condition := lock.TransactionCondition()

	//Then
	require.Equal(t, types.TransactWriteItem{
		ConditionCheck: &types.ConditionCheck{
			TableName: aws.String("DynamoDbTable"),
			Key: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "Some PK"},
			},
			ConditionExpression:                 aws.String("#LockId = :lockId"),
			ExpressionAttributeNames:            map[string]string{"#LockId": attributeNameLockId},
			ExpressionAttributeValues:           map[string]types.AttributeValue{":lockId": &types.AttributeValueMemberS{Value: "someLockId"}},
			ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureNone,
		},
	}, condition)
}

func TestLock_TransactionCondition_HashRange(t *testing.T) {
	//Given

	rh := RepositoryLockHandler{
		TableName:        "DynamoDbTable",
		PartitionKeyName: "PK",
		SortKeyName:      ptr.String("SK"),
		SortKeyValue:     SkString,
	}

	lock := Lock{
		lockId:     "someLockId",
		partition:  &types.AttributeValueMemberS{Value: "Some PK"},
		repository: &rh,
	}

	//When
	condition := lock.TransactionCondition()

	//Then
	require.Equal(t, types.TransactWriteItem{
		ConditionCheck: &types.ConditionCheck{
			TableName: aws.String("DynamoDbTable"),
			Key: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "Some PK"},
				"SK": SkString,
			},
			ConditionExpression:                 aws.String("#LockId = :lockId"),
			ExpressionAttributeNames:            map[string]string{"#LockId": attributeNameLockId},
			ExpressionAttributeValues:           map[string]types.AttributeValue{":lockId": &types.AttributeValueMemberS{Value: "someLockId"}},
			ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureNone,
		},
	}, condition)
}

func TestLock_TransactionWithRefresh_Hash(t *testing.T) {
	// Given

	idGenerator := mocks.NewIdGenerator(t)
	idGenerator.EXPECT().ID().Return("newLockId").Maybe()

	rh := RepositoryLockHandler{
		TableName:        "DynamoDbTable",
		PartitionKeyName: "PK",
		IdGenerator:      idGenerator,
	}

	lock := Lock{
		lockId:     "someLockId",
		partition:  &types.AttributeValueMemberS{Value: "Some PK"},
		repository: &rh,
	}

	// When
	writeItem, callbackFn := lock.TransactionWithRefresh()

	// Then
	require.Equal(t, types.TransactWriteItem{
		Update: &types.Update{
			TableName:                &rh.TableName,
			Key:                      map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "Some PK"}},
			ConditionExpression:      aws.String("#LockId = :lockId"),
			UpdateExpression:         aws.String("SET #LockId = :newLockId"),
			ExpressionAttributeNames: map[string]string{"#LockId": attributeNameLockId},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":lockId":    &types.AttributeValueMemberS{Value: "someLockId"},
				":newLockId": &types.AttributeValueMemberS{Value: "newLockId"},
			},
			ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureNone,
		},
	}, writeItem)

	transactResult := dynamodb.TransactWriteItemsOutput{ItemCollectionMetrics: map[string][]types.ItemCollectionMetrics{"Output": {}}}
	transactResultPassthrough, err := callbackFn(&transactResult, nil)

	require.NoError(t, err)
	require.Equal(t, &transactResult, transactResultPassthrough)
	require.Equal(t, "newLockId", lock.LockId())
}

func TestLock_TransactionWithRefresh_HashRange(t *testing.T) {
	// Given

	idGenerator := mocks.NewIdGenerator(t)
	idGenerator.EXPECT().ID().Return("newLockId").Maybe()

	rh := RepositoryLockHandler{
		TableName:        "DynamoDbTable",
		PartitionKeyName: "PK",
		SortKeyName:      ptr.String("SK"),
		SortKeyValue:     SkString,
		IdGenerator:      idGenerator,
	}

	lock := Lock{
		lockId:     "someLockId",
		partition:  &types.AttributeValueMemberS{Value: "Some PK"},
		repository: &rh,
	}

	// When
	writeItem, callbackFn := lock.TransactionWithRefresh()

	// Then
	require.Equal(t, types.TransactWriteItem{
		Update: &types.Update{
			TableName: &rh.TableName,
			Key: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "Some PK"},
				"SK": SkString,
			},
			ConditionExpression:      aws.String("#LockId = :lockId"),
			UpdateExpression:         aws.String("SET #LockId = :newLockId"),
			ExpressionAttributeNames: map[string]string{"#LockId": attributeNameLockId},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":lockId":    &types.AttributeValueMemberS{Value: "someLockId"},
				":newLockId": &types.AttributeValueMemberS{Value: "newLockId"},
			},
			ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureNone,
		},
	}, writeItem)

	transactResult := dynamodb.TransactWriteItemsOutput{ItemCollectionMetrics: map[string][]types.ItemCollectionMetrics{"Output": {}}}
	transactResultPassthrough, err := callbackFn(&transactResult, nil)

	require.NoError(t, err)
	require.Equal(t, &transactResult, transactResultPassthrough)
	require.Equal(t, "newLockId", lock.LockId())
}

func TestLock_Refresh_Success(t *testing.T) {
	// Given

	ctx := context.Background()

	tableName := "tableName"
	pkName := "pkName"
	pk := &types.AttributeValueMemberS{Value: "PK"}

	dynamodbClient := mocks.NewDynamodbClient(t)
	dynamodbClient.EXPECT().PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &tableName,
		Item: map[string]types.AttributeValue{
			pkName:               pk,
			attributeNameLockId:  &types.AttributeValueMemberS{Value: "UniqueID"},
			attributeNameTimeout: &types.AttributeValueMemberN{Value: "100000000"},
		},
		ConditionExpression:       aws.String("attribute_not_exists(#PK) OR #LockID = :lockid"),
		ExpressionAttributeNames:  map[string]string{"#LockID": attributeNameLockId, "#PK": pkName},
		ExpressionAttributeValues: map[string]types.AttributeValue{":lockid": &types.AttributeValueMemberS{Value: "existingLock"}},
	}).Return(nil, nil)

	handler := New(dynamodbClient, tableName, pkName, WithTimeout(time.Millisecond*100), MockIdGenerator(t, "UniqueID"))

	lock := Lock{
		lockId:     "existingLock",
		partition:  pk,
		repository: handler,
	}

	// When
	err := lock.Refresh(ctx)

	// Then
	require.NoError(t, err)
	require.Equal(t, "UniqueID", lock.lockId)
}

func TestLock_Refresh_Failed(t *testing.T) {
	// Given

	ctx := context.Background()

	tableName := "tableName"
	pkName := "pkName"
	pk := &types.AttributeValueMemberS{Value: "PK"}

	dynamodbClient := mocks.NewDynamodbClient(t)
	dynamodbClient.EXPECT().PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &tableName,
		Item: map[string]types.AttributeValue{
			pkName:               pk,
			attributeNameLockId:  &types.AttributeValueMemberS{Value: "UniqueID"},
			attributeNameTimeout: &types.AttributeValueMemberN{Value: "100000000"},
		},
		ConditionExpression:       aws.String("attribute_not_exists(#PK) OR #LockID = :lockid"),
		ExpressionAttributeNames:  map[string]string{"#LockID": attributeNameLockId, "#PK": pkName},
		ExpressionAttributeValues: map[string]types.AttributeValue{":lockid": &types.AttributeValueMemberS{Value: "existingLock"}},
	}).Return(nil, &types.ConditionalCheckFailedException{})

	handler := New(dynamodbClient, tableName, pkName, WithTimeout(time.Millisecond*100), MockIdGenerator(t, "UniqueID"))

	lock := Lock{
		lockId:     "existingLock",
		partition:  pk,
		repository: handler,
	}

	// When
	err := lock.Refresh(ctx)

	// Then
	require.ErrorAs(t, err, &ErrLockUpdate)
	require.Equal(t, "existingLock", lock.lockId)
}

func TestLock_Refresh_Error(t *testing.T) {
	// Given

	ctx := context.Background()

	tableName := "tableName"
	pkName := "pkName"
	pk := &types.AttributeValueMemberS{Value: "PK"}

	dynamodbClient := mocks.NewDynamodbClient(t)
	dynamodbClient.EXPECT().PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &tableName,
		Item: map[string]types.AttributeValue{
			pkName:               pk,
			attributeNameLockId:  &types.AttributeValueMemberS{Value: "UniqueID"},
			attributeNameTimeout: &types.AttributeValueMemberN{Value: "100000000"},
		},
		ConditionExpression:       aws.String("attribute_not_exists(#PK) OR #LockID = :lockid"),
		ExpressionAttributeNames:  map[string]string{"#LockID": attributeNameLockId, "#PK": pkName},
		ExpressionAttributeValues: map[string]types.AttributeValue{":lockid": &types.AttributeValueMemberS{Value: "existingLock"}},
	}).Return(nil, errors.New("boom"))

	handler := New(dynamodbClient, tableName, pkName, WithTimeout(time.Millisecond*100), MockIdGenerator(t, "UniqueID"))

	lock := Lock{
		lockId:     "existingLock",
		partition:  pk,
		repository: handler,
	}

	// When
	err := lock.Refresh(ctx)

	// Then
	require.Error(t, err)
	require.Equal(t, "existingLock", lock.lockId)
}

func MockIdGenerator(t *testing.T, id string) func(options *Options) {
	t.Helper()

	idGenerator := mocks.NewIdGenerator(t)
	idGenerator.EXPECT().ID().Return(id).Maybe()

	return func(options *Options) {
		options.IdGenerator = idGenerator
	}
}
