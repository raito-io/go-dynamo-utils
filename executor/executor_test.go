package executor

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"dynamodb_utils/executor/mocks"
)

type ElementStruct struct {
	PK         string `dynamodbav:"PK"`
	SK         string `dynamodbav:"SK"`
	Attribute1 string `dynamodbav:"attr1,omitempty"`
	Attribute2 int    `dynamodbav:"attr2,omitempty"`
}

func TestExecutor_Query(t *testing.T) {

	ctx := context.Background()
	ctx, cancelFn := context.WithCancel(ctx)

	tableName := "tablename"

	items := []ElementStruct{
		{
			PK:         "PK1",
			SK:         "SK1",
			Attribute1: "strAtr1",
			Attribute2: 1,
		},
		{
			PK:         "PK1",
			SK:         "SK2",
			Attribute1: "strAtr2",
			Attribute2: 2,
		},
		{
			PK:         "PK1",
			SK:         "SK3",
			Attribute1: "strAtr3",
			Attribute2: 3,
		},
		{
			PK:         "PK1",
			SK:         "SK4",
			Attribute1: "strAtr4",
			Attribute2: 4,
		},
	}

	lastEvaluatedKey := map[string]types.AttributeValue{
		"PK": &types.AttributeValueMemberS{Value: "PK"},
		"SK": &types.AttributeValueMemberS{Value: "SK2"},
	}

	initialQuery := dynamodb.QueryInput{
		TableName:                 &tableName,
		KeyConditionExpression:    aws.String("#PK = :pk"),
		ExpressionAttributeNames:  map[string]string{"#PK": "PK"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": &types.AttributeValueMemberS{Value: "SomePK"}},
	}

	dynamodbClientMock := mocks.NewDynamodbClient(t)
	dynamodbClientMock.EXPECT().Query(ctx, &initialQuery).Return(&dynamodb.QueryOutput{Items: marshalElements(t, items[0:2]), LastEvaluatedKey: lastEvaluatedKey}, nil).Once()

	dynamodbClientMock.EXPECT().Query(ctx, &dynamodb.QueryInput{
		TableName:                 &tableName,
		KeyConditionExpression:    aws.String("#PK = :pk"),
		ExpressionAttributeNames:  map[string]string{"#PK": "PK"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": &types.AttributeValueMemberS{Value: "SomePK"}},
		ExclusiveStartKey:         lastEvaluatedKey,
	}).Return(&dynamodb.QueryOutput{Items: marshalElements(t, items[2:4])}, nil).Once()

	lock := mocks.NewLock(t)
	lock.EXPECT().Refresh(ctx).Return(nil).Twice()

	executor := NewExecutor(dynamodbClientMock)

	// When
	outputChannel := executor.Query(ctx, &initialQuery, WithUnmarhshalToItemMapFn[ElementStruct](), WithLock(lock))

	// Then
	requireChannelWithData(t, cancelFn, items, outputChannel)
}

func TestExecutor_Query_NoMapping(t *testing.T) {

	ctx := context.Background()
	ctx, cancelFn := context.WithCancel(ctx)

	tableName := "tablename"

	items := marshalElements(t, []ElementStruct{
		{
			PK:         "PK1",
			SK:         "SK1",
			Attribute1: "strAtr1",
			Attribute2: 1,
		},
		{
			PK:         "PK1",
			SK:         "SK2",
			Attribute1: "strAtr2",
			Attribute2: 2,
		},
		{
			PK:         "PK1",
			SK:         "SK3",
			Attribute1: "strAtr3",
			Attribute2: 3,
		},
		{
			PK:         "PK1",
			SK:         "SK4",
			Attribute1: "strAtr4",
			Attribute2: 4,
		},
	})

	lastEvaluatedKey := map[string]types.AttributeValue{
		"PK": &types.AttributeValueMemberS{Value: "PK"},
		"SK": &types.AttributeValueMemberS{Value: "SK2"},
	}

	initialQuery := dynamodb.QueryInput{
		TableName:                 &tableName,
		KeyConditionExpression:    aws.String("#PK = :pk"),
		ExpressionAttributeNames:  map[string]string{"#PK": "PK"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": &types.AttributeValueMemberS{Value: "SomePK"}},
	}

	dynamodbClientMock := mocks.NewDynamodbClient(t)
	dynamodbClientMock.EXPECT().Query(ctx, &initialQuery).Return(&dynamodb.QueryOutput{Items: items[0:2], LastEvaluatedKey: lastEvaluatedKey}, nil).Once()

	dynamodbClientMock.EXPECT().Query(ctx, &dynamodb.QueryInput{
		TableName:                 &tableName,
		KeyConditionExpression:    aws.String("#PK = :pk"),
		ExpressionAttributeNames:  map[string]string{"#PK": "PK"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": &types.AttributeValueMemberS{Value: "SomePK"}},
		ExclusiveStartKey:         lastEvaluatedKey,
	}).Return(&dynamodb.QueryOutput{Items: items[2:4]}, nil).Once()

	executor := NewExecutor(dynamodbClientMock)

	// When
	outputChannel := executor.Query(ctx, &initialQuery)

	// Then
	requireChannelWithData(t, cancelFn, items, outputChannel)
}

func TestExecutor_Scan(t *testing.T) {

	ctx := context.Background()
	ctx, cancelFn := context.WithCancel(ctx)

	tableName := "tablename"

	items := []ElementStruct{
		{
			PK:         "PK1",
			SK:         "SK1",
			Attribute1: "strAtr1",
			Attribute2: 1,
		},
		{
			PK:         "PK1",
			SK:         "SK2",
			Attribute1: "strAtr2",
			Attribute2: 2,
		},
		{
			PK:         "PK1",
			SK:         "SK3",
			Attribute1: "strAtr3",
			Attribute2: 3,
		},
		{
			PK:         "PK1",
			SK:         "SK4",
			Attribute1: "strAtr4",
			Attribute2: 4,
		},
	}

	lastEvaluatedKey := map[string]types.AttributeValue{
		"PK": &types.AttributeValueMemberS{Value: "PK"},
		"SK": &types.AttributeValueMemberS{Value: "SK2"},
	}

	initialQuery := dynamodb.ScanInput{
		TableName: &tableName,
	}

	dynamodbClientMock := mocks.NewDynamodbClient(t)
	dynamodbClientMock.EXPECT().Scan(ctx, &initialQuery).Return(&dynamodb.ScanOutput{Items: marshalElements(t, items[0:2]), LastEvaluatedKey: lastEvaluatedKey}, nil).Once()

	dynamodbClientMock.EXPECT().Scan(ctx, &dynamodb.ScanInput{
		TableName:         &tableName,
		ExclusiveStartKey: lastEvaluatedKey,
	}).Return(&dynamodb.ScanOutput{Items: marshalElements(t, items[2:4])}, nil).Once()

	executor := NewExecutor(dynamodbClientMock)

	// When
	outputChannel := executor.Scan(ctx, &initialQuery, WithUnmarhshalToItemMapFn[ElementStruct]())

	// Then
	requireChannelWithData(t, cancelFn, items, outputChannel)
}

func TestExecutor_Scan_NoMapping(t *testing.T) {

	ctx := context.Background()
	ctx, cancelFn := context.WithCancel(ctx)

	tableName := "tablename"

	items := marshalElements(t, []ElementStruct{
		{
			PK:         "PK1",
			SK:         "SK1",
			Attribute1: "strAtr1",
			Attribute2: 1,
		},
		{
			PK:         "PK1",
			SK:         "SK2",
			Attribute1: "strAtr2",
			Attribute2: 2,
		},
		{
			PK:         "PK1",
			SK:         "SK3",
			Attribute1: "strAtr3",
			Attribute2: 3,
		},
		{
			PK:         "PK1",
			SK:         "SK4",
			Attribute1: "strAtr4",
			Attribute2: 4,
		},
	})

	lastEvaluatedKey := map[string]types.AttributeValue{
		"PK": &types.AttributeValueMemberS{Value: "PK"},
		"SK": &types.AttributeValueMemberS{Value: "SK2"},
	}

	initialQuery := dynamodb.ScanInput{
		TableName: &tableName,
	}

	dynamodbClientMock := mocks.NewDynamodbClient(t)
	dynamodbClientMock.EXPECT().Scan(ctx, &initialQuery).Return(&dynamodb.ScanOutput{Items: items[0:2], LastEvaluatedKey: lastEvaluatedKey}, nil).Once()

	dynamodbClientMock.EXPECT().Scan(ctx, &dynamodb.ScanInput{
		TableName:         &tableName,
		ExclusiveStartKey: lastEvaluatedKey,
	}).Return(&dynamodb.ScanOutput{Items: items[2:4]}, nil).Once()

	executor := NewExecutor(dynamodbClientMock)

	// When
	outputChannel := executor.Scan(ctx, &initialQuery)

	// Then
	requireChannelWithData(t, cancelFn, items, outputChannel)
}

func TestExecute_ErrorOnExecute(t *testing.T) {
	// Given
	ctx := context.Background()
	ctx, cancelFn := context.WithCancel(ctx)

	operation := dynamodb.QueryInput{}
	executeFn := func(ctx context.Context, input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		return nil, errors.New("boom")
	}

	// When
	outputChannel := execute(ctx, &operation, nil, executeFn, nil, nil)

	// Then
	requireChannelWithData(t, cancelFn, []error{errors.New("boom")}, outputChannel)
}

func marshalElements(t *testing.T, items []ElementStruct) []map[string]types.AttributeValue {
	t.Helper()

	marshalledItems := make([]map[string]types.AttributeValue, 0, len(items))
	for i := range items {
		mashalledItem, err := attributevalue.MarshalMap(items[i])
		require.NoError(t, err)

		marshalledItems = append(marshalledItems, mashalledItem)

	}

	return marshalledItems
}

func requireChannelWithData[T any](t *testing.T, cancelFn func(), expected []T, channel chan interface{}) {
	t.Helper()

	defer cancelFn()

	i := 0

	for item := range channel {
		require.GreaterOrEqual(t, len(expected), i+1)

		expectedItem := expected[i]
		require.IsType(t, expectedItem, item)

		itemInType := item.(T)
		require.Equal(t, expectedItem, itemInType)

		i++
	}

	require.Equal(t, len(expected), i)

}
