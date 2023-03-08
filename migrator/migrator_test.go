package migrator

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/raito-io/go-dynamo-utils/migrator/mocks"
)

func TestMigrator_Execute_NoMigrations(t *testing.T) {
	// Given
	migrationTable := "migration_table"

	client := mocks.NewDynamodbClient(t)
	client.EXPECT().GetItem(mock.Anything, &dynamodb.GetItemInput{
		TableName: &migrationTable,
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "#METADATA"},
		},
		ConsistentRead: aws.Bool(true),
	}).Return(&dynamodb.GetItemOutput{}, nil)

	migrator := NewMigrator(migrationTable, 0)

	// When
	err := migrator.Execute(context.Background(), client)

	// Then
	require.NoError(t, err)
}

func TestMigrator_Execute_SkipExecutedMigrations(t *testing.T) {
	// Given
	migrationTable := "migration_table"

	client := mocks.NewDynamodbClient(t)
	client.EXPECT().GetItem(mock.Anything, &dynamodb.GetItemInput{
		TableName: &migrationTable,
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "#METADATA"},
		},
		ConsistentRead: aws.Bool(true),
	}).Return(&dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{
		"lastJobId": &types.AttributeValueMemberN{Value: "1"},
	}}, nil)

	storeItems := make([]*dynamodb.TransactWriteItemsInput, 0, 1)

	client.EXPECT().TransactWriteItems(mock.Anything, mock.Anything).Run(func(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) {
		storeItems = append(storeItems, params)
	}).Return(&dynamodb.TransactWriteItemsOutput{}, nil).Once()

	migration1Executed := false
	migration2Executed := false

	migration1 := Migration{
		Name:        "migration_1",
		Description: "description_1",
		MigratorFn: func(ctx context.Context, client DynamodbClient) error {
			migration1Executed = true

			return nil
		},
	}

	migration2 := Migration{
		Name:        "migration_2",
		Description: "description_2",
		MigratorFn: func(ctx context.Context, client DynamodbClient) error {
			require.False(t, migration1Executed)
			require.False(t, migration2Executed)

			migration2Executed = true

			return nil
		},
		JobMetadata: map[string]interface{}{"foo": "bar"},
	}

	migrator := NewMigrator(migrationTable, 0, migration1, migration2)

	// When
	err := migrator.Execute(context.Background(), client)

	// Then
	require.NoError(t, err)
	require.False(t, migration1Executed)

	require.Equal(t, types.TransactWriteItem{
		Update: &types.Update{
			TableName: &migrationTable,
			Key: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: metadataPK},
			},
			UpdateExpression:          aws.String("SET #lastJobId = :lastJobId"),
			ExpressionAttributeNames:  map[string]string{"#lastJobId": "lastJobId"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":lastJobId": &types.AttributeValueMemberN{Value: "2"}},
		},
	}, storeItems[0].TransactItems[0])

	require.Equal(t, &types.AttributeValueMemberS{Value: "MIGRATION#2"}, storeItems[0].TransactItems[1].Put.Item["PK"])
	require.Equal(t, &types.AttributeValueMemberN{Value: "2"}, storeItems[0].TransactItems[1].Put.Item["id"])
	require.Equal(t, &types.AttributeValueMemberS{Value: "migration_2"}, storeItems[0].TransactItems[1].Put.Item["name"])
	require.Equal(t, &types.AttributeValueMemberS{Value: "description_2"}, storeItems[0].TransactItems[1].Put.Item["description"])
	require.Equal(t, &types.AttributeValueMemberS{Value: "bar"}, storeItems[0].TransactItems[1].Put.Item["foo"])
}

func TestMigrator_Execute_FirstMigrations(t *testing.T) {
	// Given
	migrationTable := "migration_table"

	client := mocks.NewDynamodbClient(t)
	client.EXPECT().GetItem(mock.Anything, &dynamodb.GetItemInput{
		TableName: &migrationTable,
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "#METADATA"},
		},
		ConsistentRead: aws.Bool(true),
	}).Return(&dynamodb.GetItemOutput{}, nil)

	storeItems := make([]*dynamodb.TransactWriteItemsInput, 0, 2)

	client.EXPECT().TransactWriteItems(mock.Anything, mock.Anything).Run(func(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) {
		storeItems = append(storeItems, params)
	}).Return(&dynamodb.TransactWriteItemsOutput{}, nil).Twice()

	migration1Executed := false
	migration2Executed := false

	migration1 := Migration{
		Name:        "migration_1",
		Description: "description_1",
		MigratorFn: func(ctx context.Context, client DynamodbClient) error {
			require.False(t, migration1Executed)
			require.False(t, migration2Executed)

			migration1Executed = true

			return nil
		},
	}

	migration2 := Migration{
		Name:        "migration_2",
		Description: "description_2",
		MigratorFn: func(ctx context.Context, client DynamodbClient) error {
			require.True(t, migration1Executed)
			require.False(t, migration2Executed)

			migration2Executed = true

			return nil
		},
		JobMetadata: map[string]interface{}{"foo": "bar", "name": "MustBeIgnored!"},
	}

	migrator := NewMigrator(migrationTable, 0, migration1, migration2)

	// When
	err := migrator.Execute(context.Background(), client)

	// Then
	require.NoError(t, err)

	require.Equal(t, types.TransactWriteItem{
		Update: &types.Update{
			TableName: &migrationTable,
			Key: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: metadataPK},
			},
			UpdateExpression:          aws.String("SET #lastJobId = :lastJobId"),
			ExpressionAttributeNames:  map[string]string{"#lastJobId": "lastJobId"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":lastJobId": &types.AttributeValueMemberN{Value: "1"}},
		},
	}, storeItems[0].TransactItems[0])

	require.Equal(t, types.TransactWriteItem{
		Update: &types.Update{
			TableName: &migrationTable,
			Key: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: metadataPK},
			},
			UpdateExpression:          aws.String("SET #lastJobId = :lastJobId"),
			ExpressionAttributeNames:  map[string]string{"#lastJobId": "lastJobId"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":lastJobId": &types.AttributeValueMemberN{Value: "2"}},
		},
	}, storeItems[1].TransactItems[0])

	require.Equal(t, &types.AttributeValueMemberS{Value: "MIGRATION#1"}, storeItems[0].TransactItems[1].Put.Item["PK"])
	require.Equal(t, &types.AttributeValueMemberN{Value: "1"}, storeItems[0].TransactItems[1].Put.Item["id"])
	require.Equal(t, &types.AttributeValueMemberS{Value: "migration_1"}, storeItems[0].TransactItems[1].Put.Item["name"])
	require.Equal(t, &types.AttributeValueMemberS{Value: "description_1"}, storeItems[0].TransactItems[1].Put.Item["description"])

	require.Equal(t, &types.AttributeValueMemberS{Value: "MIGRATION#2"}, storeItems[1].TransactItems[1].Put.Item["PK"])
	require.Equal(t, &types.AttributeValueMemberN{Value: "2"}, storeItems[1].TransactItems[1].Put.Item["id"])
	require.Equal(t, &types.AttributeValueMemberS{Value: "migration_2"}, storeItems[1].TransactItems[1].Put.Item["name"])
	require.Equal(t, &types.AttributeValueMemberS{Value: "description_2"}, storeItems[1].TransactItems[1].Put.Item["description"])
	require.Equal(t, &types.AttributeValueMemberS{Value: "bar"}, storeItems[1].TransactItems[1].Put.Item["foo"])
}

func TestMigrator_Execute_MigrationIdOffset(t *testing.T) {
	// Given
	migrationTable := "migration_table"

	client := mocks.NewDynamodbClient(t)
	client.EXPECT().GetItem(mock.Anything, &dynamodb.GetItemInput{
		TableName: &migrationTable,
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "#METADATA"},
		},
		ConsistentRead: aws.Bool(true),
	}).Return(&dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{
		"lastJobId": &types.AttributeValueMemberN{Value: "2"},
	}}, nil)

	storeItems := make([]*dynamodb.TransactWriteItemsInput, 0, 2)

	client.EXPECT().TransactWriteItems(mock.Anything, mock.Anything).Run(func(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) {
		storeItems = append(storeItems, params)
	}).Return(&dynamodb.TransactWriteItemsOutput{}, nil).Twice()

	migration1Executed := false
	migration2Executed := false

	migration1 := Migration{
		Name:        "migration_1",
		Description: "description_1",
		MigratorFn: func(ctx context.Context, client DynamodbClient) error {
			require.False(t, migration1Executed)
			require.False(t, migration2Executed)

			migration1Executed = true

			return nil
		},
	}

	migration2 := Migration{
		Name:        "migration_2",
		Description: "description_2",
		MigratorFn: func(ctx context.Context, client DynamodbClient) error {
			require.True(t, migration1Executed)
			require.False(t, migration2Executed)

			migration2Executed = true

			return nil
		},
		JobMetadata: map[string]interface{}{"foo": "bar"},
	}

	migrator := NewMigrator(migrationTable, 2, migration1, migration2)

	// When
	err := migrator.Execute(context.Background(), client)

	// Then
	require.NoError(t, err)

	require.Equal(t, types.TransactWriteItem{
		Update: &types.Update{
			TableName: &migrationTable,
			Key: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: metadataPK},
			},
			UpdateExpression:          aws.String("SET #lastJobId = :lastJobId"),
			ExpressionAttributeNames:  map[string]string{"#lastJobId": "lastJobId"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":lastJobId": &types.AttributeValueMemberN{Value: "3"}},
		},
	}, storeItems[0].TransactItems[0])

	require.Equal(t, types.TransactWriteItem{
		Update: &types.Update{
			TableName: &migrationTable,
			Key: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: metadataPK},
			},
			UpdateExpression:          aws.String("SET #lastJobId = :lastJobId"),
			ExpressionAttributeNames:  map[string]string{"#lastJobId": "lastJobId"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":lastJobId": &types.AttributeValueMemberN{Value: "4"}},
		},
	}, storeItems[1].TransactItems[0])

	require.Equal(t, &types.AttributeValueMemberS{Value: "MIGRATION#3"}, storeItems[0].TransactItems[1].Put.Item["PK"])
	require.Equal(t, &types.AttributeValueMemberN{Value: "3"}, storeItems[0].TransactItems[1].Put.Item["id"])
	require.Equal(t, &types.AttributeValueMemberS{Value: "migration_1"}, storeItems[0].TransactItems[1].Put.Item["name"])
	require.Equal(t, &types.AttributeValueMemberS{Value: "description_1"}, storeItems[0].TransactItems[1].Put.Item["description"])

	require.Equal(t, &types.AttributeValueMemberS{Value: "MIGRATION#4"}, storeItems[1].TransactItems[1].Put.Item["PK"])
	require.Equal(t, &types.AttributeValueMemberN{Value: "4"}, storeItems[1].TransactItems[1].Put.Item["id"])
	require.Equal(t, &types.AttributeValueMemberS{Value: "migration_2"}, storeItems[1].TransactItems[1].Put.Item["name"])
	require.Equal(t, &types.AttributeValueMemberS{Value: "description_2"}, storeItems[1].TransactItems[1].Put.Item["description"])
	require.Equal(t, &types.AttributeValueMemberS{Value: "bar"}, storeItems[1].TransactItems[1].Put.Item["foo"])
}

func TestMigrator_Execute_FailedToLoadMetadata(t *testing.T) {
	// Given
	migrationTable := "migration_table"

	client := mocks.NewDynamodbClient(t)
	client.EXPECT().GetItem(mock.Anything, &dynamodb.GetItemInput{
		TableName: &migrationTable,
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "#METADATA"},
		},
		ConsistentRead: aws.Bool(true),
	}).Return(nil, errors.New("some error"))

	migration1 := Migration{
		Name:        "migration_1",
		Description: "description_1",
		MigratorFn: func(ctx context.Context, client DynamodbClient) error {
			return nil
		},
	}

	migration2 := Migration{
		Name:        "migration_2",
		Description: "description_2",
		MigratorFn: func(ctx context.Context, client DynamodbClient) error {
			return nil
		},
		JobMetadata: map[string]interface{}{"foo": "bar"},
	}

	migrator := NewMigrator(migrationTable, 2, migration1, migration2)

	// When
	err := migrator.Execute(context.Background(), client)

	// Then
	require.EqualError(t, err, "loading metadata: some error")
}

func TestMigrator_Execute_FailedToExecuteMigration(t *testing.T) {
	// Given
	migrationTable := "migration_table"

	client := mocks.NewDynamodbClient(t)
	client.EXPECT().GetItem(mock.Anything, &dynamodb.GetItemInput{
		TableName: &migrationTable,
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "#METADATA"},
		},
		ConsistentRead: aws.Bool(true),
	}).Return(&dynamodb.GetItemOutput{}, nil)

	storeItems := make([]*dynamodb.TransactWriteItemsInput, 0, 1)

	client.EXPECT().TransactWriteItems(mock.Anything, mock.Anything).Run(func(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) {
		storeItems = append(storeItems, params)
	}).Return(&dynamodb.TransactWriteItemsOutput{}, nil).Once()

	migration1Executed := false
	migration2Executed := false

	migration1 := Migration{
		Name:        "migration_1",
		Description: "description_1",
		MigratorFn: func(ctx context.Context, client DynamodbClient) error {
			require.False(t, migration1Executed)
			require.False(t, migration2Executed)

			migration1Executed = true

			return nil
		},
	}

	migration2 := Migration{
		Name:        "migration_2",
		Description: "description_2",
		MigratorFn: func(ctx context.Context, client DynamodbClient) error {
			require.True(t, migration1Executed)
			require.False(t, migration2Executed)

			migration2Executed = true

			return errors.New("some error")
		},
		JobMetadata: map[string]interface{}{"foo": "bar"},
	}

	migrator := NewMigrator(migrationTable, 0, migration1, migration2)

	// When
	err := migrator.Execute(context.Background(), client)

	// Then
	require.EqualError(t, err, "running migration migration_2: some error")

	require.Equal(t, types.TransactWriteItem{
		Update: &types.Update{
			TableName: &migrationTable,
			Key: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: metadataPK},
			},
			UpdateExpression:          aws.String("SET #lastJobId = :lastJobId"),
			ExpressionAttributeNames:  map[string]string{"#lastJobId": "lastJobId"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":lastJobId": &types.AttributeValueMemberN{Value: "1"}},
		},
	}, storeItems[0].TransactItems[0])

	require.Equal(t, &types.AttributeValueMemberS{Value: "MIGRATION#1"}, storeItems[0].TransactItems[1].Put.Item["PK"])
	require.Equal(t, &types.AttributeValueMemberN{Value: "1"}, storeItems[0].TransactItems[1].Put.Item["id"])
	require.Equal(t, &types.AttributeValueMemberS{Value: "migration_1"}, storeItems[0].TransactItems[1].Put.Item["name"])
	require.Equal(t, &types.AttributeValueMemberS{Value: "description_1"}, storeItems[0].TransactItems[1].Put.Item["description"])
}
