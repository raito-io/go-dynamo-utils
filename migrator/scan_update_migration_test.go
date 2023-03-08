package migrator

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/raito-io/go-dynamo-utils/inputbuilder/conditionexpression"
	"github.com/raito-io/go-dynamo-utils/migrator/mocks"
)

func TestNewScanAndUpdateMigration_SimpleMigration(t *testing.T) {
	name := "simple_migration"
	description := "Simple migration"
	table := "table_to_migrate"

	updateQuery := &dynamodb.UpdateItemInput{
		TableName:        aws.String(table),
		UpdateExpression: aws.String("Dummy Update"),
	}

	updateFn := func(ctx context.Context, item map[string]types.AttributeValue) *dynamodb.UpdateItemInput {
		if _, found := item["upgrade"]; found {
			return updateQuery
		}

		return nil
	}

	// When
	m, err := NewScanAndUpdateMigration(name, description, table, updateFn)

	// Then
	require.NoError(t, err)
	require.Equal(t, name, m.Name)
	require.Equal(t, description, m.Description)
	require.Equal(t, map[string]interface{}{"table": table}, m.JobMetadata)

	client := mocks.NewDynamodbClient(t)
	client.EXPECT().Scan(mock.Anything, &dynamodb.ScanInput{
		TableName:                 &table,
		ConsistentRead:            aws.Bool(true),
		ExpressionAttributeNames:  map[string]string{},
		ExpressionAttributeValues: map[string]types.AttributeValue{},
	}).Return(&dynamodb.ScanOutput{
		Count: 2,
		Items: []map[string]types.AttributeValue{
			{

				"upgrade":            &types.AttributeValueMemberS{Value: "Dummy Update"},
				"someOtherAttribute": &types.AttributeValueMemberS{Value: "dummyValue"},
			},
			{
				"someOtherAttribute": &types.AttributeValueMemberS{Value: "dummyValue2"},
			},
		},
	}, nil).Once()

	client.EXPECT().UpdateItem(mock.Anything, updateQuery).Return(&dynamodb.UpdateItemOutput{}, nil).Once()

	err = m.MigratorFn(context.Background(), client)
	require.NoError(t, err)
}

func TestNewScanAndUpdateMigration_WithOptions(t *testing.T) {
	name := "simple_migration"
	description := "Simple migration"
	table := "table_to_migrate"

	updateQuery := &dynamodb.UpdateItemInput{
		TableName:        aws.String(table),
		UpdateExpression: aws.String("Dummy Update"),
	}

	updateFn := func(ctx context.Context, item map[string]types.AttributeValue) *dynamodb.UpdateItemInput {
		if _, found := item["upgrade"]; found {
			return updateQuery
		}

		return nil
	}

	// When
	m, err := NewScanAndUpdateMigration(name, description, table, updateFn,
		ScanAndUpdateMigrationWithMetadata(map[string]interface{}{"foo": "bar"}), ScanAndUpdateMigrationWithConsistentRead(false),
		ScanAndUpdateMigrationWithFilterExpression(conditionexpression.BeginsWith("SK", "START#")))

	// Then
	require.NoError(t, err)
	require.Equal(t, name, m.Name)
	require.Equal(t, description, m.Description)
	require.Equal(t, map[string]interface{}{"table": table, "foo": "bar"}, m.JobMetadata)

	client := mocks.NewDynamodbClient(t)
	client.EXPECT().Scan(mock.Anything, &dynamodb.ScanInput{
		TableName:                 &table,
		ConsistentRead:            aws.Bool(false),
		FilterExpression:          aws.String("begins_with(#SK, :sk)"),
		ExpressionAttributeNames:  map[string]string{"#SK": "SK"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":sk": &types.AttributeValueMemberS{Value: "START#"}},
	}).Return(&dynamodb.ScanOutput{
		Count: 2,
		Items: []map[string]types.AttributeValue{
			{

				"upgrade":            &types.AttributeValueMemberS{Value: "Dummy Update"},
				"someOtherAttribute": &types.AttributeValueMemberS{Value: "dummyValue"},
			},
			{
				"someOtherAttribute": &types.AttributeValueMemberS{Value: "dummyValue2"},
			},
		},
	}, nil).Once()

	client.EXPECT().UpdateItem(mock.Anything, updateQuery).Return(&dynamodb.UpdateItemOutput{}, nil).Once()

	err = m.MigratorFn(context.Background(), client)
	require.NoError(t, err)
}
