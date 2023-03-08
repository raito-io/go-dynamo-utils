package migrator

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const metadataPK = "#METADATA"

// Interface validation check
var _ DynamodbClient = (*dynamodb.Client)(nil)

//go:generate go run github.com/vektra/mockery/v2 --name=DynamodbClient --with-expecter
type DynamodbClient interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(options *dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(options *dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(options *dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(options *dynamodb.Options)) (*dynamodb.ScanOutput, error)
	TransactWriteItems(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(options *dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
	Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(options *dynamodb.Options)) (*dynamodb.QueryOutput, error)
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(options *dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
}

// Migration to execute
type Migration struct {
	// Name of the migration
	Name string

	// Description of the migration
	Description string

	// Migration function
	MigratorFn func(ctx context.Context, client DynamodbClient) error

	// JobMetadata to store in the migration metadata table
	JobMetadata map[string]interface{}
}

func Must(migration Migration, err error) Migration {
	if err != nil {
		panic(err)
	}

	return migration
}

type Migrator struct {
	MigrationIdOffset  uint64
	MigrationTableName string
	Migrations         []Migration
}

// NewMigrator creates a new Migrator that can execute a migration.
// The following parameters are used:
//   - migrationTableName: specifies the migration metadata table
//   - migrationOffsetId: requires to ID offset that should be used. IDs are automatically generated based on the order of the parameter 'migrations'. If you want to remove old migrations you need to specify the offset so new IDs are correctly generated.
//   - migrations: are the migrations to be executed
//
// Migration with an ID that are already successful executed will be skipped.
// Note that migrations should be idempotent (towards itself). If a migration fails or the update of the metadata table fails, the migration will be retried in the next execution.
func NewMigrator(migrationTableName string, migrationOffsetId uint64, migrations ...Migration) *Migrator {
	return &Migrator{
		MigrationTableName: migrationTableName,
		MigrationIdOffset:  migrationOffsetId,
		Migrations:         migrations,
	}
}

func (m *Migrator) Execute(ctx context.Context, client DynamodbClient) error {
	metadata, err := m.getMetadataObject(ctx, client)
	if err != nil {
		return fmt.Errorf("loading metadata: %w", err)
	}

	for i, migration := range m.Migrations {
		migrationId := m.MigrationIdOffset + uint64(i) + 1
		if migrationId <= metadata.LastJobId {
			continue
		}

		start := time.Now()

		err = migration.MigratorFn(ctx, client)
		if err != nil {
			return fmt.Errorf("running migration %s: %w", migration.Name, err)
		}

		err = m.annotateSuccessfulRun(ctx, client, migrationId, &m.Migrations[i], start, time.Now())
		if err != nil {
			return fmt.Errorf("updating migration: %w", err)
		}
	}

	return nil
}

func (m *Migrator) getMetadataObject(ctx context.Context, client DynamodbClient) (metadata metadataObject, err error) {
	result, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &m.MigrationTableName,
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: metadataPK},
		},
		ConsistentRead: aws.Bool(true),
	})

	if err != nil {
		return metadata, err
	}

	err = attributevalue.UnmarshalMap(result.Item, &metadata)

	return
}

func (m *Migrator) annotateSuccessfulRun(ctx context.Context, client DynamodbClient, id uint64, migration *Migration, startTime time.Time, endTime time.Time) error {
	migrationResult := migrationObject{
		PK:          fmt.Sprintf("MIGRATION#%d", id),
		ID:          id,
		Name:        migration.Name,
		Description: migration.Description,
		StartTime:   startTime,
		EndTime:     endTime,
	}

	item, err := attributevalue.MarshalMap(migrationResult)
	if err != nil {
		return err
	}

	for k, v := range migration.JobMetadata {
		if _, found := item[k]; !found {
			av, avErr := attributevalue.Marshal(v)
			if avErr != nil {
				return avErr
			}

			item[k] = av
		}
	}

	transaction := dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Update: &types.Update{
					TableName: &m.MigrationTableName,
					Key: map[string]types.AttributeValue{
						"PK": &types.AttributeValueMemberS{Value: metadataPK},
					},
					UpdateExpression:          aws.String("SET #lastJobId = :lastJobId"),
					ExpressionAttributeNames:  map[string]string{"#lastJobId": "lastJobId"},
					ExpressionAttributeValues: map[string]types.AttributeValue{":lastJobId": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", id)}},
				},
			},
			{
				Put: &types.Put{
					TableName: &m.MigrationTableName,
					Item:      item,
				},
			},
		},
	}

	_, err = client.TransactWriteItems(ctx, &transaction)

	return err
}
