package migrator

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/raito-io/go-dynamo-utils/executor"
	"github.com/raito-io/go-dynamo-utils/inputbuilder"
	"github.com/raito-io/go-dynamo-utils/inputbuilder/conditionexpression"
)

type ScanAndUpdateMigrationOptions struct {
	FilterExpression conditionexpression.ExpressionItem
	ConsistentRead   *bool
	Metadata         map[string]interface{}
}

type OptionFn func(*ScanAndUpdateMigrationOptions)

// NewScanAndUpdateMigration create a migration that will execute a scan. For each item in the table an updateFn will be executed.
// If the updateFn return a non nil *dynamodb.UpdateItemInput, the update will be executed.
func NewScanAndUpdateMigration(name string, description string, table string, updateFn func(ctx context.Context, item map[string]types.AttributeValue) *dynamodb.UpdateItemInput, optFn ...OptionFn) (*Migration, error) {
	options := ScanAndUpdateMigrationOptions{}

	for _, opt := range optFn {
		opt(&options)
	}

	scanBuilder := inputbuilder.NewScanBuilder()
	scanBuilder.WithTableName(table)

	if options.ConsistentRead != nil {
		scanBuilder.WithConsistentRead(*options.ConsistentRead)
	} else {
		scanBuilder.WithConsistentRead(true)
	}

	if options.FilterExpression != nil {
		scanBuilder.WithFilterExpression(options.FilterExpression)
	}

	scanInput := &dynamodb.ScanInput{}

	err := scanBuilder.Build(scanInput)
	if err != nil {
		return nil, err
	}

	metadata := map[string]interface{}{"table": table}

	if options.Metadata != nil {
		for key, value := range options.Metadata {
			metadata[key] = value
		}
	}

	return &Migration{
		Name:        name,
		Description: description,
		MigratorFn: func(ctx context.Context, client DynamodbClient) error {
			exec := executor.New(client)
			items := exec.Scan(ctx, scanInput)

			for item := range items {
				switch v := item.(type) {
				case error:
					return v
				case map[string]types.AttributeValue:
					update := updateFn(ctx, v)
					if update != nil {
						_, err := client.UpdateItem(ctx, update)
						if err != nil {
							return err
						}
					}
				}
			}

			return nil
		},
		JobMetadata: metadata,
	}, nil
}

// ScanAndUpdateMigrationWithFilterExpression set filter condition on ScanAndUpdateMigration
func ScanAndUpdateMigrationWithFilterExpression(filter conditionexpression.ExpressionItem) OptionFn {
	return func(options *ScanAndUpdateMigrationOptions) {
		options.FilterExpression = filter
	}
}

// ScanAndUpdateMigrationWithConsistentRead annotate consistentRead on ScanAndUpdateMigration. Note that default consistent read will be true
func ScanAndUpdateMigrationWithConsistentRead(consistentRead bool) OptionFn {
	return func(options *ScanAndUpdateMigrationOptions) {
		options.ConsistentRead = &consistentRead
	}
}

// ScanAndUpdateMigrationWithMetadata add metadata to ScanAndUpdateMigration
func ScanAndUpdateMigrationWithMetadata(metadata map[string]interface{}) OptionFn {
	return func(options *ScanAndUpdateMigrationOptions) {
		options.Metadata = metadata
	}
}
