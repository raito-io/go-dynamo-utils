package inputbuilder

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/raito-io/go-dynamo-utils/inputbuilder/conditionexpression"
)

// ScanBuilder is a builder to create dynamodb.ScanInput objects
type ScanBuilder struct {
	TableName        string
	FilterExpression conditionexpression.ExpressionItem
	ConsistentRead   bool
	IndexName        *string
	Limit            *int32
}

// NewScanBuilder creates a new and empty ScanBuilder
func NewScanBuilder() ScanBuilder {
	return ScanBuilder{}
}

// WithTableName sets the table name for the dynamodb.ScanInput object
func (b *ScanBuilder) WithTableName(tableName string) {
	b.TableName = tableName
}

// WithFilterExpression sets the filter expression for the dynamodb.ScanInput object
func (b *ScanBuilder) WithFilterExpression(filterExpression conditionexpression.ExpressionItem) {
	b.FilterExpression = filterExpression
}

// SetConsistentRead ensures the DynamoDB scan is executed as consistent read
func (b *ScanBuilder) SetConsistentRead() {
	b.ConsistentRead = true
}

// WithIndexName sets the index name for the dynamodb.QueryInput object
func (b *ScanBuilder) WithIndexName(indexName string) {
	b.IndexName = &indexName
}

// WithLimit sets the limit for the dynamodb.QueryInput object
func (b *ScanBuilder) WithLimit(limit int32) {
	b.Limit = &limit
}

// Build builds the dynamodb.ScanInput object
func (b *ScanBuilder) Build(input *dynamodb.ScanInput) error {
	if b.TableName == "" && input.TableName == nil {
		return errors.New("tableName may not be empty")
	}

	if input.ExpressionAttributeNames == nil {
		input.ExpressionAttributeNames = make(map[string]string)
	}

	if input.ExpressionAttributeValues == nil {
		input.ExpressionAttributeValues = make(map[string]types.AttributeValue)
	}

	filterExpressionString, err := conditionexpression.Marshal(b.FilterExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues)
	if err != nil {
		return err
	}

	input.TableName = &b.TableName
	input.ConsistentRead = &b.ConsistentRead
	input.FilterExpression = filterExpressionString
	input.IndexName = b.IndexName
	input.Limit = b.Limit

	return nil
}
