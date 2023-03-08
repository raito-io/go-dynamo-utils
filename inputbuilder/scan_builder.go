package inputbuilder

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/raito-io/go-dynamo-utils/inputbuilder/conditionexpression"
	"github.com/raito-io/go-dynamo-utils/inputbuilder/expressionutils"
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

// WithConsistentRead sets the consistent read flag for the dynamodb.QueryInput object
func (b *ScanBuilder) WithConsistentRead(consistentRead bool) {
	b.ConsistentRead = consistentRead
}

// Build builds the dynamodb.ScanInput object
func (b *ScanBuilder) Build(input *dynamodb.ScanInput) error {
	if b.TableName == "" && input.TableName == nil {
		return errors.New("tableName may not be empty")
	}

	expressionAttributeNamesTmp := make(map[string]string)
	expressionAttributeValuesTmp := make(map[string]types.AttributeValue)

	filterExpressionString, err := conditionexpression.Marshal(expressionutils.EmptyPath(), b.FilterExpression, expressionAttributeNamesTmp, expressionAttributeValuesTmp)
	if err != nil {
		return err
	}

	if len(expressionAttributeNamesTmp) > 0 {
		if input.ExpressionAttributeNames == nil {
			input.ExpressionAttributeNames = expressionAttributeNamesTmp
		} else {
			for k, v := range expressionAttributeNamesTmp {
				input.ExpressionAttributeNames[k] = v
			}
		}
	}

	if len(expressionAttributeValuesTmp) > 0 {
		if input.ExpressionAttributeValues == nil {
			input.ExpressionAttributeValues = expressionAttributeValuesTmp
		} else {
			for k, v := range expressionAttributeValuesTmp {
				input.ExpressionAttributeValues[k] = v
			}
		}
	}

	input.TableName = &b.TableName
	input.ConsistentRead = &b.ConsistentRead
	input.FilterExpression = filterExpressionString
	input.IndexName = b.IndexName
	input.Limit = b.Limit

	return nil
}
