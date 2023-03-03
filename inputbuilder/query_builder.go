package inputbuilder

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/raito-io/go-dynamo-utils/inputbuilder/conditionexpression"
	"github.com/raito-io/go-dynamo-utils/inputbuilder/expressionutils"
)

// QueryBuilder is a builder to create dynamodb.QueryInput objects
type QueryBuilder struct {
	TableName         string
	HashKeyCondition  *conditionexpression.EqualComparisonOperator
	RangeKeyCondition conditionexpression.RangeKeyConditionExpressionItem
	FilterExpression  conditionexpression.ExpressionItem
	ConsistentRead    bool
	IndexName         *string
	Limit             *int32
	ForwardScan       *bool
}

// NewQueryBuilder creates a new and empty QueryBuilder
func NewQueryBuilder() QueryBuilder {
	return QueryBuilder{}
}

// WithTableName sets the table name for the dynamodb.QueryInput object
func (b *QueryBuilder) WithTableName(tableName string) {
	b.TableName = tableName
}

// WithHashKeyCondition sets the hash key condition for the dynamodb.QueryInput object
// The hash key condition must be a conditionexpression.EqualComparisonOperator representing an equality between the tables Partition Key and a corresponding scalar of correct type.
func (b *QueryBuilder) WithHashKeyCondition(hashKeyCondition *conditionexpression.EqualComparisonOperator) {
	b.HashKeyCondition = hashKeyCondition
}

// WithRangeKeyCondition sets the range key condition for the dynamodb.QueryInput object
// The range key condition must be a conditionexpression.RangeKeyConditionExpressionItem representing a valid comparison between the Sort Key and a corresponding scalar.
func (b *QueryBuilder) WithRangeKeyCondition(rangeKeyCondition conditionexpression.RangeKeyConditionExpressionItem) {
	b.RangeKeyCondition = rangeKeyCondition
}

// WithFilterExpression sets the filter expression for the dynamodb.QueryInput object
func (b *QueryBuilder) WithFilterExpression(filterExpression conditionexpression.ExpressionItem) {
	b.FilterExpression = filterExpression
}

// SetConsistentRead ensures the DynamoDB query is executed as consistent read
func (b *QueryBuilder) SetConsistentRead() {
	b.ConsistentRead = true
}

// WithIndexName sets the index name for the dynamodb.QueryInput object
func (b *QueryBuilder) WithIndexName(indexName string) {
	b.IndexName = &indexName
}

// WithLimit sets the limit for the dynamodb.QueryInput object
func (b *QueryBuilder) WithLimit(limit int32) {
	b.Limit = &limit
}

// WithForwardScan sets the forward scan for the dynamodb.QueryInput object
func (b *QueryBuilder) WithForwardScan(forwardScan bool) {
	b.ForwardScan = &forwardScan
}

// Build builds the dynamodb.QueryInput object
func (b *QueryBuilder) Build(queryInput *dynamodb.QueryInput) error {
	if b.TableName == "" && queryInput.TableName == nil {
		return errors.New("tableName may not be empty")
	}

	if b.HashKeyCondition == nil {
		return errors.New("hashKeyCondition may not be nil")
	}

	if queryInput.ExpressionAttributeNames == nil {
		queryInput.ExpressionAttributeNames = make(map[string]string)
	}

	if queryInput.ExpressionAttributeValues == nil {
		queryInput.ExpressionAttributeValues = make(map[string]types.AttributeValue)
	}

	var keyConditionExpression conditionexpression.ExpressionItem
	keyConditionExpression = b.HashKeyCondition

	if b.RangeKeyCondition != nil {
		keyConditionExpression = conditionexpression.And(b.HashKeyCondition, b.RangeKeyCondition)
	}

	keyConditionExpressionString, err := conditionexpression.Marshal(expressionutils.EmptyPath().ExtendPath("key"), keyConditionExpression, queryInput.ExpressionAttributeNames, queryInput.ExpressionAttributeValues)
	if err != nil {
		return err
	}

	filterExpressionString, err := conditionexpression.Marshal(expressionutils.EmptyPath().ExtendPath("filter"), b.FilterExpression, queryInput.ExpressionAttributeNames, queryInput.ExpressionAttributeValues)
	if err != nil {
		return err
	}

	queryInput.TableName = &b.TableName
	queryInput.ConsistentRead = &b.ConsistentRead
	queryInput.FilterExpression = filterExpressionString
	queryInput.IndexName = b.IndexName
	queryInput.KeyConditionExpression = keyConditionExpressionString
	queryInput.Limit = b.Limit
	queryInput.ScanIndexForward = b.ForwardScan

	return nil
}
