package inputbuilder

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/raito-io/go-dynamo-utils/inputbuilder/conditionexpression"
)

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

func NewQueryBuilder() QueryBuilder {
	return QueryBuilder{}
}

func (b *QueryBuilder) WithTableName(tableName string) {
	b.TableName = tableName
}

func (b *QueryBuilder) WithHashKeyCondition(hashKeyCondition *conditionexpression.EqualComparisonOperator) {
	b.HashKeyCondition = hashKeyCondition
}

func (b *QueryBuilder) WithRangeKeyCondition(rangeKeyCondition conditionexpression.RangeKeyConditionExpressionItem) {
	b.RangeKeyCondition = rangeKeyCondition
}

func (b *QueryBuilder) WithFilterExpression(filterExpression conditionexpression.ExpressionItem) {
	b.FilterExpression = filterExpression
}

func (b *QueryBuilder) SetConsistentRead() {
	b.ConsistentRead = true
}

func (b *QueryBuilder) WithIndexName(indexName string) {
	b.IndexName = &indexName
}

func (b *QueryBuilder) WithLimit(limit int32) {
	b.Limit = &limit
}

func (b *QueryBuilder) WithForwardScan(forwardScan bool) {
	b.ForwardScan = &forwardScan
}

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

	keyConditionExpressionString, err := conditionexpression.Marshal(keyConditionExpression, queryInput.ExpressionAttributeNames, queryInput.ExpressionAttributeValues)
	if err != nil {
		return err
	}

	filterExpressionString, err := conditionexpression.Marshal(b.FilterExpression, queryInput.ExpressionAttributeNames, queryInput.ExpressionAttributeValues)
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
