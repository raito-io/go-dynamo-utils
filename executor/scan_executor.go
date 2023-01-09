package executor

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (e *Executor) scanExecution(ctx context.Context, scanInput *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	return e.client.Scan(ctx, scanInput)
}

func (e *Executor) scanGetItems(output *dynamodb.ScanOutput) []map[string]types.AttributeValue {
	return output.Items
}

func (e *Executor) scanNextPage(query *dynamodb.ScanInput, output *dynamodb.ScanOutput) (*dynamodb.ScanInput, bool) {
	if output.LastEvaluatedKey != nil && len(output.LastEvaluatedKey) > 0 {
		query.ExclusiveStartKey = output.LastEvaluatedKey

		return query, true
	}

	return nil, false
}
