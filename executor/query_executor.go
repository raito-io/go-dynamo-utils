package executor

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (e *Executor) queryExecution(ctx context.Context, query *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	return e.client.Query(ctx, query)
}

func (e *Executor) queryGetItems(output *dynamodb.QueryOutput) []map[string]types.AttributeValue {
	return output.Items
}

func (e *Executor) queryNextPage(query *dynamodb.QueryInput, output *dynamodb.QueryOutput) (*dynamodb.QueryInput, bool) {
	if output.LastEvaluatedKey != nil && len(output.LastEvaluatedKey) > 0 {
		query.ExclusiveStartKey = output.LastEvaluatedKey

		return query, true
	}

	return nil, false
}
