package inputbuilder

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/raito-io/go-dynamo-utils/inputbuilder/conditionexpression"
	"github.com/raito-io/go-dynamo-utils/inputbuilder/expressionutils"
)

func TestQueryBuilder_Build(t *testing.T) {
	type fields struct {
		TableName         string
		HashKeyCondition  *conditionexpression.EqualComparisonOperator
		RangeKeyCondition conditionexpression.RangeKeyConditionExpressionItem
		FilterExpression  conditionexpression.ExpressionItem
		ConsistentRead    bool
		IndexName         *string
		Limit             *int32
		ForwardScan       *bool
	}
	type want struct {
		queryInput *dynamodb.QueryInput
		err        bool
	}
	tests := []struct {
		name   string
		fields fields
		want   want
	}{
		{
			name: "hash key condition on index",
			fields: fields{
				TableName:        "test-table",
				HashKeyCondition: conditionexpression.Equal(expressionutils.AttributePath("AttributeA"), "PartitionKey"),
				IndexName:        aws.String("IndexName"),
			},
			want: want{
				queryInput: &dynamodb.QueryInput{
					TableName:                 aws.String("test-table"),
					IndexName:                 aws.String("IndexName"),
					KeyConditionExpression:    aws.String("#AttributeA = :key_binarycomparison_right"),
					ExpressionAttributeNames:  map[string]string{"#AttributeA": "AttributeA"},
					ExpressionAttributeValues: map[string]types.AttributeValue{":key_binarycomparison_right": &types.AttributeValueMemberS{Value: "PartitionKey"}},
					ConsistentRead:            aws.Bool(false),
				},
			},
		},
		{
			name: "hash and range key condition",
			fields: fields{
				TableName:         "test-table",
				HashKeyCondition:  conditionexpression.Equal(expressionutils.AttributePath("AttributeA"), "PartitionKey"),
				RangeKeyCondition: conditionexpression.BeginsWith("AttributeB", "Prefix"),
				ConsistentRead:    true,
				Limit:             aws.Int32(100),
				ForwardScan:       aws.Bool(false),
			},
			want: want{
				queryInput: &dynamodb.QueryInput{
					TableName:                 aws.String("test-table"),
					KeyConditionExpression:    aws.String("(#AttributeA = :key_andleft_binarycomparison_right AND begins_with(#AttributeB, :key_andright_attributeb))"),
					ExpressionAttributeNames:  map[string]string{"#AttributeA": "AttributeA", "#AttributeB": "AttributeB"},
					ExpressionAttributeValues: map[string]types.AttributeValue{":key_andleft_binarycomparison_right": &types.AttributeValueMemberS{Value: "PartitionKey"}, ":key_andright_attributeb": &types.AttributeValueMemberS{Value: "Prefix"}},
					ConsistentRead:            aws.Bool(true),
					Limit:                     aws.Int32(100),
					ScanIndexForward:          aws.Bool(false),
				},
			},
		},
		{
			name: "error if table not set",
			fields: fields{
				HashKeyCondition:  conditionexpression.Equal(expressionutils.AttributePath("AttributeA"), "PartitionKey"),
				RangeKeyCondition: conditionexpression.BeginsWith("AttributeB", "Prefix"),
			},
			want: want{
				err: true,
			},
		},
		{
			name: "error if hash not set",
			fields: fields{
				TableName: "test-table",
			},
			want: want{
				err: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			b := NewQueryBuilder()
			b.WithTableName(tt.fields.TableName)
			b.WithHashKeyCondition(tt.fields.HashKeyCondition)
			b.WithRangeKeyCondition(tt.fields.RangeKeyCondition)
			b.WithFilterExpression(tt.fields.FilterExpression)

			if tt.fields.ConsistentRead {
				b.SetConsistentRead()
			}

			if tt.fields.IndexName != nil {
				b.WithIndexName(*tt.fields.IndexName)
			}

			if tt.fields.Limit != nil {
				b.WithLimit(*tt.fields.Limit)
			}

			if tt.fields.ForwardScan != nil {
				b.WithForwardScan(*tt.fields.ForwardScan)
			}

			queryInput := dynamodb.QueryInput{}

			// When
			err := b.Build(&queryInput)

			// Then
			if tt.want.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, *tt.want.queryInput, queryInput)
			}
		})
	}
}
