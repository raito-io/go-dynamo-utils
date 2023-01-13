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

func TestScanBuilder_Build(t *testing.T) {
	type fields struct {
		TableName        string
		FilterExpression conditionexpression.ExpressionItem
		ConsistentRead   bool
		IndexName        *string
		Limit            *int32
	}
	type want struct {
		scanInput *dynamodb.ScanInput
		err       bool
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
				IndexName:        aws.String("IndexName"),
				FilterExpression: conditionexpression.Equal(expressionutils.AttributePath("AttributeA"), 42),
				ConsistentRead:   true,
				Limit:            aws.Int32(100),
			},
			want: want{
				scanInput: &dynamodb.ScanInput{
					TableName:                 aws.String("test-table"),
					IndexName:                 aws.String("IndexName"),
					FilterExpression:          aws.String("#AttributeA = :binarycomparison_right"),
					ExpressionAttributeNames:  map[string]string{"#AttributeA": "AttributeA"},
					ExpressionAttributeValues: map[string]types.AttributeValue{":binarycomparison_right": &types.AttributeValueMemberN{Value: "42"}},
					ConsistentRead:            aws.Bool(true),
					Limit:                     aws.Int32(100),
				},
			},
		},
		{
			name: "error if table not set",
			fields: fields{
				IndexName:        aws.String("IndexName"),
				FilterExpression: conditionexpression.Equal(expressionutils.AttributePath("AttributeA"), 42),
				ConsistentRead:   true,
				Limit:            aws.Int32(100),
			},
			want: want{
				err: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			b := NewScanBuilder()
			b.WithTableName(tt.fields.TableName)
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

			scanInput := dynamodb.ScanInput{}

			// When
			err := b.Build(&scanInput)

			// Then
			if tt.want.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, *tt.want.scanInput, scanInput)
			}
		})
	}
}
