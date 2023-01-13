package conditionexpression

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/ptr"
	"github.com/stretchr/testify/require"

	"github.com/raito-io/go-dynamo-utils/inputbuilder/expressionutils"
)

func TestMarshal(t *testing.T) {
	type args struct {
		item ExpressionItem
	}
	type want struct {
		output          *string
		attributeNames  map[string]string
		attributeValues map[string]types.AttributeValue
	}
	tests := []struct {
		name    string
		args    args
		want    want
		wantErr bool
	}{
		{
			name: "no marshalling if no item",
			args: args{
				item: nil,
			},
			want: want{
				output:          nil,
				attributeNames:  map[string]string{},
				attributeValues: map[string]types.AttributeValue{},
			},
		},
		{
			name: "marshal objects",
			args: args{
				item: Or(LessThan(expressionutils.AttributePath("AttributeA"), 42), BeginsWith("AttributeB", "Prefix")),
			},
			want: want{
				output:          ptr.String("(#AttributeA < :orleft_binarycomparison_right OR begins_with(#AttributeB, :orright_attributeb))"),
				attributeNames:  map[string]string{"#AttributeA": "AttributeA", "#AttributeB": "AttributeB"},
				attributeValues: map[string]types.AttributeValue{":orleft_binarycomparison_right": &types.AttributeValueMemberN{Value: "42"}, ":orright_attributeb": &types.AttributeValueMemberS{Value: "Prefix"}},
			},
		},
		{
			name: "forward already marshalled objects",
			args: args{
				item: LessThan(expressionutils.AttributePath("AttributeA"), &types.AttributeValueMemberN{Value: "42"}),
			},
			want: want{
				output:          ptr.String("#AttributeA < :binarycomparison_right"),
				attributeNames:  map[string]string{"#AttributeA": "AttributeA"},
				attributeValues: map[string]types.AttributeValue{":binarycomparison_right": &types.AttributeValueMemberN{Value: "42"}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			attributeNames := make(map[string]string)
			attributeValues := make(map[string]types.AttributeValue)

			// When
			output, err := Marshal(tt.args.item, attributeNames, attributeValues)

			// Then
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want.output, output)
				require.Equal(t, tt.want.attributeNames, attributeNames)
				require.Equal(t, tt.want.attributeValues, attributeValues)
			}
		})
	}
}
