package inputbuilder

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/raito-io/go-dynamo-utils/inputbuilder/conditionexpression"
	"github.com/raito-io/go-dynamo-utils/inputbuilder/expressionutils"
	"github.com/raito-io/go-dynamo-utils/inputbuilder/updateexpression"
)

func TestUpdateBuilder_BuildUpdateItemInput(t *testing.T) {
	type fields struct {
		TableName           string
		Key                 map[string]interface{}
		Set                 []*updateexpression.SetOperationItem
		Add                 []*updateexpression.AddOperationItem
		Delete              []*updateexpression.DeleteOperationItem
		Remove              []expressionutils.AttributePath
		ConditionExpression conditionexpression.ExpressionItem
	}
	tests := []struct {
		name           string
		fields         fields
		expectedOutput *dynamodb.UpdateItemInput
		wantErr        bool
	}{
		{
			name: "empty simple build",
			fields: fields{
				TableName:           "tableName",
				Key:                 map[string]interface{}{"key": "key"},
				Set:                 []*updateexpression.SetOperationItem{},
				Add:                 []*updateexpression.AddOperationItem{},
				Delete:              []*updateexpression.DeleteOperationItem{},
				Remove:              []expressionutils.AttributePath{},
				ConditionExpression: nil,
			},
			expectedOutput: &dynamodb.UpdateItemInput{
				TableName: aws.String("tableName"),
				Key: map[string]types.AttributeValue{
					"key": &types.AttributeValueMemberS{Value: "key"},
				},
			},
			wantErr: false,
		},
		{
			name: "build updateItemInput",
			fields: fields{
				TableName:           "tableName",
				Key:                 map[string]interface{}{"key": "key"},
				Set:                 []*updateexpression.SetOperationItem{updateexpression.Set("attribute1", "value1")},
				Add:                 []*updateexpression.AddOperationItem{updateexpression.Add("attribute2", 5)},
				Delete:              []*updateexpression.DeleteOperationItem{updateexpression.Delete("attribute3", 10)},
				Remove:              []expressionutils.AttributePath{"attribute4"},
				ConditionExpression: nil,
			},
			expectedOutput: &dynamodb.UpdateItemInput{
				TableName: aws.String("tableName"),
				Key: map[string]types.AttributeValue{
					"key": &types.AttributeValueMemberS{Value: "key"},
				},
				ExpressionAttributeNames:  map[string]string{"#attribute1": "attribute1", "#attribute2": "attribute2", "#attribute3": "attribute3", "#attribute4": "attribute4"},
				ExpressionAttributeValues: map[string]types.AttributeValue{":set_attribute1": &types.AttributeValueMemberS{Value: "value1"}, ":add_attribute2": &types.AttributeValueMemberN{Value: "5"}, ":delete_attribute3": &types.AttributeValueMemberN{Value: "10"}},
				UpdateExpression:          aws.String("SET #attribute1 = :set_attribute1 ADD #attribute2 :add_attribute2 DELETE #attribute3 :delete_attribute3 REMOVE #attribute4"),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &UpdateBuilder{
				TableName:           tt.fields.TableName,
				Key:                 tt.fields.Key,
				Set:                 tt.fields.Set,
				Add:                 tt.fields.Add,
				Delete:              tt.fields.Delete,
				Remove:              tt.fields.Remove,
				ConditionExpression: tt.fields.ConditionExpression,
			}

			input := &dynamodb.UpdateItemInput{}

			err := b.BuildUpdateItemInput(input)

			require.NoError(t, err)
			require.Equal(t, tt.expectedOutput, input)
		})
	}
}

func TestUpdateBuilder_BuildUpdateTransactItem(t *testing.T) {
	type fields struct {
		TableName           string
		Key                 map[string]interface{}
		Set                 []*updateexpression.SetOperationItem
		Add                 []*updateexpression.AddOperationItem
		Delete              []*updateexpression.DeleteOperationItem
		Remove              []expressionutils.AttributePath
		ConditionExpression conditionexpression.ExpressionItem
	}
	tests := []struct {
		name           string
		fields         fields
		expectedOutput *types.Update
		wantErr        bool
	}{
		{
			name: "empty simple build",
			fields: fields{
				TableName:           "tableName",
				Key:                 map[string]interface{}{"key": "key"},
				Set:                 []*updateexpression.SetOperationItem{},
				Add:                 []*updateexpression.AddOperationItem{},
				Delete:              []*updateexpression.DeleteOperationItem{},
				Remove:              []expressionutils.AttributePath{},
				ConditionExpression: nil,
			},
			expectedOutput: &types.Update{
				TableName: aws.String("tableName"),
				Key: map[string]types.AttributeValue{
					"key": &types.AttributeValueMemberS{Value: "key"},
				},
			},
			wantErr: false,
		},
		{
			name: "build updateItemInput",
			fields: fields{
				TableName:           "tableName",
				Key:                 map[string]interface{}{"key": "key"},
				Set:                 []*updateexpression.SetOperationItem{updateexpression.Set("attribute1", "value1")},
				Add:                 []*updateexpression.AddOperationItem{updateexpression.Add("attribute2", 5)},
				Delete:              []*updateexpression.DeleteOperationItem{updateexpression.Delete("attribute3", 10)},
				Remove:              []expressionutils.AttributePath{"attribute4"},
				ConditionExpression: nil,
			},
			expectedOutput: &types.Update{
				TableName: aws.String("tableName"),
				Key: map[string]types.AttributeValue{
					"key": &types.AttributeValueMemberS{Value: "key"},
				},
				ExpressionAttributeNames:  map[string]string{"#attribute1": "attribute1", "#attribute2": "attribute2", "#attribute3": "attribute3", "#attribute4": "attribute4"},
				ExpressionAttributeValues: map[string]types.AttributeValue{":set_attribute1": &types.AttributeValueMemberS{Value: "value1"}, ":add_attribute2": &types.AttributeValueMemberN{Value: "5"}, ":delete_attribute3": &types.AttributeValueMemberN{Value: "10"}},
				UpdateExpression:          aws.String("SET #attribute1 = :set_attribute1 ADD #attribute2 :add_attribute2 DELETE #attribute3 :delete_attribute3 REMOVE #attribute4"),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &UpdateBuilder{
				TableName:           tt.fields.TableName,
				Key:                 tt.fields.Key,
				Set:                 tt.fields.Set,
				Add:                 tt.fields.Add,
				Delete:              tt.fields.Delete,
				Remove:              tt.fields.Remove,
				ConditionExpression: tt.fields.ConditionExpression,
			}

			input := &types.Update{}

			err := b.BuildUpdateTransactItem(input)

			require.NoError(t, err)
			require.Equal(t, tt.expectedOutput, input)
		})
	}
}

func Test_marshalList(t *testing.T) {
	type args struct {
		list interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    *types.AttributeValueMemberL
		wantErr bool
	}{
		{
			name: "empty list",
			args: args{
				list: []interface{}{},
			},
			want:    &types.AttributeValueMemberL{Value: nil},
			wantErr: false,
		},
		{
			name: "regular list",
			args: args{
				list: []int{1, 2},
			},
			want:    &types.AttributeValueMemberL{Value: []types.AttributeValue{&types.AttributeValueMemberN{Value: "1"}, &types.AttributeValueMemberN{Value: "2"}}},
			wantErr: false,
		},
		{
			name: "mixed list",
			args: args{
				list: []interface{}{1, &types.AttributeValueMemberS{Value: "SomeString"}},
			},
			want:    &types.AttributeValueMemberL{Value: []types.AttributeValue{&types.AttributeValueMemberN{Value: "1"}, &types.AttributeValueMemberS{Value: "SomeString"}}},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := marshalList(tt.args.list)
			if (err != nil) != tt.wantErr {
				t.Errorf("marshalList() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("marshalList() got = %v, want %v", got, tt.want)
			}
		})
	}
}
