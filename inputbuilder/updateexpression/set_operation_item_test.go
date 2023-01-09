package updateexpression

import (
	"testing"

	"github.com/stretchr/testify/require"

	"dynamodb_utils/inputbuilder/expressionutils"
)

func TestSetOperationItem_Marshal(t *testing.T) {
	type fields struct {
		Path  expressionutils.AttributePath
		Value interface{}
	}
	type args struct {
		path *expressionutils.OperationPath
	}
	type want struct {
		output          string
		attributeNames  map[string]string
		attributeValues map[string]interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   want
	}{
		{
			name: "set a value",
			fields: fields{
				Path:  "AttributeA",
				Value: "someValue",
			},
			args: args{
				path: expressionutils.EmptyPath(),
			},
			want: want{
				output:          "#AttributeA = :attributea",
				attributeNames:  map[string]string{"#AttributeA": "AttributeA"},
				attributeValues: map[string]interface{}{":attributea": "someValue"},
			},
		},
		{
			name: "set a valueOperation",
			fields: fields{
				Path: "AttributeA",
				Value: &AdditionOperationItem{
					BinaryOperationItem{
						LeftOperand:  expressionutils.AttributePath("AttributeA"),
						RightOperand: 42,
					},
				},
			},
			args: args{
				path: expressionutils.EmptyPath(),
			},
			want: want{
				output:          "#AttributeA = #AttributeA + :attributea_addition_right",
				attributeNames:  map[string]string{"#AttributeA": "AttributeA"},
				attributeValues: map[string]interface{}{":attributea_addition_right": 42},
			},
		},
		{
			name: "set a functionOperation",
			fields: fields{
				Path: "AttributeA",
				Value: &ListAppendOperationItem{
					Path:   "AttributeA",
					Values: []interface{}{"Element1", "Element2"},
				},
			},
			args: args{
				path: expressionutils.EmptyPath(),
			},
			want: want{
				output:          "#AttributeA = list_append(#AttributeA, :attributea_append_attributea)",
				attributeNames:  map[string]string{"#AttributeA": "AttributeA"},
				attributeValues: map[string]interface{}{":attributea_append_attributea": []interface{}{"Element1", "Element2"}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			o := Set(tt.fields.Path, tt.fields.Value)
			attributeNames := make(map[string]string)
			attributeValues := make(map[string]interface{})

			// When
			output := o.Marshal(tt.args.path, attributeNames, attributeValues)

			// Then
			require.Equal(t, tt.want.output, output)
			require.Equal(t, tt.want.attributeNames, attributeNames)
			require.Equal(t, tt.want.attributeValues, attributeValues)
		})
	}
}

func TestAdditionOperationItem_Marshal(t *testing.T) {
	type fields struct {
		LeftOperand  interface{}
		RightOperand interface{}
	}
	type args struct {
		path *expressionutils.OperationPath
	}
	type want struct {
		output          string
		attributeNames  map[string]string
		attributeValues map[string]interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   want
	}{
		{
			name: "attribute + value",
			fields: fields{
				LeftOperand:  expressionutils.AttributePath("AttributeA"),
				RightOperand: 42,
			},
			args: args{
				path: expressionutils.EmptyPath(),
			},
			want: want{
				output:          "#AttributeA + :addition_right",
				attributeNames:  map[string]string{"#AttributeA": "AttributeA"},
				attributeValues: map[string]interface{}{":addition_right": 42},
			},
		},
		{
			name: "attribute + function",
			fields: fields{
				LeftOperand: expressionutils.AttributePath("AttributeA"),
				RightOperand: &IfNotExistsOperationItem{
					Path:  "AttributeB",
					Value: 3.1415,
				},
			},
			args: args{
				path: &expressionutils.OperationPath{
					CurrentOperation: "attributea",
				},
			},
			want: want{
				output:          "#AttributeA + if_not_exists(#AttributeB, :attributea_addition_right_ifnotexists_attributeb)",
				attributeNames:  map[string]string{"#AttributeA": "AttributeA", "#AttributeB": "AttributeB"},
				attributeValues: map[string]interface{}{":attributea_addition_right_ifnotexists_attributeb": 3.1415},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			o := Addition(tt.fields.LeftOperand, tt.fields.RightOperand)
			attributeNames := make(map[string]string)
			attributeValues := make(map[string]interface{})

			// When
			output := o.Marshal(tt.args.path, attributeNames, attributeValues)

			// Then
			require.Equal(t, tt.want.output, output)
			require.Equal(t, tt.want.attributeNames, attributeNames)
			require.Equal(t, tt.want.attributeValues, attributeValues)
		})
	}
}

func TestSubtractionOperationItem_Marshal(t *testing.T) {
	type fields struct {
		LeftOperand  interface{}
		RightOperand interface{}
	}
	type args struct {
		path *expressionutils.OperationPath
	}
	type want struct {
		output          string
		attributeNames  map[string]string
		attributeValues map[string]interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   want
	}{
		{
			name: "attribute - value",
			fields: fields{
				LeftOperand:  expressionutils.AttributePath("AttributeA"),
				RightOperand: 42,
			},
			args: args{
				path: expressionutils.EmptyPath(),
			},
			want: want{
				output:          "#AttributeA - :subtraction_right",
				attributeNames:  map[string]string{"#AttributeA": "AttributeA"},
				attributeValues: map[string]interface{}{":subtraction_right": 42},
			},
		},
		{
			name: "attribute - function",
			fields: fields{
				LeftOperand: expressionutils.AttributePath("AttributeA"),
				RightOperand: &IfNotExistsOperationItem{
					Path:  "AttributeB",
					Value: 3.1415,
				},
			},
			args: args{
				path: &expressionutils.OperationPath{
					CurrentOperation: "attributea",
				},
			},
			want: want{
				output:          "#AttributeA - if_not_exists(#AttributeB, :attributea_subtraction_right_ifnotexists_attributeb)",
				attributeNames:  map[string]string{"#AttributeA": "AttributeA", "#AttributeB": "AttributeB"},
				attributeValues: map[string]interface{}{":attributea_subtraction_right_ifnotexists_attributeb": 3.1415},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			o := Subtraction(tt.fields.LeftOperand, tt.fields.RightOperand)
			attributeNames := make(map[string]string)
			attributeValues := make(map[string]interface{})

			// When
			output := o.Marshal(tt.args.path, attributeNames, attributeValues)

			// Then
			require.Equal(t, tt.want.output, output)
			require.Equal(t, tt.want.attributeNames, attributeNames)
			require.Equal(t, tt.want.attributeValues, attributeValues)
		})
	}
}

func TestListAppendOperationItem_Marshal(t *testing.T) {
	type fields struct {
		Path   expressionutils.AttributePath
		Values []interface{}
	}
	type args struct {
		path *expressionutils.OperationPath
	}
	type want struct {
		output          string
		attributeNames  map[string]string
		attributeValues map[string]interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   want
	}{
		{
			name: "empty list",
			fields: fields{
				Path:   "AttributeA",
				Values: []interface{}{},
			},
			args: args{
				path: &expressionutils.OperationPath{
					CurrentOperation: "attributea",
				},
			},
			want: want{
				output:          "list_append(#AttributeA, :attributea_append_attributea)",
				attributeNames:  map[string]string{"#AttributeA": "AttributeA"},
				attributeValues: map[string]interface{}{":attributea_append_attributea": []interface{}{}},
			},
		},
		{
			name: "append list",
			fields: fields{
				Path:   "AttributeA",
				Values: []interface{}{"SomeString", 42, 3.1415},
			},
			args: args{
				path: &expressionutils.OperationPath{
					CurrentOperation: "attributea",
				},
			},
			want: want{
				output:          "list_append(#AttributeA, :attributea_append_attributea)",
				attributeNames:  map[string]string{"#AttributeA": "AttributeA"},
				attributeValues: map[string]interface{}{":attributea_append_attributea": []interface{}{"SomeString", 42, 3.1415}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			o := ListAppend(tt.fields.Path, tt.fields.Values...)
			attributeNames := make(map[string]string)
			attributeValues := make(map[string]interface{})

			// When
			output := o.Marshal(tt.args.path, attributeNames, attributeValues)

			// Then
			require.Equal(t, tt.want.output, output)
			require.Equal(t, tt.want.attributeNames, attributeNames)
			require.Equal(t, tt.want.attributeValues, attributeValues)
		})
	}
}

func TestIfNotExistsOperationItem_Marshal(t *testing.T) {
	// Given
	o := IfNotExists("AttributeA", 42)
	attributeNames := make(map[string]string)
	attributeValues := make(map[string]interface{})

	// When
	output := o.Marshal(nil, attributeNames, attributeValues)

	// Then
	require.Equal(t, "if_not_exists(#AttributeA, :ifnotexists_attributea)", output)
	require.Equal(t, map[string]string{"#AttributeA": "AttributeA"}, attributeNames)
	require.Equal(t, map[string]interface{}{":ifnotexists_attributea": 42}, attributeValues)
}
