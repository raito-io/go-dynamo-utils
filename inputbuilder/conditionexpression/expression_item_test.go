package conditionexpression

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/raito-io/go-dynamo-utils/inputbuilder/expressionutils"
)

func TestAttributeName_Marshal(t *testing.T) {

	// Given
	attributeName := expressionutils.AttributePath("AttributePath")
	attributeNames := make(map[string]string)

	// When
	output := attributeName.Marshal(attributeNames)

	// Then
	require.Equal(t, "#AttributePath", output)
	require.Equal(t, map[string]string{"#AttributePath": "AttributePath"}, attributeNames)
}

func TestAttributeName_ValueName(t *testing.T) {

	type args struct {
		path *expressionutils.OperationPath
		i    int
	}
	tests := []struct {
		name string
		a    expressionutils.AttributePath
		args args
		want string
	}{
		{
			name: "simple",
			a:    expressionutils.AttributePath("AttributePath"),
			args: args{
				path: &expressionutils.OperationPath{},
				i:    0,
			},
			want: ":attributepath",
		},
		{
			name: "Include Path",
			a:    expressionutils.AttributePath("AttributePath"),
			args: args{
				path: &expressionutils.OperationPath{
					CurrentOperation: "current_operation",
					UpperOperation: &expressionutils.OperationPath{
						CurrentOperation: "first_operation",
					},
				},
				i: 0,
			},
			want: ":first_operation_current_operation_attributepath",
		},
		{
			name: "Include number",
			a:    expressionutils.AttributePath("AttributePath"),
			args: args{
				path: &expressionutils.OperationPath{},
				i:    1,
			},
			want: ":attributepath_1",
		},
		{
			name: "Include number and path",
			a:    expressionutils.AttributePath("AttributePath"),
			args: args{
				path: &expressionutils.OperationPath{
					CurrentOperation: "current_operation",
					UpperOperation: &expressionutils.OperationPath{
						CurrentOperation: "first_operation",
					},
				},
				i: 2,
			},
			want: ":first_operation_current_operation_attributepath_2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.a.ValueName(tt.args.path, tt.args.i), tt.want)
		})
	}
}

func TestAttributeExistsOperation_Marshal(t *testing.T) {

	// Given
	operation := Exists("AttributeA")
	attributeNames := make(map[string]string)
	attributeValues := make(map[string]interface{})
	path := expressionutils.EmptyPath()

	// When
	output := operation.Marshal(path, attributeNames, attributeValues)

	// Then
	require.Equal(t, "attribute_exists(#AttributeA)", output)
	require.Equal(t, map[string]string{"#AttributeA": "AttributeA"}, attributeNames)
	require.Empty(t, attributeValues)
}

func TestAttributeNotExistsOperation_Marshal(t *testing.T) {

	// Given
	operation := NotExists("AttributeA")
	attributeNames := make(map[string]string)
	attributeValues := make(map[string]interface{})
	path := expressionutils.EmptyPath()

	// When
	output := operation.Marshal(path, attributeNames, attributeValues)

	// Then
	require.Equal(t, "attribute_not_exists(#AttributeA)", output)
	require.Equal(t, map[string]string{"#AttributeA": "AttributeA"}, attributeNames)
	require.Empty(t, attributeValues)
}

func TestAttributeTypeOperation_Marshal(t *testing.T) {

	// Given
	operation := Type("AttributeA", AttributeTypeString)
	attributeNames := make(map[string]string)
	attributeValues := make(map[string]interface{})
	path := expressionutils.EmptyPath()

	// When
	output := operation.Marshal(path, attributeNames, attributeValues)

	// Then
	require.Equal(t, "attribute_type(#AttributeA, S)", output)
	require.Equal(t, map[string]string{"#AttributeA": "AttributeA"}, attributeNames)
	require.Empty(t, attributeValues)
}

func TestBeginsWithOperation_Marshal(t *testing.T) {

	// Given
	operation := BeginsWith("AttributeA", "ValueA")
	attributeNames := make(map[string]string)
	attributeValues := make(map[string]interface{})
	path := expressionutils.EmptyPath()

	// When
	output := operation.Marshal(path, attributeNames, attributeValues)

	// Then
	require.Equal(t, "begins_with(#AttributeA, :attributea)", output)
	require.Equal(t, map[string]string{"#AttributeA": "AttributeA"}, attributeNames)
	require.Equal(t, map[string]interface{}{":attributea": "ValueA"}, attributeValues)
}

func TestContainsOperation_Marshal(t *testing.T) {

	// Given
	operation := Contains("AttributeA", "ValueA")
	attributeNames := make(map[string]string)
	attributeValues := make(map[string]interface{})
	path := expressionutils.EmptyPath()

	// When
	output := operation.Marshal(path, attributeNames, attributeValues)

	// Then
	require.Equal(t, "contains(#AttributeA, :attributea)", output)
	require.Equal(t, map[string]string{"#AttributeA": "AttributeA"}, attributeNames)
	require.Equal(t, map[string]interface{}{":attributea": "ValueA"}, attributeValues)
}

func TestSizeOperand_Marshal(t *testing.T) {

	// Given
	operation := Size("AttributeA")
	attributeNames := make(map[string]string)

	// When
	output := operation.Marshal(attributeNames)

	// Then
	require.Equal(t, "size(#AttributeA)", output)
	require.Equal(t, map[string]string{"#AttributeA": "AttributeA"}, attributeNames)
}

func TestEqualOperation_Marshal(t *testing.T) {

	// Given
	operation := Equal(expressionutils.AttributePath("AttributeA"), 42)
	attributeNames := make(map[string]string)
	attributeValues := make(map[string]interface{})

	// When
	output := operation.Marshal(expressionutils.EmptyPath(), attributeNames, attributeValues)

	// Then
	require.Equal(t, "#AttributeA = :binarycomparison_right", output)
	require.Equal(t, map[string]string{"#AttributeA": "AttributeA"}, attributeNames)
	require.Equal(t, map[string]interface{}{":binarycomparison_right": 42}, attributeValues)
}

func TestNotEqualOperation_Marshal(t *testing.T) {

	// Given
	operation := NotEqual(expressionutils.AttributePath("AttributeA"), 42)
	attributeNames := make(map[string]string)
	attributeValues := make(map[string]interface{})

	// When
	output := operation.Marshal(expressionutils.EmptyPath(), attributeNames, attributeValues)

	// Then
	require.Equal(t, "#AttributeA <> :binarycomparison_right", output)
	require.Equal(t, map[string]string{"#AttributeA": "AttributeA"}, attributeNames)
	require.Equal(t, map[string]interface{}{":binarycomparison_right": 42}, attributeValues)
}

func TestLessThanComparisonOperator_Marshal(t *testing.T) {

	// Given
	operation := LessThan(expressionutils.AttributePath("AttributeA"), 42)
	attributeNames := make(map[string]string)
	attributeValues := make(map[string]interface{})

	// When
	output := operation.Marshal(expressionutils.EmptyPath(), attributeNames, attributeValues)

	// Then
	require.Equal(t, "#AttributeA < :binarycomparison_right", output)
	require.Equal(t, map[string]string{"#AttributeA": "AttributeA"}, attributeNames)
	require.Equal(t, map[string]interface{}{":binarycomparison_right": 42}, attributeValues)
}

func TestLessOrEqualThanComparisonOperator_Marshal(t *testing.T) {

	// Given
	operation := LessOrEqualThan(expressionutils.AttributePath("AttributeA"), 42)
	attributeNames := make(map[string]string)
	attributeValues := make(map[string]interface{})

	// When
	output := operation.Marshal(expressionutils.EmptyPath(), attributeNames, attributeValues)

	// Then
	require.Equal(t, "#AttributeA <= :binarycomparison_right", output)
	require.Equal(t, map[string]string{"#AttributeA": "AttributeA"}, attributeNames)
	require.Equal(t, map[string]interface{}{":binarycomparison_right": 42}, attributeValues)
}

func TestGreaterThanComparisonOperator_Marshal(t *testing.T) {

	// Given
	operation := GreaterThan(expressionutils.AttributePath("AttributeA"), 42)
	attributeNames := make(map[string]string)
	attributeValues := make(map[string]interface{})

	// When
	output := operation.Marshal(expressionutils.EmptyPath(), attributeNames, attributeValues)

	// Then
	require.Equal(t, "#AttributeA > :binarycomparison_right", output)
	require.Equal(t, map[string]string{"#AttributeA": "AttributeA"}, attributeNames)
	require.Equal(t, map[string]interface{}{":binarycomparison_right": 42}, attributeValues)
}

func TestGreaterOrEqualThanComparisonOperator_Marshal(t *testing.T) {

	// Given
	operation := GreaterOrEqualThan(expressionutils.AttributePath("AttributeA"), 42)
	attributeNames := make(map[string]string)
	attributeValues := make(map[string]interface{})

	// When
	output := operation.Marshal(expressionutils.EmptyPath(), attributeNames, attributeValues)

	// Then
	require.Equal(t, "#AttributeA >= :binarycomparison_right", output)
	require.Equal(t, map[string]string{"#AttributeA": "AttributeA"}, attributeNames)
	require.Equal(t, map[string]interface{}{":binarycomparison_right": 42}, attributeValues)
}

func TestBetweenComparisonOperator_Marshal(t *testing.T) {

	// Given
	o := Between("AttributeA", 3.1415, 42)
	attributeNames := make(map[string]string)
	attributeValues := make(map[string]interface{})

	// When
	output := o.Marshal(nil, attributeNames, attributeValues)

	// Then
	require.Equal(t, "#AttributeA BETWEEN :between_0 AND :between_1", output)
	require.Equal(t, map[string]string{"#AttributeA": "AttributeA"}, attributeNames)
	require.Equal(t, map[string]interface{}{":between_0": 3.1415, ":between_1": 42}, attributeValues)
}

func TestInComparisonOperator_Marshal(t *testing.T) {
	type fields struct {
		LeftOperand   expressionutils.AttributePath
		RightOperands []interface{}
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
			name: "Single element",
			fields: fields{
				LeftOperand:   expressionutils.AttributePath("AttributeA"),
				RightOperands: []interface{}{"value"},
			},
			args: args{
				path: nil,
			},
			want: want{
				output:          "#AttributeA IN (:in_0)",
				attributeNames:  map[string]string{"#AttributeA": "AttributeA"},
				attributeValues: map[string]interface{}{":in_0": "value"},
			},
		},
		{
			name: "Multiple elements",
			fields: fields{
				LeftOperand:   expressionutils.AttributePath("AttributeA"),
				RightOperands: []interface{}{"value", 3.1415, 42},
			},
			args: args{
				path: &expressionutils.OperationPath{
					CurrentOperation: "parent_operation",
				},
			},
			want: want{
				output:          "#AttributeA IN (:parent_operation_in_0, :parent_operation_in_1, :parent_operation_in_2)",
				attributeNames:  map[string]string{"#AttributeA": "AttributeA"},
				attributeValues: map[string]interface{}{":parent_operation_in_0": "value", ":parent_operation_in_1": 3.1415, ":parent_operation_in_2": 42},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			o := In(tt.fields.LeftOperand, tt.fields.RightOperands...)
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

func TestAndBinaryConditionOperator_Marshal(t *testing.T) {

	// Given
	o := And(&LessThanComparisonOperator{
		BinaryComparisonOperator: BinaryComparisonOperator{
			LeftOperand:  expressionutils.AttributePath("AttributeA"),
			RightOperand: 42,
		},
	},
		&AttributeExistsOperation{
			Attribute: "AttributeB",
		},
	)
	attributeNames := make(map[string]string)
	attributeValues := make(map[string]interface{})
	path := expressionutils.OperationPath{
		CurrentOperation: "current_path",
	}

	// When
	output := o.Marshal(&path, attributeNames, attributeValues)

	// Then
	require.Equal(t, "(#AttributeA < :current_path_andleft_binarycomparison_right AND attribute_exists(#AttributeB))", output)
	require.Equal(t, map[string]string{"#AttributeA": "AttributeA", "#AttributeB": "AttributeB"}, attributeNames)
	require.Equal(t, map[string]interface{}{":current_path_andleft_binarycomparison_right": 42}, attributeValues)
}

func TestOrBinaryConditionOperator_Marshal(t *testing.T) {

	// Given
	o := Or(&LessThanComparisonOperator{
		BinaryComparisonOperator: BinaryComparisonOperator{
			LeftOperand:  expressionutils.AttributePath("AttributeA"),
			RightOperand: 42,
		},
	},
		&AttributeExistsOperation{
			Attribute: "AttributeB",
		},
	)
	attributeNames := make(map[string]string)
	attributeValues := make(map[string]interface{})
	paht := expressionutils.OperationPath{
		CurrentOperation: "current_path",
	}

	// When
	output := o.Marshal(&paht, attributeNames, attributeValues)

	// Then
	require.Equal(t, "(#AttributeA < :current_path_orleft_binarycomparison_right OR attribute_exists(#AttributeB))", output)
	require.Equal(t, map[string]string{"#AttributeA": "AttributeA", "#AttributeB": "AttributeB"}, attributeNames)
	require.Equal(t, map[string]interface{}{":current_path_orleft_binarycomparison_right": 42}, attributeValues)
}

func TestNotCondition_Marshal(t *testing.T) {

	//given
	o := Not(&LessThanComparisonOperator{
		BinaryComparisonOperator: BinaryComparisonOperator{
			LeftOperand:  &SizeOperand{Attribute: expressionutils.AttributePath("AttributeA")},
			RightOperand: 42,
		},
	},
	)
	attributeNames := make(map[string]string)
	attributeValues := make(map[string]interface{})
	path := expressionutils.OperationPath{
		CurrentOperation: "current_path",
	}

	// When
	output := o.Marshal(&path, attributeNames, attributeValues)

	// Then
	require.Equal(t, "NOT (size(#AttributeA) < :current_path_not_binarycomparison_right)", output)
	require.Equal(t, map[string]string{"#AttributeA": "AttributeA"}, attributeNames)
	require.Equal(t, map[string]interface{}{":current_path_not_binarycomparison_right": 42}, attributeValues)
}
