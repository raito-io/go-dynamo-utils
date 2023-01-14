package conditionexpression

import (
	"fmt"
	"strings"

	"github.com/raito-io/go-dynamo-utils/inputbuilder/expressionutils"
)

type RangeKeyConditionExpressionItem interface {
	ConditionItem
	IsRangeKeyConditionExpressionItem()
}

type ConditionItem interface {
	ExpressionItem
	IsConditionExpressionItem()
}

type ExpressionItem interface {
	Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string
}

// Exists creates an AttributeExistsOperation object to represent an `attribute_exists (path)` DynamoDB expression
func Exists(path expressionutils.AttributePath) *AttributeExistsOperation {
	return &AttributeExistsOperation{
		Path: path,
	}
}

// AttributeExistsOperation represents an `attribute_exists (path)` DynamoDB expression
type AttributeExistsOperation struct {
	Path expressionutils.AttributePath
}

var _ ExpressionItem = (*AttributeExistsOperation)(nil)

func (o *AttributeExistsOperation) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	attributeName := o.Path.Marshal(attributeNames)

	return fmt.Sprintf("attribute_exists(%s)", attributeName)
}

func (o *AttributeExistsOperation) IsConditionExpressionItem() {}

// NotExists creates an AttributeNotExistsOperation object to represent an `attribute_not_exists (path)` DynamoDB expression
func NotExists(path expressionutils.AttributePath) *AttributeNotExistsOperation {
	return &AttributeNotExistsOperation{
		Path: path,
	}
}

// AttributeNotExistsOperation represents an `attribute_not_exists (path)` DynamoDB expression
type AttributeNotExistsOperation struct {
	Path expressionutils.AttributePath
}

var _ ExpressionItem = (*AttributeNotExistsOperation)(nil)

func (o *AttributeNotExistsOperation) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	attributeName := o.Path.Marshal(attributeNames)

	return fmt.Sprintf("attribute_not_exists(%s)", attributeName)
}

func (o *AttributeNotExistsOperation) IsConditionExpressionItem() {}

// Type creates an AttributeTypeOperation object to represent an `attribute_type (path, type)` DynamoDB expression
func Type(path expressionutils.AttributePath, attributeType AttributeType) *AttributeTypeOperation {
	return &AttributeTypeOperation{
		Path: path,
		Type: attributeType,
	}
}

// AttributeTypeOperation represents an `attribute_type (path, type)` DynamoDB expression
type AttributeTypeOperation struct {
	Path expressionutils.AttributePath
	Type AttributeType
}

var _ ExpressionItem = (*AttributeTypeOperation)(nil)

func (o *AttributeTypeOperation) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	attributeName := o.Path.Marshal(attributeNames)

	return fmt.Sprintf("attribute_type(%s, %s)", attributeName, o.Type)
}

// BeginsWith creates a BeginsWithOperation object to represent a `begins_with (path, substr)` DynamoDB expression
func BeginsWith(path expressionutils.AttributePath, substr string) *BeginsWithOperation {
	return &BeginsWithOperation{
		Path:   path,
		Substr: substr,
	}
}

// BeginsWithOperation represents a `begins_with (path, substr)` DynamoDB expression
type BeginsWithOperation struct {
	Path   expressionutils.AttributePath
	Substr string
}

var _ ExpressionItem = (*BeginsWithOperation)(nil)

func (o *BeginsWithOperation) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	attributeName := o.Path.Marshal(attributeNames)

	attributeValueName := o.Path.ValueName(path, 0)
	attributeValues[attributeValueName] = o.Substr

	return fmt.Sprintf("begins_with(%s, %s)", attributeName, attributeValueName)
}

func (o *BeginsWithOperation) IsConditionExpressionItem() {}

func (o *BeginsWithOperation) IsRangeKeyConditionExpressionItem() {}

// Contains creates a ContainsOperation object to represent a `contains (path, operand)` DynamoDB expression
// value must be a string if the attribute is a string. Otherwise, if the attribute is a set value must be of same type as the elements in the set
func Contains(attribute expressionutils.AttributePath, value interface{}) *ContainsOperation {
	return &ContainsOperation{
		Path:  attribute,
		Value: value,
	}
}

// ContainsOperation represents a `contains (path, operand)` DynamoDB expression
// Value must be a string if the attribute is a string. Otherwise, if the attribute is a set value must be of same type as the elements in the set
type ContainsOperation struct {
	Path  expressionutils.AttributePath
	Value interface{}
}

var _ ExpressionItem = (*ContainsOperation)(nil)

func (o *ContainsOperation) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	attributeName := o.Path.Marshal(attributeNames)

	attributeValueName := o.Path.ValueName(path, 0)
	attributeValues[attributeValueName] = o.Value

	return fmt.Sprintf("contains(%s, %v)", attributeName, attributeValueName)
}

func (o *ContainsOperation) IsConditionExpressionItem() {}

// Size creates a SizeOperand object to represent a `size (path)` DynamoDB expression
func Size(path expressionutils.AttributePath) *SizeOperand {
	return &SizeOperand{
		Path: path,
	}
}

// SizeOperand represents a `size (path)` DynamoDB expression
type SizeOperand struct {
	Path expressionutils.AttributePath
}

var _ ExpressionOperand = (*SizeOperand)(nil)

func (o *SizeOperand) Marshal(attributeNames map[string]string) string {
	attributeName := o.Path.Marshal(attributeNames)

	return fmt.Sprintf("size(%s)", attributeName)
}

// Equal create an EqualComparisonOperator object to represent a `lh = rh` DynamoDB expression
func Equal(leftOperand interface{}, rightOperand interface{}) *EqualComparisonOperator {
	return &EqualComparisonOperator{
		BinaryComparisonOperator{
			LeftOperand:  leftOperand,
			RightOperand: rightOperand,
		},
	}
}

type EqualComparisonOperator struct {
	BinaryComparisonOperator
}

var _ ExpressionItem = (*EqualComparisonOperator)(nil)

func (o *EqualComparisonOperator) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	return o.marshal(path, EqualComparator, attributeNames, attributeValues)
}

// NotEqual creates a NotEqualComparisonOperator object to represent a `lh <> rh` DynamoDB expression
func NotEqual(leftOperand interface{}, rightOperand interface{}) *NotEqualComparisonOperator {
	return &NotEqualComparisonOperator{
		BinaryComparisonOperator{
			LeftOperand:  leftOperand,
			RightOperand: rightOperand,
		},
	}
}

type NotEqualComparisonOperator struct {
	BinaryComparisonOperator
}

var _ ExpressionItem = (*NotEqualComparisonOperator)(nil)

func (o *NotEqualComparisonOperator) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	return o.marshal(path, NotEqualComparator, attributeNames, attributeValues)
}

// LessThan creates a LessThanComparisonOperator object to represent a `lh < rh` DynamoDB expression
func LessThan(leftOperand interface{}, rightOperand interface{}) *LessThanComparisonOperator {
	return &LessThanComparisonOperator{
		BinaryComparisonOperator{
			LeftOperand:  leftOperand,
			RightOperand: rightOperand,
		},
	}
}

type LessThanComparisonOperator struct {
	BinaryComparisonOperator
}

var _ ExpressionItem = (*LessThanComparisonOperator)(nil)

func (o *LessThanComparisonOperator) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	return o.marshal(path, LessThanComparator, attributeNames, attributeValues)
}

// LessOrEqualThan creates a LessOrEqualComparisonOperator object to represent a `lh <= rh` DynamoDB expression
func LessOrEqualThan(leftOperand interface{}, rightOperand interface{}) *LessOrEqualThanComparisonOperator {
	return &LessOrEqualThanComparisonOperator{
		BinaryComparisonOperator{
			LeftOperand:  leftOperand,
			RightOperand: rightOperand,
		},
	}
}

type LessOrEqualThanComparisonOperator struct {
	BinaryComparisonOperator
}

var _ ExpressionItem = (*LessOrEqualThanComparisonOperator)(nil)

func (o *LessOrEqualThanComparisonOperator) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	return o.marshal(path, LessOrEqualThanComparator, attributeNames, attributeValues)
}

// GreaterThan creates a GreaterThanComparisonOperator object to represent a `lh > rh` DynamoDB expression
func GreaterThan(leftOperand interface{}, rightOperand interface{}) *GreaterThanComparisonOperator {
	return &GreaterThanComparisonOperator{
		BinaryComparisonOperator{
			LeftOperand:  leftOperand,
			RightOperand: rightOperand,
		},
	}
}

type GreaterThanComparisonOperator struct {
	BinaryComparisonOperator
}

var _ ExpressionItem = (*GreaterThanComparisonOperator)(nil)

func (o *GreaterThanComparisonOperator) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	return o.marshal(path, GreaterThanComparator, attributeNames, attributeValues)
}

// GreaterOrEqualThan creates a GreaterOrEqualComparisonOperator object to represent a `lh >= rh` DynamoDB
func GreaterOrEqualThan(leftOperand interface{}, rightOperand interface{}) *GreaterOrEqualThanComparisonOperator {
	return &GreaterOrEqualThanComparisonOperator{
		BinaryComparisonOperator{
			LeftOperand:  leftOperand,
			RightOperand: rightOperand,
		},
	}
}

type GreaterOrEqualThanComparisonOperator struct {
	BinaryComparisonOperator
}

var _ ExpressionItem = (*GreaterOrEqualThanComparisonOperator)(nil)

func (o *GreaterOrEqualThanComparisonOperator) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	return o.marshal(path, GreaterOrEqualThanComparator, attributeNames, attributeValues)
}

type BinaryComparisonOperator struct {
	LeftOperand  interface{}
	RightOperand interface{}
}

func (o *BinaryComparisonOperator) marshal(path *expressionutils.OperationPath, comparator Comparator, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	nextPath := path.ExtendPath("binarycomparison")
	leftOperator := MarshalOperand(nextPath, "left", o.LeftOperand, attributeNames, attributeValues)
	rightOperator := MarshalOperand(nextPath, "right", o.RightOperand, attributeNames, attributeValues)

	return fmt.Sprintf("%s %s %s", leftOperator, comparator, rightOperator)
}

func (o *BinaryComparisonOperator) IsRangeKeyConditionExpressionItem() {}

func (o *BinaryComparisonOperator) IsConditionExpressionItem() {}

// Between creates a BetweenComparisonOperator object to represent `a BETWEEN b AND c` DynamoDB expression
func Between(attribute expressionutils.AttributePath, value0 interface{}, value1 interface{}) *BetweenComparisonOperator {
	return &BetweenComparisonOperator{
		LeftOperand:         attribute,
		RightSmallerOperand: value0,
		RightBiggerOperand:  value1,
	}
}

type BetweenComparisonOperator struct {
	LeftOperand         expressionutils.AttributePath
	RightSmallerOperand interface{}
	RightBiggerOperand  interface{}
}

var _ ExpressionItem = (*BetweenComparisonOperator)(nil)

func (o *BetweenComparisonOperator) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	nextPath := path.ExtendPath("between")
	leftOperator := o.LeftOperand.Marshal(attributeNames)
	rightSmallerOperand := MarshalValue(nextPath, "0", o.RightSmallerOperand, attributeValues)
	rightBiggerOperand := MarshalValue(nextPath, "1", o.RightBiggerOperand, attributeValues)

	return fmt.Sprintf("%s BETWEEN %s AND %s", leftOperator, rightSmallerOperand, rightBiggerOperand)
}

func (o *BetweenComparisonOperator) IsRangeKeyConditionExpressionItem() {}

func (o *BetweenComparisonOperator) IsConditionExpressionItem() {}

// In creates a InComparisonOperator object to represent `lh IN (rh...)` DynamoDB expression
func In(attribute expressionutils.AttributePath, collection ...interface{}) *InComparisonOperator {
	return &InComparisonOperator{
		LeftOperand:   attribute,
		RightOperands: collection,
	}
}

type InComparisonOperator struct {
	LeftOperand   expressionutils.AttributePath
	RightOperands []interface{}
}

var _ ExpressionItem = (*InComparisonOperator)(nil)

func (o *InComparisonOperator) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	nextPath := path.ExtendPath("in")
	leftOperator := o.LeftOperand.Marshal(attributeNames)
	rightOperator := make([]string, len(o.RightOperands))

	for i, rightOperand := range o.RightOperands {
		rightOperator[i] = MarshalOperand(nextPath, fmt.Sprintf("%d", i), rightOperand, attributeNames, attributeValues)
	}

	return fmt.Sprintf("%s IN (%s)", leftOperator, strings.Join(rightOperator, ", "))
}

func (o *InComparisonOperator) IsConditionExpressionItem() {}

// And creates a AndBinaryConditionOperator object to and concat two condition expressions representing `lh AND rh` DynamoDB expression
func And(leftCondition ConditionItem, rightCondition ConditionItem) *AndBinaryConditionOperator {
	return &AndBinaryConditionOperator{
		BinaryCondition: BinaryCondition{
			LeftCondition:  leftCondition,
			RightCondition: rightCondition,
		},
	}
}

type AndBinaryConditionOperator struct {
	BinaryCondition
}

var _ ExpressionItem = (*AndBinaryConditionOperator)(nil)

func (o *AndBinaryConditionOperator) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	return o.marshal(path, AndConditionOperation, attributeNames, attributeValues)
}

// Or creates a OrBinaryConditionOperator object to and concat two condition expressions representing `lh OR rh` DynamoDB expression
func Or(leftCondition ConditionItem, rightCondition ConditionItem) *OrBinaryConditionOperator {
	return &OrBinaryConditionOperator{
		BinaryCondition: BinaryCondition{
			LeftCondition:  leftCondition,
			RightCondition: rightCondition,
		},
	}
}

type OrBinaryConditionOperator struct {
	BinaryCondition
}

var _ ExpressionItem = (*OrBinaryConditionOperator)(nil)

func (o *OrBinaryConditionOperator) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	return o.marshal(path, OrConditionOperation, attributeNames, attributeValues)
}

type BinaryCondition struct {
	LeftCondition  ConditionItem
	RightCondition ConditionItem
}

func (o *BinaryCondition) marshal(path *expressionutils.OperationPath, operator BinaryConditionOperation, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	leftCondition := o.LeftCondition.Marshal(path.ExtendPath(string(operator)+"left"), attributeNames, attributeValues)
	rightCondition := o.RightCondition.Marshal(path.ExtendPath(string(operator)+"right"), attributeNames, attributeValues)

	return fmt.Sprintf("(%s %s %s)", leftCondition, operator, rightCondition)
}

func (*BinaryCondition) IsConditionExpressionItem() {}

// Not create a NotCondition object representing a `NOT condition` DynamoDB expression
func Not(condition ConditionItem) *NotCondition {
	return &NotCondition{
		Condition: condition,
	}
}

type NotCondition struct {
	Condition ConditionItem
}

var _ ExpressionItem = (*NotCondition)(nil)

func (o *NotCondition) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	condition := o.Condition.Marshal(path.ExtendPath("NOT"), attributeNames, attributeValues)

	return fmt.Sprintf("NOT (%s)", condition)
}

func (o *NotCondition) IsConditionExpressionItem() {}
