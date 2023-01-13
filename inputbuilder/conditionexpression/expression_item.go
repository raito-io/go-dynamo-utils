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

func Exists(attribute expressionutils.AttributePath) *AttributeExistsOperation {
	return &AttributeExistsOperation{
		Attribute: attribute,
	}
}

type AttributeExistsOperation struct {
	Attribute expressionutils.AttributePath
}

var _ ExpressionItem = (*AttributeExistsOperation)(nil)

func (o *AttributeExistsOperation) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	attributeName := o.Attribute.Marshal(attributeNames)

	return fmt.Sprintf("attribute_exists(%s)", attributeName)
}

func (o *AttributeExistsOperation) IsConditionExpressionItem() {}

func NotExists(attribute expressionutils.AttributePath) *AttributeNotExistsOperation {
	return &AttributeNotExistsOperation{
		Attribute: attribute,
	}
}

type AttributeNotExistsOperation struct {
	Attribute expressionutils.AttributePath
}

var _ ExpressionItem = (*AttributeNotExistsOperation)(nil)

func (o *AttributeNotExistsOperation) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	attributeName := o.Attribute.Marshal(attributeNames)

	return fmt.Sprintf("attribute_not_exists(%s)", attributeName)
}

func (o *AttributeNotExistsOperation) IsConditionExpressionItem() {}

func Type(attribute expressionutils.AttributePath, attributeType AttributeType) *AttributeTypeOperation {
	return &AttributeTypeOperation{
		Attribute: attribute,
		Type:      attributeType,
	}
}

type AttributeTypeOperation struct {
	Attribute expressionutils.AttributePath
	Type      AttributeType
}

var _ ExpressionItem = (*AttributeTypeOperation)(nil)

func (o *AttributeTypeOperation) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	attributeName := o.Attribute.Marshal(attributeNames)

	return fmt.Sprintf("attribute_type(%s, %s)", attributeName, o.Type)
}

func BeginsWith(attribute expressionutils.AttributePath, value string) *BeginsWithOperation {
	return &BeginsWithOperation{
		Attribute: attribute,
		Value:     value,
	}
}

type BeginsWithOperation struct {
	Attribute expressionutils.AttributePath
	Value     string
}

var _ ExpressionItem = (*BeginsWithOperation)(nil)

func (o *BeginsWithOperation) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	attributeName := o.Attribute.Marshal(attributeNames)

	attributeValueName := o.Attribute.ValueName(path, 0)
	attributeValues[attributeValueName] = o.Value

	return fmt.Sprintf("begins_with(%s, %s)", attributeName, attributeValueName)
}

func (o *BeginsWithOperation) IsConditionExpressionItem() {}

func (o *BeginsWithOperation) IsRangeKeyConditionExpressionItem() {}

func Contains(attribute expressionutils.AttributePath, value interface{}) *ContainsOperation {
	return &ContainsOperation{
		Attribute: attribute,
		Value:     value,
	}
}

type ContainsOperation struct {
	Attribute expressionutils.AttributePath
	Value     interface{}
}

var _ ExpressionItem = (*ContainsOperation)(nil)

func (o *ContainsOperation) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	attributeName := o.Attribute.Marshal(attributeNames)

	attributeValueName := o.Attribute.ValueName(path, 0)
	attributeValues[attributeValueName] = o.Value

	return fmt.Sprintf("contains(%s, %v)", attributeName, attributeValueName)
}

func (o *ContainsOperation) IsConditionExpressionItem() {}

func Size(attribute expressionutils.AttributePath) *SizeOperand {
	return &SizeOperand{
		Attribute: attribute,
	}
}

type SizeOperand struct {
	Attribute expressionutils.AttributePath
}

var _ ExpressionOperand = (*SizeOperand)(nil)

func (o *SizeOperand) Marshal(attributeNames map[string]string) string {
	attributeName := o.Attribute.Marshal(attributeNames)

	return fmt.Sprintf("size(%s)", attributeName)
}

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
