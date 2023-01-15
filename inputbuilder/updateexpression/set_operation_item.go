package updateexpression

import (
	"fmt"
	"strings"

	"github.com/raito-io/go-dynamo-utils/inputbuilder/expressionutils"
)

type OperationItem interface {
	Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string
}

type SetFunctionOperationItem interface {
	OperationItem
	IsFunctionOperation()
}

type ValueOperation interface {
	OperationItem
	IsValueOperation()
}

// Set creates a SetOperationItem object representing a `SET p = value` DynamoDB expression
// value can be a scalar, or it can be a ValueOperation or SetFunctionOperationItem
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html#Expressions.UpdateExpressions.SET
func Set(path expressionutils.AttributePath, value interface{}) *SetOperationItem {
	return &SetOperationItem{
		Path:  path,
		Value: value,
	}
}

type SetOperationItem struct {
	Path  expressionutils.AttributePath
	Value interface{}
}

func (o *SetOperationItem) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	attributeName := o.Path.Marshal(attributeNames)
	rightValueString := marshalValue(path.ExtendPath(o.Path.Name()), o.Value, attributeNames, attributeValues)

	return fmt.Sprintf("%s = %s", attributeName, rightValueString)
}

// Addition creates a AdditionOperationItem representing a `operand + operand` DynamoDB expression.
func Addition(left interface{}, right interface{}) *AdditionOperationItem {
	return &AdditionOperationItem{
		BinaryOperationItem: BinaryOperationItem{
			LeftOperand:  left,
			RightOperand: right,
		},
	}
}

type AdditionOperationItem struct {
	BinaryOperationItem
}

func (o *AdditionOperationItem) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	return o.marshal(AdditionOperation, path.ExtendPath("addition"), attributeNames, attributeValues)
}

// Subtraction creates a SubtractionOperationItem representing a `operand + operand` DynamoDB expression.
func Subtraction(left interface{}, right interface{}) *SubtractionOperationItem {
	return &SubtractionOperationItem{
		BinaryOperationItem: BinaryOperationItem{
			LeftOperand:  left,
			RightOperand: right,
		},
	}
}

type SubtractionOperationItem struct {
	BinaryOperationItem
}

func (o *SubtractionOperationItem) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	return o.marshal(SubtractionOperation, path.ExtendPath("subtraction"), attributeNames, attributeValues)
}

type BinaryOperationItem struct {
	LeftOperand  interface{}
	RightOperand interface{}
}

func (i *BinaryOperationItem) marshal(operation BinaryOperation, path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	leftOperandString := marshalOperand(path.ExtendPath("left"), i.LeftOperand, attributeNames, attributeValues)
	rightOperandString := marshalOperand(path.ExtendPath("right"), i.RightOperand, attributeNames, attributeValues)

	return fmt.Sprintf("%s %s %s", leftOperandString, operation, rightOperandString)
}

func (i *BinaryOperationItem) IsValueOperation() {}

// ListAppend creates a ListAppendOperationItem representing a `list_append(path, values)` DynamoDB expression
func ListAppend(listA interface{}, listB interface{}) *ListAppendOperationItem {
	return &ListAppendOperationItem{
		ListA: listA,
		ListB: listB,
	}
}

type ListAppendOperationItem struct {
	ListA interface{}
	ListB interface{}
}

func (l *ListAppendOperationItem) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	listA := marshalOperand(path.ExtendPath("list_append_0"), l.ListA, attributeNames, attributeValues)
	listB := marshalOperand(path.ExtendPath("list_append_1"), l.ListB, attributeNames, attributeValues)

	return fmt.Sprintf("list_append(%s, %s)", listA, listB)
}

func (l *ListAppendOperationItem) IsFunctionOperation() {}

// IfNotExists creates an IfNotExistsOperationItem object representing a `if_not_exists(path, value)` DynamoDB expression
func IfNotExists(path expressionutils.AttributePath, value interface{}) *IfNotExistsOperationItem {
	return &IfNotExistsOperationItem{
		Path:  path,
		Value: value,
	}
}

type IfNotExistsOperationItem struct {
	Path  expressionutils.AttributePath
	Value interface{}
}

func (l *IfNotExistsOperationItem) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	attributeName := l.Path.Marshal(attributeNames)
	attributeValueName := l.Path.ValueName(path.ExtendPath("ifnotexists"), 0)

	attributeValues[attributeValueName] = l.Value

	return fmt.Sprintf("if_not_exists(%s, %s)", attributeName, attributeValueName)
}

func (l *IfNotExistsOperationItem) IsFunctionOperation() {}

func marshalValue(path *expressionutils.OperationPath, operand interface{}, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	switch t := operand.(type) {
	case ValueOperation:
		return t.Marshal(path, attributeNames, attributeValues)
	default:
		return marshalOperand(path, operand, attributeNames, attributeValues)
	}
}

func marshalOperand(path *expressionutils.OperationPath, operand interface{}, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	switch t := operand.(type) {
	case SetFunctionOperationItem:
		return t.Marshal(path, attributeNames, attributeValues)
	case expressionutils.AttributePath:
		return t.Marshal(attributeNames)
	default:
		return marshalAttributeValue(path, operand, attributeValues)
	}
}

func marshalAttributeValue(path *expressionutils.OperationPath, value interface{}, attributeValues map[string]interface{}) string {
	valueName := ":" + strings.ToLower(path.String())
	attributeValues[valueName] = value

	return valueName
}

// SetIfNotExists set attribute if not exist.
// This operation creates a SetOperationItem representing `path = if_not_exists(path, value)` as DynamoDB expression
func SetIfNotExists(path expressionutils.AttributePath, value interface{}) *SetOperationItem {
	return Set(path, IfNotExists(path, value))
}

// SetUpsertToList upsert values to a list.
// This operation creates a SetOperationItem representing `path = list_append(if_not_exists(path, :empty_list), values)` as DynamoDB expression
func SetUpsertToList[I any](path expressionutils.AttributePath, values ...I) *SetOperationItem {
	return Set(path, ListAppend(IfNotExists(path, []interface{}{}), values))
}

// SetIncrement increment attribute
// This operation create a SetOperationItem representing `path = path + value` as DynamoDB expression
// value could be a scalar or an Operand
func SetIncrement(path expressionutils.AttributePath, value interface{}) *SetOperationItem {
	return Set(path, Addition(path, value))
}

// SetDecrement decrements attribute
// This operation create a SetOperationItem representing `path = path - value` as DynamoDB expression
// value could be a scalar or an Operand
func SetDecrement(path expressionutils.AttributePath, value interface{}) *SetOperationItem {
	return Set(path, Subtraction(path, value))
}
