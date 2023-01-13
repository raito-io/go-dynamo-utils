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

func ListAppend(path expressionutils.AttributePath, values ...interface{}) *ListAppendOperationItem {
	return &ListAppendOperationItem{
		Path:   path,
		Values: values,
	}
}

type ListAppendOperationItem struct {
	Path   expressionutils.AttributePath
	Values []interface{}
}

func (l *ListAppendOperationItem) Marshal(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	attributeName := l.Path.Marshal(attributeNames)
	attributeValueName := l.Path.ValueName(path.ExtendPath("append"), 0)

	attributeValues[attributeValueName] = l.Values

	return fmt.Sprintf("list_append(%s, %s)", attributeName, attributeValueName)
}

func (l *ListAppendOperationItem) IsFunctionOperation() {}

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
