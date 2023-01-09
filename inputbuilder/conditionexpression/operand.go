package conditionexpression

import (
	"strings"

	"dynamodb_utils/inputbuilder/expressionutils"
)

type ExpressionOperand interface {
	Marshal(attributeNames map[string]string) string
}

func MarshalOperand(path *expressionutils.OperationPath, valueNamePostfix string, operand interface{}, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	switch t := operand.(type) {
	case ExpressionOperand:
		return t.Marshal(attributeNames)
	default:
		return MarshalValue(path, valueNamePostfix, operand, attributeValues)
	}
}

func MarshalValue(path *expressionutils.OperationPath, valueNamePostfix string, value interface{}, attributeValues map[string]interface{}) string {
	valueName := AttributeNameToValueName(path, valueNamePostfix)
	attributeValues[valueName] = value
	return valueName
}

func AttributeNameToValueName(path *expressionutils.OperationPath, attributeName string) string {
	name := strings.ToLower(path.Prefix(attributeName))

	return ":" + name
}
