package updateexpression

import (
	"fmt"

	"github.com/raito-io/go-dynamo-utils/inputbuilder/expressionutils"
)

// Add create an AddOperationItem object to representing a single `ADD` action
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html#Expressions.UpdateExpressions.ADD
func Add(path expressionutils.AttributePath, value interface{}) *AddOperationItem {
	return &AddOperationItem{
		Path:  path,
		Value: value,
	}
}

type AddOperationItem struct {
	Path  expressionutils.AttributePath
	Value interface{}
}

func (i *AddOperationItem) Marshall(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	attributeName := i.Path.Marshal(attributeNames)
	attributeValueName := marshalAttributeValue(path.ExtendPath(i.Path.Name()), i.Value, attributeValues)

	return fmt.Sprintf("%s %s", attributeName, attributeValueName)
}
