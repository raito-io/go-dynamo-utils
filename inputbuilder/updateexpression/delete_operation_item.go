package updateexpression

import (
	"fmt"

	"github.com/raito-io/go-dynamo-utils/inputbuilder/expressionutils"
)

// Delete create a DeleteOperationItem object, representing a single `DELETE` action
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html#Expressions.UpdateExpressions.DELETE
func Delete(path expressionutils.AttributePath, value interface{}) *DeleteOperationItem {
	return &DeleteOperationItem{
		Path:  path,
		Value: value,
	}
}

type DeleteOperationItem struct {
	Path  expressionutils.AttributePath
	Value interface{}
}

func (i *DeleteOperationItem) Marshall(path *expressionutils.OperationPath, attributeNames map[string]string, attributeValues map[string]interface{}) string {
	attributeName := i.Path.Marshal(attributeNames)
	attributeValueName := marshalAttributeValue(path.ExtendPath(i.Path.Name()), i.Value, attributeValues)

	return fmt.Sprintf("%s %s", attributeName, attributeValueName)
}
