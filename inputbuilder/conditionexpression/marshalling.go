package conditionexpression

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"dynamodb_utils/inputbuilder/expressionutils"
)

func Marshal(item ExpressionItem, attributeNames map[string]string, attributeValues map[string]types.AttributeValue) (*string, error) {
	if item == nil {
		return nil, nil
	}

	attributeToMarshal := make(map[string]interface{})
	expressionString := item.Marshal(expressionutils.EmptyPath(), attributeNames, attributeToMarshal)

	for key, value := range attributeToMarshal {
		switch t := value.(type) {
		case types.AttributeValue:
			attributeValues[key] = t
		default:
			attributeValue, err := attributevalue.Marshal(t)
			if err != nil {
				return nil, err
			}
			attributeValues[key] = attributeValue
		}
	}

	return &expressionString, nil
}
