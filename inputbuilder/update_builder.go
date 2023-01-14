package inputbuilder

import (
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/raito-io/go-dynamo-utils/inputbuilder/conditionexpression"
	"github.com/raito-io/go-dynamo-utils/inputbuilder/expressionutils"
	"github.com/raito-io/go-dynamo-utils/inputbuilder/updateexpression"
)

// UpdateBuilder is a builder to create dynamodb.UpdateItemInput and types.Update objects
type UpdateBuilder struct {
	TableName string
	Key       map[string]interface{}

	Set    []*updateexpression.SetOperationItem
	Add    []*updateexpression.AddOperationItem
	Delete []*updateexpression.DeleteOperationItem
	Remove []expressionutils.AttributePath

	ConditionExpression conditionexpression.ExpressionItem
}

// NewUpdateBuilder creates a new and empty UpdateBuilder
func NewUpdateBuilder() *UpdateBuilder {
	return &UpdateBuilder{
		Key: make(map[string]interface{}),
	}
}

// WithTableName sets the table name on the Update Input object
func (b *UpdateBuilder) WithTableName(tableName string) {
	b.TableName = tableName
}

// WithKeyMap sets the key on the Update Input object.
// The key of the map represents the key attribute names. The values of the map represents the attribute values.
// The values of the map are marshalled when the update object is build.
// If a value is of type types.AttributeValue, the marshaling will be skipped and the value is directly used instead.
func (b *UpdateBuilder) WithKeyMap(key map[string]interface{}) {
	b.Key = key
}

// WithKey appends the given key and value pair to the key defining the item to update
// value will be marshalled during when the update object is build.
// If the value is of type types.AttributeValue, the marshaling will be skipped and the value is directly used instead.
func (b *UpdateBuilder) WithKey(attribute expressionutils.AttributePath, value interface{}) {
	b.Key[string(attribute)] = value
}

// AppendSet appends DynamoDB SET operations to the update query
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html#Expressions.UpdateExpressions.SET
func (b *UpdateBuilder) AppendSet(setOperations ...*updateexpression.SetOperationItem) {
	b.Set = append(b.Set, setOperations...)
}

// AppendAdd appends DynamoDB ADD operations to the update query
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html#Expressions.UpdateExpressions.ADD
func (b *UpdateBuilder) AppendAdd(addOperations ...*updateexpression.AddOperationItem) {
	b.Add = append(b.Add, addOperations...)
}

// AppendDelete appends DynamoDB DELETE operations to the update query
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html#Expressions.UpdateExpressions.DELETE
func (b *UpdateBuilder) AppendDelete(deleteOperations ...*updateexpression.DeleteOperationItem) {
	b.Delete = append(b.Delete, deleteOperations...)
}

// AppendRemove appends DynamoDB REMOVE operations to the update query
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html#Expressions.UpdateExpressions.REMOVE
func (b *UpdateBuilder) AppendRemove(removeOperations ...expressionutils.AttributePath) {
	b.Remove = append(b.Remove, removeOperations...)
}

// WithConditionExpression sets the condition expression on the Update Input object
func (b *UpdateBuilder) WithConditionExpression(conditionExpression conditionexpression.ExpressionItem) {
	b.ConditionExpression = conditionExpression
}

func (b *UpdateBuilder) build(tableName **string, key *map[string]types.AttributeValue, updateExpression **string, expressionAttributeNames *map[string]string, expressionAttributeValues *map[string]types.AttributeValue) error { //nolint:gocritic
	if b.TableName == "" && *tableName == nil {
		return errors.New("tableName may not be empty")
	}

	if len(b.Key) == 0 && len(*key) == 0 {
		return errors.New("key may not be empty")
	}

	if b.TableName != "" {
		*tableName = &b.TableName
	}

	if len(b.Key) > 0 {
		if *key == nil {
			*key = make(map[string]types.AttributeValue)
		}

		for keyAttributeName, v := range b.Key {
			if value, ok := v.(types.AttributeValue); ok {
				(*key)[keyAttributeName] = value
			} else {
				value, err := attributevalue.Marshal(v)
				if err != nil {
					return err
				}

				(*key)[keyAttributeName] = value
			}
		}
	}

	expressionAttributeNamesTmp := make(map[string]string)

	var updateExpressionBuilder strings.Builder
	elementsToMarshal := make(map[string]interface{})

	if len(b.Set) > 0 {
		path := expressionutils.OperationPath{
			CurrentOperation: "SET",
		}

		updateExpressionBuilder.WriteString("SET ")

		for i, setOperation := range b.Set {
			if i > 0 {
				updateExpressionBuilder.WriteString(", ")
			}

			updateExpressionBuilder.WriteString(setOperation.Marshal(&path, expressionAttributeNamesTmp, elementsToMarshal))
		}
	}

	if len(b.Add) > 0 {
		path := expressionutils.OperationPath{
			CurrentOperation: "ADD",
		}

		updateExpressionBuilder.WriteString(" ADD ")

		for i, addOperation := range b.Add {
			if i > 0 {
				updateExpressionBuilder.WriteString(", ")
			}

			updateExpressionBuilder.WriteString(addOperation.Marshall(&path, expressionAttributeNamesTmp, elementsToMarshal))
		}
	}

	if len(b.Delete) > 0 {
		path := expressionutils.OperationPath{
			CurrentOperation: "DELETE",
		}

		updateExpressionBuilder.WriteString(" DELETE ")

		for i, deleteOperation := range b.Delete {
			if i > 0 {
				updateExpressionBuilder.WriteString(", ")
			}

			updateExpressionBuilder.WriteString(deleteOperation.Marshall(&path, expressionAttributeNamesTmp, elementsToMarshal))
		}
	}

	if len(b.Remove) > 0 {
		updateExpressionBuilder.WriteString(" REMOVE ")

		for i, removeOperation := range b.Remove {
			if i > 0 {
				updateExpressionBuilder.WriteString(", ")
			}

			updateExpressionBuilder.WriteString(removeOperation.Marshal(expressionAttributeNamesTmp))
		}
	}

	if updateExpressionBuilder.Len() > 0 {
		*updateExpression = aws.String(strings.TrimSpace(updateExpressionBuilder.String()))

		if expressionAttributeNames == nil {
			*expressionAttributeNames = expressionAttributeNamesTmp
		} else {
			*expressionAttributeNames = make(map[string]string)
			for keyAttributeName, value := range expressionAttributeNamesTmp {
				(*expressionAttributeNames)[keyAttributeName] = value
			}
		}

		marshalledValues, err := attributevalue.MarshalMap(elementsToMarshal)
		if err != nil {
			return err
		}

		if *expressionAttributeValues == nil {
			*expressionAttributeValues = marshalledValues
		} else {
			for keyAttributeName, value := range marshalledValues {
				(*expressionAttributeValues)[keyAttributeName] = value
			}
		}
	}

	return nil
}

// BuildUpdateItemInput builds a dynamodb.UpdateItemInput object
func (b *UpdateBuilder) BuildUpdateItemInput(input *dynamodb.UpdateItemInput) error {
	err := b.build(&input.TableName, &input.Key, &input.UpdateExpression, &input.ExpressionAttributeNames, &input.ExpressionAttributeValues)

	return err
}

// BuildUpdateTransactItem builds a types.Update object that can be used in a dynamodb.TransactWriteItemsInput object
func (b *UpdateBuilder) BuildUpdateTransactItem(input *types.Update) error {
	err := b.build(&input.TableName, &input.Key, &input.UpdateExpression, &input.ExpressionAttributeNames, &input.ExpressionAttributeValues)

	return err
}
