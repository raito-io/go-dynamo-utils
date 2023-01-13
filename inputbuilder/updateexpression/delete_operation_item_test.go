package updateexpression

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/raito-io/go-dynamo-utils/inputbuilder/expressionutils"
)

func TestDeleteOperationItem_Marshall(t *testing.T) {
	// Given
	o := Delete("AttributePath", "someValue")
	attributeNames := map[string]string{}
	attributeValues := map[string]interface{}{}

	// When
	output := o.Marshall(expressionutils.EmptyPath(), attributeNames, attributeValues)

	// Then
	require.Equal(t, "#AttributePath :attributepath", output)
	require.Equal(t, map[string]string{"#AttributePath": "AttributePath"}, attributeNames)
	require.Equal(t, map[string]interface{}{":attributepath": "someValue"}, attributeValues)
}
