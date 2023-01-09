package updateexpression

import (
	"testing"

	"github.com/stretchr/testify/require"

	"dynamodb_utils/inputbuilder/expressionutils"
)

func TestAddOperationItem_Marshall(t *testing.T) {
	// Given
	o := Add("AttributeA[0]", 42)
	attributeNames := make(map[string]string)
	attributeValues := make(map[string]interface{})

	// When
	output := o.Marshall(expressionutils.EmptyPath(), attributeNames, attributeValues)

	// Then
	require.Equal(t, "#AttributeA0 :attributea0", output)
	require.Equal(t, map[string]string{"#AttributeA0": "AttributeA[0]"}, attributeNames)
	require.Equal(t, map[string]interface{}{":attributea0": 42}, attributeValues)
}
