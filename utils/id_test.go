package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIdGenerator_ID(t *testing.T) {
	// Given
	idGenerator := IdGenerator{}

	// When
	id1 := idGenerator.ID()
	id2 := idGenerator.ID()

	// Then
	require.NotEqual(t, id1, id2)
}
