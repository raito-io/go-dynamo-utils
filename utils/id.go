package utils

import gonanoid "github.com/matoous/go-nanoid/v2"

type IdGenerator struct {
}

func (IdGenerator) ID() string {
	return gonanoid.Must()
}
