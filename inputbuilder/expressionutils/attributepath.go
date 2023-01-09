package expressionutils

import (
	"fmt"
	"regexp"
	"strings"
)

type AttributePath string

func (a AttributePath) Marshal(attributeNames map[string]string) string {
	attributeQueryName := "#" + a.Name()
	attributeNames[attributeQueryName] = string(a)

	return attributeQueryName
}

func (a AttributePath) ValueName(path *OperationPath, i int) string {
	name := a.Name()

	if i > 0 {
		name += fmt.Sprintf("_%d", i)
	}

	return ":" + strings.ToLower(path.Prefix(name))
}

var _nameRegex = regexp.MustCompile(`[^a-zA-Z0-9 ]+`)

func (a AttributePath) Name() string {
	return _nameRegex.ReplaceAllString(string(a), "")
}
