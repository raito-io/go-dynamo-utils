package expressionutils

type OperationPath struct {
	CurrentOperation string
	UpperOperation   *OperationPath

	cachedPath *string
}

func (p *OperationPath) String() string {
	if p == nil {
		return ""
	}

	if p.cachedPath != nil {
		return *p.cachedPath
	}

	path := ""
	if p.UpperOperation != nil {
		path = p.UpperOperation.String() + "_"
	}

	path += p.CurrentOperation

	p.cachedPath = &path

	return *p.cachedPath
}

func (p *OperationPath) Prefix(value string) string {
	nextPath := p.ExtendPath(value)
	return nextPath.String()
}

func (p *OperationPath) ExtendPath(operation string) *OperationPath {
	upperPath := p

	if p != nil && p.CurrentOperation == "" {
		upperPath = nil
	}

	return &OperationPath{
		CurrentOperation: operation,
		UpperOperation:   upperPath,
	}
}

func EmptyPath() *OperationPath {
	return nil
}
