package conditionexpression

type AttributeType string

const (
	AttributeTypeString      AttributeType = "S"
	AttributeTypeStringSet   AttributeType = "SS"
	AttributeTypeNumber      AttributeType = "N"
	AttributeTypeNumberSet   AttributeType = "NS"
	AttributeTypeBinary      AttributeType = "B"
	AttributeTypeBinarySet   AttributeType = "BS"
	AttributeTypeBoolean     AttributeType = "BOOL"
	AttributeTypeBooleanNull AttributeType = "NULL"
	AttributeTypeList        AttributeType = "L"
	AttributeTypeMap         AttributeType = "M"
)

type Comparator string

const (
	EqualComparator              Comparator = "="
	NotEqualComparator           Comparator = "<>"
	LessThanComparator           Comparator = "<"
	LessOrEqualThanComparator    Comparator = "<="
	GreaterThanComparator        Comparator = ">"
	GreaterOrEqualThanComparator Comparator = ">="
)

type BinaryConditionOperation string

const (
	AndConditionOperation BinaryConditionOperation = "AND"
	OrConditionOperation  BinaryConditionOperation = "OR"
)
