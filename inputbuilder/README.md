# Input Builder
Input builder is a package that provides an easy way to construct dynamic input objects for scan, query and update dynamoDB actions.

There are currenlty three different input builders for three different kind of operations
- Query Input Builder `QueryBuilder`: for building DynamoDB query input objects
- Scan Input Builder `ScanBuilder`: for building DynamoDB scan input objects
- Update Input Builder `UpdateBuilder`: for building DynamoDB update input objects

The focus of the package is to dynamically create filter, condition, query and update expression as well as correct the marshalling of the related values.

## Examples
### Query input builder
```go
func foo() (*dynamodb.QueryInput, error) {
	qb := inputbuilder.NewQueryBuilder()
	qb.WithTableName("SomeTableName")
	qb.WithHashKeyCondition(conditionexpression.Equal("PK", "partitionKeyValue"))
	qb.WithRangeKeyCondition(conditionexpression.BeginsWith("SK", "startOfSK"))
	
	queryInput := dynamodb.QueryInput{}
	
	err := qb.Build(&queryInput)
	if err != nil {
		return nil, err
	}
	
	return &queryInput, nil
}
```

### Scan input builder
```go
func foo() (*dynamodb.ScanInput, error) {
	sb := inputbuilder.NewScanBuilder()
	sb.WithTableName("SomeTableName")
	sb.WithFilterExpression(conditionexpression.And(conditionexpression.Equal("attribute1", "value1")),conditionexpression.NotEqual("attribute2", "value2")))
	
	scanInput := dynamodb.ScanInput{}
	
	err := sb.Build(&scanInput)
	if err != nil {
		return nil, err
	}
	
	return &scanInput, nil
}
```

### Update input builder
```go
func foo() (*dynamodb.UpdateItemInput, error) {
	ub := inputbuilder.NewUpdateBuilder()
	ub.WithTableName("SomeTableName")
	
	ub.WithKey("PK", "partitionKeyValue")
	ub.WithKey("SK", "sortKeyValue")
	
	ub.AppendSet(updateexpression.SET("attribute1", "value1"), 
		updateexpression.SET("attribute2", updateexpression.IfNotExists("attribute2", updateexpression.Addition("attribute2", 7))
    )
	
	updateItemInput := dynamodb.UpdateItemInput{}
	
	err := ub.BuildUpdateItemInput(&updateItemInput)
	if err != nil {
		return nil, err
	}
	
	return &updateItemInput, nil
}
```
