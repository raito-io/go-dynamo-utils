# Executor
The executor package providers a DynamoDB Query/Scan executor.
The executor returns a channel containing the returning elements or corresponding errors.

The executor abstracts the paginated complexity that can occurs during a DynamoDB query or scan execution, 
as well as the unmarshalling of all objects.
The executor and corresponding channel can be closed by the provided context.

## Example
```go
type DBObject struct {
    PK         string `dynamodbav:"PK"`
    SK         string `dynamodbav:"SK"`
    Attribute1 string `dynamodbav:"attr1,omitempty"`
    Attribute2 int    `dynamodbav:"attr2,omitempty"`
}

func query(ctx context.Context, client *dynamodb.Client, query *dynamodb.QueryInput) error {
	e := executor.New(client)
	
	queryContext, cancelFn := context.WithCancel(ctx)
	defer cancelFn()
	
	for object := range e.Query(queryContext, query, executor.WithUnmarshalToItemMapFn[DBOject]()) {
		switch o := object.(type) {
		case error:
		    return o
		case DBObject:
		    fmt.Printf("Get element of partition %s: %+v\n", o.PK, o)
        } 
    }
	
}
```