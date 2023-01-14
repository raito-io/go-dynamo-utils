# Distributed Lock
A distributed lock can be used to ensure mutual exclusive access to a specific resource across different running applications.

To acquire a lock, the lock handler will try to create a new lock. This can only succeed if the no lock with the same key exists.
One can overtake a lock (and refresh it) once a specified timeout is passed.

The lock handler can work on a table with only a Partition Key. Or on a table containing a Partition Key and Sort Key.
In the later case, one can argue that we can lock a specific partition on the DynamoDB table.

## Example
```go
func foo(ctx context.Context, client *dynamodb.Client, tablename string, partitionKey string) error {
	lockHandler := distrlock.New(client, tablename, partitionKey, distrlock.WithTimeout(time.Second))
	
	lock, err := lockHandler.Lock(ctx, &types.AttributeValueMemberS{Value: "partitionToLock"})
	if err != nil {
        return err
    }
	
	defer lock.Release(ctx)
	
	// Lock is acquired. You can access specific data in a mutual exclusive way
}
```