package migrator

import "time"

type metadataObject struct {
	LastJobId uint64 `dynamodbav:"lastJobId"`
}

type migrationObject struct {
	PK          string    `dynamodbav:"PK"`
	ID          uint64    `dynamodbav:"id"`
	Name        string    `dynamodbav:"name"`
	Description string    `dynamodbav:"description"`
	StartTime   time.Time `dynamodbav:"startTime"`
	EndTime     time.Time `dynamodbav:"endTime"`
}
