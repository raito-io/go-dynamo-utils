# Migrator
The Migrator is a tool to execute schema migrations of dynamodb tables.

## Requirements

The Migrator tool requires on a dedicated Metadata dynamodb table.
The migration tool should have the following permissions on the migration metadata table :
- dynamodb:GetItem
- dynamodb:PutItem
- dynamodb:UpdateItem



## Examples
### Execute a schema migration
```go
const migrationMetadataTable = "migrationMetadata"

func ExecuteMigrations(ctx context.Context, client *dynamodb.Client) error {
	migrator := migrator.NewMigrator(migrationMetadataTable, 0, migrator.Migration{
		Name: "InitialMigration",
		Description: "Initial migration",
		MigratorFn: func(_ context.Context, _ migrator.DynamoDBClient) error {
			return nil
        }
    },
	    migrator.Must(migrator.NewScanAndUpdateMigration("SecondMigration", "Second migration", "tableToMigrate", 
			func(ctx context.Context, item map[string]types.AttributeValue) *dynamodb.UpdateItemInput) {
                //implement the migration for an attribute here
            }
        ),
    }
	
	return migrator.Execute(ctx, client)
```
