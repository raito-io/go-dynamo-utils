package executor

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"dynamodb_utils/distrlock"
)

// Interface validation check
var _ DynamodbClient = (*dynamodb.Client)(nil)

//go:generate go run github.com/vektra/mockery/v2 --name=DynamodbClient --with-expecter
type DynamodbClient interface {
	Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(options *dynamodb.Options)) (*dynamodb.ScanOutput, error)
}

// Interface validation check
var _ Lock = (*distrlock.Lock)(nil)

//go:generate go run github.com/vektra/mockery/v2 --name=Lock --with-expecter
type Lock interface {
	Refresh(ctx context.Context) error
}

type Executor struct {
	client DynamodbClient
}

type Options struct {
	// MapFn is used to map all returned elements in the executor
	MapFn func(map[string]types.AttributeValue) (interface{}, error)

	// Lock if not nil Lock is refreshed after every call
	// Note there are no guarantees that retrieved data is still locked by the lock
	Lock Lock
}

func NewExecutor(client DynamodbClient) *Executor {
	return &Executor{client: client}
}

func (e *Executor) Query(ctx context.Context, query *dynamodb.QueryInput, optFns ...func(options *Options)) chan interface{} {
	executionFn := e.queryExecution
	getItemFn := e.queryGetItems
	nextpageFn := e.queryNextPage

	return execute(ctx, query, optFns, executionFn, getItemFn, nextpageFn)
}

func (e *Executor) Scan(ctx context.Context, query *dynamodb.ScanInput, optFns ...func(options *Options)) chan interface{} {
	executionFn := e.scanExecution
	getItemFn := e.scanGetItems
	nextpageFn := e.scanNextPage

	return execute(ctx, query, optFns, executionFn, getItemFn, nextpageFn)
}

func WithMapFn(mapFn func(map[string]types.AttributeValue) (interface{}, error)) func(options *Options) {
	return func(options *Options) {
		options.MapFn = mapFn
	}
}

func WithUnmarhshalToItemMapFn[T any]() func(options *Options) {
	return WithMapFn(func(m map[string]types.AttributeValue) (interface{}, error) {
		var item T
		err := attributevalue.UnmarshalMap(m, &item)

		return item, err
	})
}

func WithLock(lock Lock) func(options *Options) {
	return func(options *Options) {
		options.Lock = lock
	}
}

func defaultMapFn(m map[string]types.AttributeValue) (interface{}, error) {
	return m, nil
}

type executionInput interface {
	dynamodb.QueryInput | dynamodb.ScanInput
}

type executionOutput interface {
	dynamodb.QueryOutput | dynamodb.ScanOutput
}

func execute[I executionInput, R executionOutput](ctx context.Context, operation *I, optFns []func(options *Options),
	executionFn func(context.Context, *I) (*R, error), getItemsFn func(*R) []map[string]types.AttributeValue, nextPageFn func(*I, *R) (*I, bool)) chan interface{} {
	outputChannel := make(chan interface{}, 1)

	go func() {
		defer close(outputChannel)

		var options Options
		parseOptions(&options, optFns...)

		publishOnChannel := func(outputItem interface{}) bool {
			select {
			case <-ctx.Done():
				return false
			case outputChannel <- outputItem:
				return true
			}
		}

		for {
			result, err := executionFn(ctx, operation)

			if err != nil {
				publishOnChannel(err)

				return
			}

			if options.Lock != nil {
				err = options.Lock.Refresh(ctx)

				if err != nil {
					publishOnChannel(err)

					return
				}
			}

			items := getItemsFn(result)

			for i := range items {
				outputItem, err := options.MapFn(items[i])
				if err != nil {
					success := publishOnChannel(err)
					if !success {
						return
					}
				}

				if outputItem != nil {
					success := publishOnChannel(outputItem)

					if !success {
						return
					}
				}
			}

			var loadNextPage bool
			operation, loadNextPage = nextPageFn(operation, result)

			if !loadNextPage {
				return
			}
		}
	}()

	return outputChannel
}

func parseOptions(options *Options, optFns ...func(options *Options)) {
	if options.MapFn == nil {
		options.MapFn = defaultMapFn
	}

	for _, optFn := range optFns {
		optFn(options)
	}
}
