// Code generated by mockery v2.37.1. DO NOT EDIT.

package mocks

import (
	context "context"

	mock "github.com/stretchr/testify/mock"
)

// Lock is an autogenerated mock type for the Lock type
type Lock struct {
	mock.Mock
}

type Lock_Expecter struct {
	mock *mock.Mock
}

func (_m *Lock) EXPECT() *Lock_Expecter {
	return &Lock_Expecter{mock: &_m.Mock}
}

// Refresh provides a mock function with given fields: ctx
func (_m *Lock) Refresh(ctx context.Context) error {
	ret := _m.Called(ctx)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context) error); ok {
		r0 = rf(ctx)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Lock_Refresh_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Refresh'
type Lock_Refresh_Call struct {
	*mock.Call
}

// Refresh is a helper method to define mock.On call
//   - ctx context.Context
func (_e *Lock_Expecter) Refresh(ctx interface{}) *Lock_Refresh_Call {
	return &Lock_Refresh_Call{Call: _e.mock.On("Refresh", ctx)}
}

func (_c *Lock_Refresh_Call) Run(run func(ctx context.Context)) *Lock_Refresh_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *Lock_Refresh_Call) Return(_a0 error) *Lock_Refresh_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *Lock_Refresh_Call) RunAndReturn(run func(context.Context) error) *Lock_Refresh_Call {
	_c.Call.Return(run)
	return _c
}

// NewLock creates a new instance of Lock. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewLock(t interface {
	mock.TestingT
	Cleanup(func())
}) *Lock {
	mock := &Lock{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
