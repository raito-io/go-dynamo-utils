// Code generated by mockery v2.18.0. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// IdGenerator is an autogenerated mock type for the IdGenerator type
type IdGenerator struct {
	mock.Mock
}

type IdGenerator_Expecter struct {
	mock *mock.Mock
}

func (_m *IdGenerator) EXPECT() *IdGenerator_Expecter {
	return &IdGenerator_Expecter{mock: &_m.Mock}
}

// ID provides a mock function with given fields:
func (_m *IdGenerator) ID() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// IdGenerator_ID_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ID'
type IdGenerator_ID_Call struct {
	*mock.Call
}

// ID is a helper method to define mock.On call
func (_e *IdGenerator_Expecter) ID() *IdGenerator_ID_Call {
	return &IdGenerator_ID_Call{Call: _e.mock.On("ID")}
}

func (_c *IdGenerator_ID_Call) Run(run func()) *IdGenerator_ID_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *IdGenerator_ID_Call) Return(_a0 string) *IdGenerator_ID_Call {
	_c.Call.Return(_a0)
	return _c
}

type mockConstructorTestingTNewIdGenerator interface {
	mock.TestingT
	Cleanup(func())
}

// NewIdGenerator creates a new instance of IdGenerator. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewIdGenerator(t mockConstructorTestingTNewIdGenerator) *IdGenerator {
	mock := &IdGenerator{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
