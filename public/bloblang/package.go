package bloblang

import (
	"github.com/Jeffail/benthos/v3/internal/bloblang/parser"
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
)

// Method defines a Bloblang function that executes on a value. Arguments are
// provided to the constructor, allowing the implementation of this method to
// resolve them statically when possible.
type Method func(v interface{}) (interface{}, error)

// MethodConstructor defines a constructor for a Bloblang method, where a
// variadic list of arguments are provided.
type MethodConstructor func(args ...interface{}) (Method, error)

// Function defines a Bloblang function, arguments are provided to the
// constructor, allowing the implementation of this function to resolve them
// statically when possible.
type Function func() (interface{}, error)

// FunctionConstructor defines a constructor for a Bloblang function, where a
// variadic list of arguments are provided.
type FunctionConstructor func(args ...interface{}) (Function, error)

//------------------------------------------------------------------------------

// Environment provides an isolated Bloblang environment where the available
// features, functions and methods can be modified.
type Environment struct {
	functions *query.FunctionSet
	methods   *query.MethodSet
}

// NewEnvironment creates a fresh Bloblang environment, starting with the full
// range of globally defined features (functions and methods), and provides APIs
// for expanding or contracting the features available to this environment.
//
// It's worth using an environment when you need to restrict the access or
// capabilities that certain bloblang mappings have versus others.
//
// For example, an environment could be created that removes any functions for
// accessing environment variables or reading data from the host disk, which
// could be used in certain situations without removing those functions globally
// for all mappings.
func NewEnvironment() *Environment {
	return &Environment{
		functions: query.AllFunctions.Without(),
		methods:   query.AllMethods.Without(),
	}
}

// Parse a Bloblang mapping using the Environment to determine the features
// (functions and methods) available to the mapping.
func (e *Environment) Parse(blobl string) (*Executor, error) {
	exec, err := parser.ParseMapping("", blobl, parser.Context{
		Functions: e.functions,
		Methods:   e.methods,
	})
	if err != nil {
		return nil, err
	}
	return &Executor{exec}, nil
}

// RegisterMethod adds a new Bloblang method to the environment.
func (e *Environment) RegisterMethod(name string, ctor MethodConstructor) error {
	return e.methods.Add(
		query.NewMethodSpec(name, "").InCategory(query.MethodCategoryPlugin, ""),
		func(target query.Function, args ...interface{}) (query.Function, error) {
			fn, err := ctor(args...)
			if err != nil {
				return nil, err
			}
			return query.ClosureFunction(func(ctx query.FunctionContext) (interface{}, error) {
				v, err := target.Exec(ctx)
				if err != nil {
					return nil, err
				}
				return fn(v)
			}, target.QueryTargets), nil
		},
		false,
	)
}

// RegisterFunction adds a new Bloblang function to the environment.
func (e *Environment) RegisterFunction(name string, ctor FunctionConstructor) error {
	return e.functions.Add(
		query.NewFunctionSpec(query.FunctionCategoryPlugin, name, ""),
		func(args ...interface{}) (query.Function, error) {
			fn, err := ctor(args...)
			if err != nil {
				return nil, err
			}
			return query.ClosureFunction(func(ctx query.FunctionContext) (interface{}, error) {
				return fn()
			}, nil), nil
		},
		false,
	)
}

//------------------------------------------------------------------------------

// Parse a Bloblang mapping allowing the use of the globally accessible range of
// features (functions and methods).
func Parse(blobl string) (*Executor, error) {
	exec, err := parser.ParseMapping("", blobl, parser.Context{
		Functions: query.AllFunctions,
		Methods:   query.AllMethods,
	})
	if err != nil {
		return nil, err
	}
	return &Executor{exec}, nil
}

// RegisterMethod adds a new Bloblang method to the global environment and any
// derivatives.
func RegisterMethod(name string, ctor MethodConstructor) error {
	return query.AllMethods.Add(
		query.NewMethodSpec(name, "").InCategory(query.MethodCategoryPlugin, ""),
		func(target query.Function, args ...interface{}) (query.Function, error) {
			fn, err := ctor(args...)
			if err != nil {
				return nil, err
			}
			return query.ClosureFunction(func(ctx query.FunctionContext) (interface{}, error) {
				v, err := target.Exec(ctx)
				if err != nil {
					return nil, err
				}
				return fn(v)
			}, target.QueryTargets), nil
		},
		true,
	)
}

// RegisterFunction adds a new Bloblang function to the global environment and
// any derivatives.
func RegisterFunction(name string, ctor FunctionConstructor) error {
	return query.AllFunctions.Add(
		query.NewFunctionSpec(query.FunctionCategoryPlugin, name, ""),
		func(args ...interface{}) (query.Function, error) {
			fn, err := ctor(args...)
			if err != nil {
				return nil, err
			}
			return query.ClosureFunction(func(ctx query.FunctionContext) (interface{}, error) {
				return fn()
			}, nil), nil
		},
		true,
	)
}
