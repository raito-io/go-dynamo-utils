package expressionutils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_operationPath_String(t *testing.T) {

	type fields struct {
		currentOperation string
		upperOperation   *OperationPath
		cachedPath       *string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "empty",
			fields: fields{
				currentOperation: "",
				upperOperation:   nil,
				cachedPath:       nil,
			},
			want: "",
		},
		{
			name: "current",
			fields: fields{
				currentOperation: "current",
				upperOperation:   nil,
				cachedPath:       nil,
			},
			want: "current",
		},
		{
			name: "nested",
			fields: fields{
				currentOperation: "nested",
				upperOperation: &OperationPath{
					CurrentOperation: "upper",
				},
			},
			want: "upper_nested",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			p := &OperationPath{
				CurrentOperation: tt.fields.currentOperation,
				UpperOperation:   tt.fields.upperOperation,
				cachedPath:       tt.fields.cachedPath,
			}

			// When
			output := p.String()

			// Then
			require.Equal(t, tt.want, output)
		})
	}
}

func Test_operationPath_Prefix(t *testing.T) {

	type fields struct {
		currentOperation string
		upperOperation   *OperationPath
		cachedPath       *string
	}
	type args struct {
		value string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name: "empty",
			fields: fields{
				currentOperation: "",
				upperOperation:   nil,
			},
			args: args{
				value: "value",
			},
			want: "value",
		},
		{
			name: "non_empty",
			fields: fields{
				currentOperation: "nested",
				upperOperation: &OperationPath{
					CurrentOperation: "upper",
				},
			},
			args: args{
				value: "value",
			},
			want: "upper_nested_value",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			p := &OperationPath{
				CurrentOperation: tt.fields.currentOperation,
				UpperOperation:   tt.fields.upperOperation,
				cachedPath:       tt.fields.cachedPath,
			}

			// When
			output := p.Prefix(tt.args.value)

			// Then
			require.Equal(t, tt.want, output)
		})
	}
}

func Test_operationPath_ExtendPath(t *testing.T) {

	type fields struct {
		currentOperation string
		upperOperation   *OperationPath
		cachedPath       *string
	}
	type args struct {
		operation string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   OperationPath
	}{
		{
			name: "nest_path",
			fields: fields{
				currentOperation: "current_operation",
				upperOperation: &OperationPath{
					CurrentOperation: "first_operation",
				},
			},
			args: args{
				operation: "new_operation",
			},
			want: OperationPath{
				CurrentOperation: "new_operation",
				UpperOperation: &OperationPath{
					CurrentOperation: "current_operation",
					UpperOperation: &OperationPath{
						CurrentOperation: "first_operation",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			p := &OperationPath{
				CurrentOperation: tt.fields.currentOperation,
				UpperOperation:   tt.fields.upperOperation,
				cachedPath:       tt.fields.cachedPath,
			}

			// When
			output := p.ExtendPath(tt.args.operation)

			// Then
			require.Equal(t, &tt.want, output)
		})
	}
}
