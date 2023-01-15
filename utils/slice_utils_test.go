package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsSlice(t *testing.T) {
	type args struct {
		v interface{}
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "isSlice",
			args: args{
				v: []int{1, 2, 3},
			},
			want: true,
		},
		{
			name: "isNotSlice",
			args: args{
				v: 3,
			},
			want: false,
		},
		{
			name: "isNill",
			args: args{
				v: nil,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsSlice(tt.args.v)
			require.Equalf(t, tt.want, got, "IsSlice() = %v, want %v", got, tt.want)
		})
	}
}
