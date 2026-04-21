package rabbitmqamqp

import "testing"

func TestStreamOffsetFromAnnotation(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		v      any
		want   int64
		wantOK bool
	}{
		{"int64", int64(42), 42, true},
		{"int32", int32(7), 7, true},
		{"uint64", uint64(99), 99, true},
		{"uint64 overflow", uint64(1 << 63), 0, false},
		{"string", "nope", 0, false},
		{"nil", nil, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := streamOffsetFromAnnotation(tc.v)
			if ok != tc.wantOK || got != tc.want {
				t.Fatalf("streamOffsetFromAnnotation(%v (%T)) = (%d, %v); want (%d, %v)", tc.v, tc.v, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}
