package consistentHash

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestMap_Add(t *testing.T) {
	type args struct {
		nodes []string
	}
	tests := []struct {
		name    string
		args    args
		wantLen int
	}{
		{
			name: "正常",
			args: args{
				nodes: []string{"node1", "node2", "node3", "node4", "node5"},
			},
			wantLen: 50,
		},
		{
			name: "有重复",
			args: args{
				nodes: []string{"node1", "node2", "node3", "node4", "node5", "node5"},
			},
			wantLen: 50,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New(10, crc32.ChecksumIEEE)
			for _, node := range tt.args.nodes {
				m.Add(node)
			}

			assert.Equal(t, tt.wantLen, len(m.nodes))
			t.Log(m.nodes)
			t.Log(m.hashMap)
		})
	}
}

func TestMap_Get(t *testing.T) {
	type args struct {
		key   string
		nodes []string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "正常",
			args: args{
				key:   "key1",
				nodes: []string{"node1", "node2", "node3", "node4", "node5"},
			},
			want: "node5",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New(10, crc32.ChecksumIEEE)
			for _, node := range tt.args.nodes {
				m.Add(node)
			}
			assert.Equal(t, tt.want, m.Get(tt.args.key))
		})
	}
}
