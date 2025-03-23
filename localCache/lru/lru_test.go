package lru

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type testValue struct {
	value string
}

func (tv testValue) Len() int {
	return len(tv.value)
}

func TestCache_Get(t *testing.T) {
	type args struct {
		maxBytes   int64
		key        string
		value      Value
		expireTime int64
	}
	tests := []struct {
		name      string
		args      args
		prepare   func(lru *Cache, arg args)
		wantValue interface{}
		wantOk    bool
	}{
		{
			name: "缓存命中",
			args: args{
				maxBytes:   1000,
				key:        "test_key",
				value:      testValue{"test_value"},
				expireTime: time.Now().Add(time.Second * 10).Unix(),
			},
			prepare: func(lru *Cache, arg args) {
				lru.Add(arg.key, arg.value, arg.expireTime)
			},
			wantValue: testValue{"test_value"},
			wantOk:    true,
		},
		{
			name: "缓存未命中",
			args: args{
				maxBytes:   1000,
				key:        "test_key",
				value:      testValue{"test_value"},
				expireTime: time.Now().Add(time.Second * 10).Unix(),
			},
			prepare: func(lru *Cache, arg args) {
			},
			wantValue: nil,
			wantOk:    false,
		},
		{
			name: "键值对过期",
			args: args{
				maxBytes:   3,
				key:        "test_key",
				value:      testValue{"test_value"},
				expireTime: 100,
			},
			prepare: func(lru *Cache, arg args) {
				lru.Add(arg.key, arg.value, arg.expireTime)
			},
			wantValue: nil,
			wantOk:    false,
		},
		{
			name: "值被更新",
			args: args{
				maxBytes:   1000,
				key:        "test_key",
				value:      testValue{"test_value"},
				expireTime: time.Now().Add(time.Second * 10).Unix(),
			},
			prepare: func(lru *Cache, arg args) {
				lru.Add(arg.key, arg.value, arg.expireTime)
				lru.Add(arg.key, testValue{"test_value_update"}, arg.expireTime)
			},
			wantValue: testValue{"test_value_update"},
			wantOk:    true,
		},
		{
			name: "kv对被LRU算法淘汰了",
			args: args{
				maxBytes:   int64(len("test_new_value")),
				key:        "test_key",
				value:      testValue{"test_value"},
				expireTime: time.Now().Add(time.Second * 10).Unix(),
			},
			prepare: func(lru *Cache, arg args) {
				lru.Add(arg.key, arg.value, arg.expireTime)
				lru.Add("new_key", testValue{"test_new_value"}, arg.expireTime)
			},
			wantValue: nil,
			wantOk:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//ch := make(chan string, 1000)
			lru := New(tt.args.maxBytes)
			if tt.prepare != nil {
				tt.prepare(lru, tt.args)
			}
			val, ok := lru.Get(tt.args.key)
			assert.Equal(t, tt.wantValue, val)
			assert.Equal(t, tt.wantOk, ok)
		})
	}
}

func TestCache_ExpireTime(t *testing.T) {
	type args struct {
		maxBytes   int64
		key        string
		value      Value
		expireTime int64
	}
	tests := []struct {
		name    string
		args    args
		prepare func(lru *Cache, arg args)
		wantTTL func() int64 // 修改为函数，确保测试运行时计算 ExpireTime
		wantOk  bool
	}{
		{
			name: "存在且未过期",
			args: args{
				maxBytes:   1000,
				key:        "valid_key",
				value:      testValue{"test_value"},
				expireTime: time.Now().Add(10 * time.Second).Unix(),
			},
			prepare: func(lru *Cache, arg args) {
				lru.Add(arg.key, arg.value, arg.expireTime)
			},
			wantTTL: func() int64 { return time.Now().Add(10 * time.Second).Unix() },
			wantOk:  true,
		},
		{
			name: "存在但已过期",
			args: args{
				maxBytes:   1000,
				key:        "expired_key",
				value:      testValue{"test_value"},
				expireTime: time.Now().Add(-1 * time.Second).Unix(),
			},
			prepare: func(lru *Cache, arg args) {
				lru.Add(arg.key, arg.value, arg.expireTime)
			},
			wantTTL: func() int64 { return 0 },
			wantOk:  false,
		},
		{
			name: "key不存在",
			args: args{
				maxBytes: 1000,
				key:      "nonexistent_key",
			},
			prepare: nil,
			wantTTL: func() int64 { return 0 },
			wantOk:  false,
		},
		{
			name: "永不过期项",
			args: args{
				maxBytes:   1000,
				key:        "no_expire_key",
				value:      testValue{"test_value"},
				expireTime: -1, // 永不过期
			},
			prepare: func(lru *Cache, arg args) {
				lru.Add(arg.key, arg.value, arg.expireTime)
			},
			wantTTL: func() int64 { return -1 },
			wantOk:  true,
		},
		{
			name: "更新过期时间",
			args: args{
				maxBytes:   1000,
				key:        "update_key",
				value:      testValue{"test_value"},
				expireTime: time.Now().Add(20 * time.Second).Unix(),
			},
			prepare: func(lru *Cache, arg args) {
				lru.Add(arg.key, arg.value, time.Now().Add(10*time.Second).Unix())
				lru.Add(arg.key, arg.value, arg.expireTime) // 更新过期时间
			},
			wantTTL: func() int64 { return time.Now().Add(20 * time.Second).Unix() },
			wantOk:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//ch := make(chan string, 1000)
			lru := New(tt.args.maxBytes)
			if tt.prepare != nil {
				tt.prepare(lru, tt.args)
			}

			ttl, ok := lru.ExpireTime(tt.args.key)

			// 计算期望的 ExpireTime
			expectedTTL := tt.wantTTL()

			// 允许 1 秒误差
			const timeTolerance int64 = 1
			if expectedTTL > 0 {
				assert.GreaterOrEqual(t, ttl, expectedTTL-timeTolerance, "ExpireTime should be close to expected value")
				assert.LessOrEqual(t, ttl, expectedTTL+timeTolerance, "ExpireTime should be close to expected value")
			} else {
				assert.Equal(t, expectedTTL, ttl, "ExpireTime should match expected value")
			}

			assert.Equal(t, tt.wantOk, ok, "ExpireTime existence check should match expected result")
		})
	}
}

func TestCache_Del(t *testing.T) {
	type args struct {
		maxBytes   int64
		key        string
		value      Value
		expireTime int64
	}
	tests := []struct {
		name    string
		args    args
		prepare func(lru *Cache, arg args)
		wantLen int
		wantOk  bool
	}{
		{
			name: "删除存在的 key",
			args: args{
				maxBytes:   3,
				key:        "existing_key",
				value:      testValue{"test_value"},
				expireTime: time.Now().Add(10 * time.Second).Unix(),
			},
			prepare: func(lru *Cache, arg args) {
				lru.Add(arg.key, arg.value, arg.expireTime)
			},
			wantLen: 0, // 删除后长度应为 0
			wantOk:  false,
		},
		{
			name: "删除不存在的 key",
			args: args{
				maxBytes: 3,
				key:      "nonexistent_key",
			},
			prepare: nil, // 不添加任何数据
			wantLen: 0,
			wantOk:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//ch := make(chan string, 1000)
			lru := New(tt.args.maxBytes)
			if tt.prepare != nil {
				tt.prepare(lru, tt.args)
			}

			// 执行删除
			lru.Del(tt.args.key)

			// 检查 key 是否仍然存在
			_, ok := lru.Get(tt.args.key)
			assert.Equal(t, tt.wantOk, ok)

			// 检查缓存的长度是否符合预期
			assert.Equal(t, tt.wantLen, lru.Length())
		})
	}
}
