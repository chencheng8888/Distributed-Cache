package lru

import (
	"container/list"
	"time"
)

type Cache struct {
	//如果为0，代表无限制
	maxBytes int64
	nBytes   int64
	list     *list.List
	cache    map[interface{}]*list.Element
}

type Key interface{}

type Value interface {
	Len() int
}

type entry struct {
	key        Key
	value      Value
	expireTime int64 //过期时间的时间戳
}

func New(maxBytes int64) *Cache {
	return &Cache{
		maxBytes: maxBytes,
		list:     list.New(),
		cache:    make(map[interface{}]*list.Element),
	}
}

func (c *Cache) Add(key Key, value Value, expireTime int64) {
	//如果缓存为空，则初始化（延迟初始化）
	if c.cache == nil {
		c.cache = make(map[interface{}]*list.Element)
		c.list = list.New()
	}
	//如果缓存中存在，则更新数据
	if e, ok := c.cache[key]; ok {
		//将该key对应的元素移动到链表头部
		c.list.MoveToFront(e)
		c.nBytes += int64(value.Len() - e.Value.(*entry).value.Len())
		e.Value.(*entry).value = value
		e.Value.(*entry).expireTime = expireTime
		return
	}
	//如果不存在，则添加到缓存中
	//首先检查是否超出最大限制
	if c.maxBytes > 0 && c.nBytes+int64(value.Len()) > c.maxBytes {
		c.removeElement(c.list.Back())
	}

	//添加元素
	c.nBytes += int64(value.Len())
	e := c.list.PushFront(&entry{key, value, expireTime})
	c.cache[key] = e
}
func (c *Cache) Get(key Key) (value interface{}, ok bool) {
	//首先检查是否cache是否存在
	if e, hit := c.cache[key]; hit {
		//存在，则先检查是否过期
		if e.Value.(*entry).expireTime < 0 ||
			(e.Value.(*entry).expireTime >= 0 && e.Value.(*entry).expireTime >= getTime()) {
			//将该key对应的元素移动到链表头部
			c.list.MoveToFront(e)
			return e.Value.(*entry).value, true
		} else {
			c.removeElement(e)
		}
	}
	return
}

func (c *Cache) TTL(key Key) (ttl int64, ok bool) {
	//首先检查是否cache是否存在
	if e, hit := c.cache[key]; hit {
		//存在，则先检查是否过期
		if e.Value.(*entry).expireTime < 0 ||
			(e.Value.(*entry).expireTime >= 0 && e.Value.(*entry).expireTime >= getTime()) {
			//将该key对应的元素移动到链表头部
			c.list.MoveToFront(e)
			return e.Value.(*entry).expireTime, true
		} else {
			c.removeElement(e)
		}
	}
	return 0, false
}

func (c *Cache) Del(key Key) {
	if c.cache == nil {
		return
	}
	if e, ok := c.cache[key]; ok {
		c.removeElement(e)
	}
}

func (c *Cache) removeElement(e *list.Element) {
	c.nBytes -= int64(e.Value.(*entry).value.Len())
	//过期了，删除该节点
	c.list.Remove(e)
	//同时删除cache中的key
	delete(c.cache, e.Value.(*entry).key)
}

func (c *Cache) Size() int64 {
	return c.nBytes
}

func (c *Cache) Length() int {
	return c.list.Len()
}

func getTime() int64 {
	return time.Now().Unix()
}
