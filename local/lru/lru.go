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

type Value interface {
	Len() int
}

type entry struct {
	key        string
	value      Value
	expireTime int64 //过期时间的时间戳
}

type MaxBytes int64

func New(maxBytes MaxBytes) *Cache {
	return &Cache{
		maxBytes: int64(maxBytes),
		list:     list.New(),
		cache:    make(map[interface{}]*list.Element),
	}
}

func (c *Cache) Add(key string, value Value, expireTime int64) {
	//如果缓存为空，则初始化（延迟初始化）
	if c.cache == nil {
		c.cache = make(map[interface{}]*list.Element)
		c.list = list.New()
	}
	//检查存储的过期时间
	if !checkExpire(expireTime) {
		return
	}

	//如果缓存中存在，则更新数据
	if e, ok := c.cache[key]; ok {
		//将该key对应的元素移动到链表头部
		c.list.MoveToFront(e)
		eentry := e.Value.(*entry)

		//更新byte数
		c.nBytes += int64(value.Len()) - int64(eentry.value.Len())

		eentry.value = value
		eentry.expireTime = expireTime
		return
	}

	//添加元素
	e := c.list.PushFront(&entry{key, value, expireTime})
	c.nBytes += int64(value.Len())
	c.cache[key] = e

	//如果缓存已满，则删除最老的元素
	for c.maxBytes > 0 && c.nBytes > c.maxBytes {
		c.removeOldest()
	}
}
func (c *Cache) Get(key string) (value Value, ok bool) {
	//首先检查是否cache是否存在
	if e, hit := c.cache[key]; hit {
		//存在，则先检查是否过期
		if checkExpire(e.Value.(*entry).expireTime) {
			//将该key对应的元素移动到链表头部
			c.list.MoveToFront(e)
			return e.Value.(*entry).value, true
		} else {
			c.removeElement(e)
		}
	}
	return
}

func (c *Cache) ExpireTime(key string) (expireTime int64, ok bool) {
	//首先检查是否cache是否存在
	if e, hit := c.cache[key]; hit {
		//存在，则先检查是否过期
		if checkExpire(e.Value.(*entry).expireTime) {
			//将该key对应的元素移动到链表头部
			c.list.MoveToFront(e)
			return e.Value.(*entry).expireTime, true
		} else {
			c.removeElement(e)
		}
	}
	return 0, false
}

func (c *Cache) Del(key string) {
	if c.cache == nil {
		return
	}
	if e, ok := c.cache[key]; ok {
		c.removeElement(e)
	}
}

func (c *Cache) removeOldest() {
	if c.cache == nil {
		return
	}
	ele := c.list.Back()
	if ele != nil {
		c.removeElement(ele)
	}
}

func (c *Cache) removeElement(e *list.Element) {
	//过期了，删除该节点
	c.list.Remove(e)
	//同时删除cache中的key
	eentry := e.Value.(*entry)

	c.nBytes -= int64(eentry.value.Len())

	delete(c.cache, eentry.key)
}

func (c *Cache) Bytes() int64 {
	return c.nBytes
}

func (c *Cache) Length() int {
	return c.list.Len()
}

func checkExpire(expireTime int64) bool {
	//如果expireTime小于0，代表不过期
	//合法
	if expireTime < 0 {
		return true
	}
	//如果expireTime等于0，代表不设置过期时间
	//不合法
	if expireTime == 0 {
		return false
	}
	//如果expireTime大于0，代表设置了过期时间
	//如果expireTime大于当前时间，代表未过期

	currentTime := time.Now().Unix()

	return expireTime > currentTime
}
