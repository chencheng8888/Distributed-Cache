package consistentHash

import (
	"sort"
	"strconv"
)

// Hash 哈希函数
type Hash func(data []byte) uint32

type Map struct {
	//mu sync.RWMutex
	hash     Hash
	replicas int   //虚拟节点倍数
	nodes    []int //hash环
	hashMap  map[int]string
	trueNode map[string]struct{}
}

type Replicas int

// New 创建一致性哈希
func New(replicas Replicas, fn Hash) Map {
	m := Map{
		replicas: int(replicas),
		hash:     fn,
		hashMap:  make(map[int]string),
		trueNode: make(map[string]struct{}),
	}
	return m
}

func (m *Map) Add(node string) {

	if _, ok := m.trueNode[node]; ok {
		return
	}

	for i := 0; i < m.replicas; i++ {
		hash := int(m.hash([]byte(strconv.Itoa(i) + node)))
		m.nodes = append(m.nodes, hash)
		m.hashMap[hash] = node
	}
	sort.Ints(m.nodes)

	m.trueNode[node] = struct{}{}
}

func (m *Map) Remove(node string) {
	if _, ok := m.trueNode[node]; !ok {
		return
	}

	var deleteNodes = make(map[int]struct{}, m.replicas)
	for i := 0; i < m.replicas; i++ {
		hash := int(m.hash([]byte(strconv.Itoa(i) + node)))
		deleteNodes[hash] = struct{}{}
		delete(m.hashMap, hash)
	}
	var newNodes []int
	for _, pnode := range m.nodes {
		if _, ok := deleteNodes[pnode]; !ok {
			newNodes = append(newNodes, pnode)
		}
	}
	m.nodes = newNodes
	sort.Ints(m.nodes)
	delete(m.trueNode, node)
}

func (m *Map) Get(key string) string {
	if len(m.trueNode) == 0 || key == "" {
		return ""
	}
	hash := int(m.hash([]byte(key)))

	//找到该hash值在hash环中的位置
	idx := sort.Search(len(m.nodes), func(i int) bool {
		return m.nodes[i] >= hash
	})
	//如果idx等于hash环长度，则取0
	return m.hashMap[m.nodes[idx%len(m.nodes)]]
}

func (m *Map) GetAllNodes() []string {
	var nodes []string

	for node, _ := range m.trueNode {
		nodes = append(nodes, node)
	}
	return nodes
}

// GetAndSkipNode 获取key对应的节点，并跳过指定节点
func (m *Map) GetAndSkipNode(key, node string) string {
	if len(m.trueNode) <= 1 || key == "" || node == "" {
		return ""
	}
	hash := int(m.hash([]byte(key)))

	//找到该hash值在hash环中的位置
	idx := sort.Search(len(m.nodes), func(i int) bool {
		return m.nodes[i] >= hash
	})
	var i int
	for i = idx; m.hashMap[m.nodes[i%len(m.nodes)]] == node; i = (i + 1) % len(m.nodes) {
	}
	return m.hashMap[m.nodes[i%len(m.nodes)]]
}
