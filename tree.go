package gbdb

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
)

type ValRef struct {
	Ref     []byte // 储存的数据
	Address int64  // 数据所在的地址
}

// Ref 数据引用类型接口
type Ref interface {
	get(Storage) ([]byte, error)
	store()
	_BytetoTypes([]byte, interface{})
	_TypetoBytes(interface{}) []byte
}

func (v *ValRef) address() int64 {
	return v.Address
}
func (v *ValRef) _prepareToStore(s Storage) {}

func _TypetoBytes(valType interface{}) []byte { // 添加字符串和切片支持
	switch v := valType.(type) {
	case string:
		btyes := []byte(v)
		return btyes
	case Node:
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(v); err != nil {
			log.Fatal("encode error:", err)
		}
		return buf.Bytes()
	case ValRef:
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(v); err != nil {
			log.Fatal("encode error:", err)
		}
		return buf.Bytes()
	case NodeRef:
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(v); err != nil {
			log.Fatal("encode error:", err)
		}
		return buf.Bytes()
	default:
		return []byte{}
	}
}

// 将Node的字节型数据，反序列化成Node类型
// 返回Node类型指针
func _ByteToNode(data []byte) *Node {
	var node Node
	var buf bytes.Buffer
	buf.Write(data)
	dec := gob.NewDecoder(&buf)
	if err := dec.Decode(&node); err != nil {
		log.Fatal("decode error:", err)
	}
	return &node
}

// 冗余
func _BytetoTypes(data []byte, to interface{}) { // 添加字符串和切片支持
	if data == nil {
		return
	}
	switch t := to.(type) {
	case string:
		str := string(t)
		t = str
	case Node:
		buf := new(bytes.Buffer)
		err := binary.Read(buf, binary.LittleEndian, &t)
		if err != nil {
			fmt.Println("binary.Read failed:", err)
		}
	}
}

// 从ValRef的地址中获取ValRef指向的数据
func (v *ValRef) get(storage Storage) []byte {
	if v.Ref != nil {
		_BytetoTypes(storage.Read(v.Address), v.Ref)
	}
	return v.Ref
}

// 从NodeRef指向的地址中,获取Node节点
func (n *NodeRef) get(storage Storage) *Node {
	if n.Address > 0 {
		n.Ref = _ByteToNode(storage.Read(n.Address))
	} else if n.Ref != nil && n.Ref.Length == -1 {
		return nil
	}
	return n.Ref
}

// 将ValRef里的数据,储存到磁盘
func (v *ValRef) store(storage Storage) {
	if v.Ref != nil {
		v._prepareToStore(storage)
		v.Address = storage.Write(v.Ref)
	}
}

// 将NodeRef里的节点，序列化后储存到磁盘
func (nr *NodeRef) store(storage Storage) {
	if nr.Ref != nil && nr.Address == -1 {
		nr._prepareToStore(storage)
		nr.Address = storage.Write(_TypetoBytes(*nr.Ref))
	}
}

type NodeRef struct {
	Ref *Node
	ValRef
}

// 递归遍历节点指向的引用储存到磁盘中
func (nr *NodeRef) _prepareToStore(s Storage) {
	if &nr.Ref != nil {
		nr.Ref.storeRefs(s)
	}
}

// 获取Node节点数据的长度。
func (nr *NodeRef) length() int64 {
	if nr.Ref == nil && nr.Address > 0 {
		fmt.Println("获取节点长度前，需要先加载节点")
		return 0
	} else if nr.Ref != nil {
		return nr.Ref.Length
	} else {
		return 0
	}

}

type Node struct {
	Left   *NodeRef
	Right  *NodeRef
	Val    *ValRef
	Key    int64
	Length int64
}

// Node的构造函数
func NewNode(key int64, length int64) *Node {
	node := new(Node)
	node.Left = nil
	node.Right = nil
	node.Val = nil
	node.Key = key
	node.Length = length
	return node
}

// NodeRef的构造函数
func NewNodeRef() *NodeRef {
	nodeRef := new(NodeRef)
	nodeRef.Ref = nil
	nodeRef.Address = -1
	return nodeRef
}

// NewValRef ValRef的构造函数
func NewValRef() *ValRef {
	valRef := new(ValRef)
	valRef.Ref = nil
	valRef.Address = 0
	return valRef
}

func (n *Node) storeRefs(s Storage) {
	n.Val.store(s)
	n.Left.store(s)
	n.Right.store(s)
}

// FromNode 传入一个新的Node 将会从旧的node 继承数据,会更新newNode参数中传入的node
func FromNode(node *Node, newNode *Node) {
	length := node.Length
	if newNode.Left != nil { // 更新左节点
		newLength := newNode.Left.length()
		oldLength := node.Left.length()
		length += newLength - oldLength
	}
	if newNode.Right != nil { // 更新右节点
		newLength := newNode.Right.length()
		oldLength := node.Right.length()
		length += newLength - oldLength
	}
	// 让新节点获得旧节点的引用
	if newNode.Left == nil {
		newNode.Left = node.Left
	}
	if newNode.Right == nil {
		newNode.Right = node.Right
	}
	if newNode.Val == nil {
		newNode.Val = node.Val
	}
	if newNode.Key == 0 {
		newNode.Key = node.Key
	}
	newNode.Length = length
}

type BinaryTree struct {
	LogicalBase
}

func (b BinaryTree) _get(node *Node, key int64) ([]byte, error) {
	var err error
	for node != nil {
		if key < node.Key {
			node, err = b._followNode(node.Left)
			if err != nil {
				return nil, err
			}
		} else if key > node.Key {
			node, err = b._followNode(node.Right)
			if err != nil {
				return nil, err
			}
		} else {
			return b._followVal(node.Val)
		}
	}
	return nil, errors.New("Key Error")
}

func (b BinaryTree) _insert(node *Node, key int64, valref *ValRef) *NodeRef {
	var newNode *Node
	if node == nil { // 初次插入数据，构造根节点。
		newNode = NewNode(key, 1)
		newNode.Left = NewNodeRef()
		newNode.Right = NewNodeRef()
		newNode.Val = valref
	} else if key < node.Key {
		newNode = NewNode(0, 0)
		followNode, err := b._followNode(node.Left)
		if err != nil {
			fmt.Println(err)
		}
		newNode.Left = b._insert(followNode, key, valref)
		FromNode(node, newNode)
	} else if node.Key < key {
		newNode = NewNode(0, 0)
		followNode, err := b._followNode(node.Right)
		if err != nil {
			fmt.Println(err)
		}
		newNode.Right = b._insert(followNode, key, valref)
		FromNode(node, newNode)
	} else { // 递归找到该插入的节点
		newNode = NewNode(key, 1)
		newNode.Val = valref
		FromNode(node, newNode)
	}
	re := NewNodeRef()
	re.Ref = newNode
	return re
}

// 删除树中的节点，本函数将递归构造一个新的树并返回根节点的引用
func (b BinaryTree) _delete(node *Node, key int64) (*NodeRef, error) {
	var newNode *Node
	if node == nil {
		// 找不到要删除的节点
		return &NodeRef{}, errors.New("KeyErrors")
	} else if key < node.Key {
		newNode = &Node{}
		followNode, err := b._followNode(node.Left)
		if err != nil {
			fmt.Println(err)
		}
		newNode.Left, err = b._delete(followNode, key)
		FromNode(node, newNode)
	} else if node.Key < key {
		newNode = &Node{}
		followNode, err := b._followNode(node.Right)
		if err != nil {
			fmt.Println(err)
		}
		newNode.Right, err = b._delete(followNode, key)
		FromNode(node, newNode)
	} else {
		left, err := b._followNode(node.Left)
		if err != nil {
			fmt.Println(err)
		}
		right, err := b._followNode(node.Right)
		if err != nil {
			fmt.Println(err)
		}
		if left != nil && right != nil {
			replacement := b._findMax(left)
			leftRef, err := b._delete(left, replacement.Key)
			if err != nil {
				fmt.Println(err)
			}
			newNode = NewNode(replacement.Key, leftRef.length()+node.Right.length()+1)
			newNode.Left = leftRef
			newNode.Right = node.Right
			newNode.Val = replacement.Val
		} else if left != nil {
			return node.Left, nil
		} else {
			return node.Right, nil
		}
	}
	reNodeRef := NewNodeRef()
	reNodeRef.Ref = newNode
	reNodeRef.Address = 0
	return reNodeRef, nil
}

func (b BinaryTree) _findMax(node *Node) *Node {
	for {
		nextNode, _ := b._followNode(node.Right)
		if nextNode == nil {
			return node
		}
		node = nextNode
	}
}
