package gbdb

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"strconv"
)

// ValRef 数据的引用,通过adress可以从数据库中提取数据
type ValRef struct {
	Ref     []byte // 储存的数据
	Address int64  // 数据所在的地址
}

// Ref 数据引用类型接口
type Ref interface {
	get(_Storage) ([]byte, error)
	store()
	_BytetoTypes([]byte, interface{})
	_TypetoBytes(interface{}) []byte
}

func (v *ValRef) address() int64 {
	return v.Address
}
func (v *ValRef) _prepareToStore(s _Storage) {}

func _TypetoInt(valType interface{}) (int64, error) { 
	switch v := valType.(type) {
	case string:
		_int, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return int64(0), err
		}
		return _int, nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int:
		return int64(v), nil
	default:
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err := enc.Encode(v)
		if err != nil {
			return 0, err
		}
		data := buf.Bytes()
		Sha1Inst := sha1.New()
		Sha1Inst.Write(data)

		Result := Sha1Inst.Sum([]byte(""))

		return int64(binary.BigEndian.Uint64(Result)), nil
	}
}

func _TypetoBytes(valType interface{}) []byte {
	switch v := valType.(type) {
	case string:
		btyes := []byte(v)
		return btyes
	case _Node:
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
	case _NodeRef:
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(v); err != nil {
			log.Fatal("encode error:", err)
		}
		return buf.Bytes()
	default:
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err := enc.Encode(v)
		if err != nil {
			return 0, err
		}
		data := buf.Bytes()
		return data
	}
}

// 将Node的字节型数据，反序列化成Node类型
// 返回Node类型指针
func _ByteToNode(data []byte) *_Node {
	var node _Node
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
	case _Node:
		buf := new(bytes.Buffer)
		err := binary.Read(buf, binary.LittleEndian, &t)
		if err != nil {
			fmt.Println("binary.Read failed:", err)
		}
	}
}

// 从ValRef的地址中获取ValRef指向的数据
func (v *ValRef) get(storage _Storage) []byte {
	if v.Ref != nil {
		_BytetoTypes(storage.Read(v.Address), v.Ref)
	}
	return v.Ref
}

// 从NodeRef指向的地址中,获取Node节点
func (n *_NodeRef) get(storage _Storage) *_Node {
	if n.Address > 0 {
		n.Ref = _ByteToNode(storage.Read(n.Address))
	} else if n.Ref != nil && n.Ref.Length == -1 {
		return nil
	}
	return n.Ref
}

// 将ValRef里的数据,储存到磁盘
func (v *ValRef) store(storage _Storage) {
	if v.Ref != nil {
		v._prepareToStore(storage)
		v.Address = storage.Write(v.Ref)
	}
}

// 将NodeRef里的节点，序列化后储存到磁盘
func (n *_NodeRef) store(storage _Storage) {
	if n.Ref != nil && n.Address == -1 {
		n._prepareToStore(storage)
		n.Address = storage.Write(_TypetoBytes(*n.Ref))
	}
}

type _NodeRef struct {
	Ref *_Node
	ValRef
}

// 递归遍历节点指向的引用储存到磁盘中
func (n *_NodeRef) _prepareToStore(s _Storage) {
	if &n.Ref != nil {
		n.Ref.storeRefs(s)
	}
}

// 获取Node节点数据的长度。
func (n *_NodeRef) length() int64 {
	if n.Ref == nil && n.Address > 0 {
		fmt.Println("获取节点长度前，需要先加载节点")
		return 0
	} else if n.Ref != nil {
		return n.Ref.Length
	} else {
		return 0
	}

}

type _Node struct {
	Left   *_NodeRef
	Right  *_NodeRef
	Val    *ValRef
	Key    int64
	Length int64
}

// newNode Node的构造函数
func newNode(key int64, length int64) *_Node {
	node := new(_Node)
	node.Left = nil
	node.Right = nil
	node.Val = nil
	node.Key = key
	node.Length = length
	return node
}

// newNodeRef _NodeRef 的构造函数
func newNodeRef() *_NodeRef {
	nodeRef := new(_NodeRef)
	nodeRef.Ref = nil
	nodeRef.Address = -1
	return nodeRef
}

// newValRef ValRef的构造函数
func newValRef() *ValRef {
	valRef := new(ValRef)
	valRef.Ref = nil
	valRef.Address = 0
	return valRef
}

func (n *_Node) storeRefs(s _Storage) {
	n.Val.store(s)
	n.Left.store(s)
	n.Right.store(s)
}

// fromNode 传入一个Node,该node将会继承传入的node的数据
func (n *_Node) fromNode(node *_Node) {
	length := node.Length
	if n.Left != nil { // 更新左节点
		newLength := n.Left.length()
		oldLength := node.Left.length()
		length += newLength - oldLength
	}
	if n.Right != nil { // 更新右节点
		newLength := n.Right.length()
		oldLength := node.Right.length()
		length += newLength - oldLength
	}
	// 让新节点获得旧节点的引用
	if n.Left == nil {
		n.Left = node.Left
	}
	if n.Right == nil {
		n.Right = node.Right
	}
	if n.Val == nil {
		n.Val = node.Val
	}
	if n.Key == 0 {
		n.Key = node.Key
	}
	n.Length = length
}

type _BinaryTree struct {
	_logicalBase
}

func (b _BinaryTree) _get(node *_Node, key int64) ([]byte, error) {
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

func (b _BinaryTree) _insert(node *_Node, key int64, valref *ValRef) *_NodeRef {
	var insertNode *_Node

	if node == nil { // 初次插入数据，构造根节点。
		insertNode = newNode(key, 1)
		insertNode.Left = newNodeRef()
		insertNode.Right = newNodeRef()
		insertNode.Val = valref
	} else if key < node.Key {
		insertNode = newNode(0, 0)
		followNode, err := b._followNode(node.Left)
		if err != nil {
			fmt.Println(err)
		}
		insertNode.Left = b._insert(followNode, key, valref)
		insertNode.fromNode(node)
	} else if node.Key < key {
		insertNode = newNode(0, 0)
		followNode, err := b._followNode(node.Right)
		if err != nil {
			fmt.Println(err)
		}
		insertNode.Right = b._insert(followNode, key, valref)
		insertNode.fromNode(node)
	} else { // 递归找到该插入的节点
		insertNode = newNode(key, 1)
		insertNode.Val = valref
		insertNode.fromNode(node)
	}
	re := newNodeRef()
	re.Ref = insertNode
	return re
}

// 删除树中的节点，本函数将递归构造一个新的树并返回根节点的引用
func (b _BinaryTree) _delete(node *_Node, key int64) (*_NodeRef, error) {
	var newRoot *_Node
	if node == nil {
		// 找不到要删除的节点
		return &_NodeRef{}, errors.New("KeyErrors")
	} else if key < node.Key {
		newRoot = &_Node{}
		followNode, err := b._followNode(node.Left)
		if err != nil {
			fmt.Println(err)
		}
		newRoot.Left, err = b._delete(followNode, key)
		newRoot.fromNode(node)
	} else if node.Key < key {
		newRoot = &_Node{}
		followNode, err := b._followNode(node.Right)
		if err != nil {
			fmt.Println(err)
		}
		newRoot.Right, err = b._delete(followNode, key)
		newRoot.fromNode(node)
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
			newRoot = newNode(replacement.Key, leftRef.length()+node.Right.length()+1)
			newRoot.Left = leftRef
			newRoot.Right = node.Right
			newRoot.Val = replacement.Val
		} else if left != nil {
			return node.Left, nil
		} else {
			return node.Right, nil
		}
	}
	reNodeRef := newNodeRef()
	reNodeRef.Ref = newRoot
	reNodeRef.Address = -1
	return reNodeRef, nil
}

func (b _BinaryTree) _findMax(node *_Node) *_Node {
	for {
		nextNode, _ := b._followNode(node.Right)
		if nextNode == nil {
			return node
		}
		node = nextNode
	}
}
