package gbdb

import (
	"fmt"
	"os"
)

// 二叉树暴露给外侧调用的接口
type Logical struct {
	tree *BinaryTree
}

// 二叉树的基类，储存file和根节点
type LogicalBase struct {
	_storage Storage
	_treeRef *NodeRef
}

//
func NewLogical() *Logical {
	logical := new(Logical)
	logical.tree = NewBinaryTree()
	return logical
}

func NewBinaryTree() *BinaryTree {
	tree := new(BinaryTree)
	_f, _ := os.OpenFile("1111.gbdb", os.O_RDWR|os.O_CREATE, 0)
	storage := NewStorage(_f)
	tree._storage = *storage
	tree._treeRef = nil
	return tree
}

// 从引用中获取node 并返回Node结构
func (l *LogicalBase) _followNode(ref *NodeRef) (*Node, error) {
	node := ref.get(l._storage)
	return node, nil
}

// 从从Val引用中获取储存在里面的值
func (l *LogicalBase) _followVal(ref *ValRef) ([]byte, error) {
	bytes := ref.get(l._storage)
	return bytes, nil
}

// 将二叉树储存到文件中持久化
func (l *LogicalBase) commit() {
	l._treeRef.store(l._storage)
	l._storage.CommitRootAdress(l._treeRef.address())
}

// 同步二叉树的根节点为文件中储存的根节点
func (l *LogicalBase) _refreshTreeRef() {
	l._treeRef = NewNodeRef()
	l._treeRef.Ref = NewNode(0, -1)
	l._treeRef.Address = l._storage.GetRootAdress()
}

// 对外接口get,输入键获得值
func (l *Logical) Get(key int64) ([]byte, error) {
	if l.tree._storage.lock() == false {
		l.tree._refreshTreeRef()
	}
	node, err := l.tree._followNode(l.tree._treeRef)
	if err != nil {
		fmt.Println("无法找到根节点", err)
	}
	return l.tree._get(node, key)
}

// 对外接口set 储存数据到二叉树中
func (l *Logical) Set(key int64, value interface{}) {
	if l.tree._storage.lock() == true {
		l.tree._refreshTreeRef()
	}
	node, err := l.tree._followNode(l.tree._treeRef)
	if err != nil {
		fmt.Println("无法找到根节点")
	}
	bytes := _TypetoBytes(value)
	valRef := NewValRef()
	valRef.Ref = bytes
	l.tree._treeRef = l.tree._insert(node, key, valRef)
}

func (l *Logical) Commit() {
	l.tree.commit()
}

// Pop 从二叉树中删除数据
func (l *Logical) Pop(key int64) {
	if l.tree._storage.lock() == true {
		l.tree._refreshTreeRef()
	}
	node, err := l.tree._followNode(l.tree._treeRef)
	if err != nil {
		fmt.Println("无法找到根节点")
	}
	_ref, err := l.tree._delete(node, key)
	l.tree._treeRef = _ref
}
