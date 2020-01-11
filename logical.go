package gbdb

import (
	"fmt"
	"os"
	"log"
)

// _Logical 二叉树暴露给外侧调用的接口
type _Logical struct {
	tree *_BinaryTree
}

// _logicalBase 二叉树的基类，储存file和根节点
type _logicalBase struct {
	_storage _Storage
	_treeRef *_NodeRef
}

// NewDB 数据库接口的构造函数
func NewDB(dbName string) *_Logical {
	if dbName == "" {
		dbName = "dump"
	}
	_Logical := new(_Logical)
	_Logical.tree = newBinaryTree(dbName + ".gbdb")
	return _Logical
}

// newBinaryTree 二叉树的构造函数
func newBinaryTree(path string) *_BinaryTree {
	tree := new(_BinaryTree)
	_f, _ := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0)
	storage := newStorage(_f)
	tree._storage = *storage
	tree._treeRef = nil
	return tree
}

// 从引用中获取node 并返回Node结构
func (l *_logicalBase) _followNode(ref *_NodeRef) (*_Node, error) {
	_Node := ref.get(l._storage)
	return _Node, nil
}

// 从从Val引用中获取储存在里面的值
func (l *_logicalBase) _followVal(ref *ValRef) ([]byte, error) {
	bytes := ref.get(l._storage)
	return bytes, nil
}

// 将二叉树储存到文件中持久化
func (l *_logicalBase) commit() {
	l._treeRef.store(l._storage)
	l._storage.CommitRootAdress(l._treeRef.address())
}

// 同步二叉树的根节点为文件中储存的根节点
func (l *_logicalBase) _refreshTreeRef() {
	l._treeRef = newNodeRef()
	l._treeRef.Ref = newNode(0, -1)
	l._treeRef.Address = l._storage.GetRootAdress()
}

// Get 对外接口,输入键获得值
func (l *_Logical) Get(key interface{}) ([]byte, error) {
	if l.tree._storage.locked == false {
		l.tree._refreshTreeRef()
	}
	_Node, err := l.tree._followNode(l.tree._treeRef)
	if err != nil {
		fmt.Println("无法找到根节点", err)
	}
	keyInt,err := _TypetoInt(key)
	if err != nil {
		log.Fatal("encode error:", err)
	}
	return l.tree._get(_Node, keyInt)
}

// Set 对外接口 储存数据到二叉树中
func (l *_Logical) Set(key interface{}, value interface{}) {
	if l.tree._storage.lock() == true {
		l.tree._refreshTreeRef()
	}
	_Node, err := l.tree._followNode(l.tree._treeRef)
	if err != nil {
		fmt.Println("无法找到根节点")
	}
	keyInt,err := _TypetoInt(key)
	if err != nil {
		log.Fatal("encode error:", err)
	}
	valbytes := _TypetoBytes(value)
	valRef := newValRef()
	valRef.Ref = valbytes
	l.tree._treeRef = l.tree._insert(_Node, keyInt, valRef)
}

// Commit 提交更改写入磁盘
func (l *_Logical) Commit() {
	l.tree.commit()
}

// Pop 从二叉树中删除数据
func (l *_Logical) Pop(key int64) {
	if l.tree._storage.lock() == true {
		l.tree._refreshTreeRef()
	}
	_Node, err := l.tree._followNode(l.tree._treeRef) // 获取根节点
	if err != nil {
		fmt.Println("无法找到根节点")
	}
	_ref, err := l.tree._delete(_Node, key)
	l.tree._treeRef = _ref
}
