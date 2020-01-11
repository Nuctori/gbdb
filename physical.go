package gbdb

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"os"
)

// SUPERBLOCK_SIZE 数据库superblock的大小，默认4096
const SUPERBLOCK_SIZE int64 = 4096 
// INTEGER_LENGTH 一个int类型的字节大小
const INTEGER_LENGTH int64 = 8 

// _Storage 结构内含一个逻辑锁和一个文件描述符
type _Storage struct {
	locked bool
	_f     *os.File
}

// newStorage 创建新的储存对象
func newStorage(f *os.File) *_Storage {
	s := _Storage{false, f}
	s._ensureSuperblock()
	return &s
}

func (s *_Storage) _seekEnd() {
	s._f.Seek(0, os.SEEK_END)
}

func (s *_Storage) _ensureSuperblock() {
	s._seekEnd()
	endAdress, _ := s._f.Seek(0, os.SEEK_CUR)
	if endAdress < SUPERBLOCK_SIZE {
		nullBtyes := make([]byte, SUPERBLOCK_SIZE-endAdress)
		s._f.Write(nullBtyes)

	}
}

func (s *_Storage) _seekSuperBlock() {
	s._f.Seek(0, 0)
}

func (s *_Storage) _readInt() int64 {
	buf := make([]byte, INTEGER_LENGTH)
	s._f.Read(buf)
	return s._bytesToInt64(buf)
}

func (s *_Storage) _writeInt(integer int64) {
	s.lock()
	bytes := s._Int64ToBytes(integer)
	s._f.Write(bytes)
}

func (s *_Storage) _bytesToInt64(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))
}

func (s *_Storage) _Int64ToBytes(i int64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

// 数据库的逻辑锁
func (s *_Storage) lock() bool {
	if s.locked == false {
		s.locked = true // 逻辑锁
		return true
	}
	return false
}

// 解锁逻辑锁
func (s *_Storage) unlock() {
	if s.locked {
		s._f.Sync()
		s.locked = false
	}
}

// CommitRootAdress 传入二叉树根节点数据所在的地址
// 写入到SUPERBLOCK中
func (s *_Storage) CommitRootAdress(rootAdress int64) {
	s.lock()
	s._f.Sync()
	s._seekSuperBlock()
	s._writeInt(rootAdress)
	s._f.Sync()
	s.unlock()

}

// GetRootAdress 从SUPERBLOCK中获取二叉树根节点的地址
func (s *_Storage) GetRootAdress() int64 {
	s._seekSuperBlock()
	rootAdress := s._readInt()
	return rootAdress
}

func (s *_Storage) _getBytes(key interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Write 函数传入任意可序列化的数据作为参数
// 向数据库写入参数序列化后的数据，并返回数据所在地址
func (s *_Storage) Write(data []byte) int64 {
	s.lock()
	s._seekEnd()
	objAdress, _ := s._f.Seek(0, os.SEEK_CUR)
	s._writeInt(int64(len(data)))
	s._f.Write(data)
	return objAdress
}

// Read 函数传入数据的地址，返回地址中所储存的数据
func (s *_Storage) Read(address int64) []byte {
	s._f.Seek(address, os.SEEK_SET)
	length := s._readInt()
	data := make([]byte, length)
	s._f.Read(data)
	return data
}
