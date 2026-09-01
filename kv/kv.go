package kv

import (
	"errors"
	"raftkv/raft"
	"raftkv/types"
	"sync"
	"time"
)

type KVStore struct {
	mu   sync.Mutex
	data map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{data: make(map[string]string)}
}

func (kv *KVStore) Apply(cmd types.Command) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch cmd.Op {
	case "PUT":
		kv.data[cmd.Key] = cmd.Value
		return cmd.Value, true
	case "DELETE":
		value, ok := kv.data[cmd.Key]
		if !ok {
			return "", false
		} else {
			delete(kv.data, cmd.Key)
			return value, true
		}
	}
	return "", false
}

func (kv *KVStore) Put(r *raft.Raft, key, value string) (string, error) {
	idx, term, isLeader := r.Start(types.Command{Op: "PUT", Key: key, Value: value})
	if !isLeader {
		return "", errors.New("not leader")
	}
	deadline := time.Now().Add(3 * time.Second)
	for !r.IsApplied(idx) {
		if time.Now().After(deadline) {
			return "", errors.New("timed out waiting for commit")
		}
		time.Sleep(300 * time.Millisecond)
	}
	appliedTerm, _ := r.GetLogEntryTerm(idx)
	if term != appliedTerm {
		return "", errors.New("command didn't applied")
	}
	return value, nil
}

func (kv *KVStore) Read(key string) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, ok := kv.data[key]
	return v, ok
}

func (kv *KVStore) Get(r *raft.Raft, key string) (string, error) {
	state, _ := r.GetState()
	if state != types.Leader {
		return "", errors.New("not leader")
	}
	value, ok := kv.Read(key)
	if !ok {
		return "", errors.New("key not found")
	}
	return value, nil
}

func (kv *KVStore) Delete(r *raft.Raft, key string) (string, error) {
	idx, term, isLeader := r.Start(types.Command{Op: "DELETE", Key: key, Value: ""})
	if !isLeader {
		return "", errors.New("not leader")
	}
	deadline := time.Now().Add(3 * time.Second)
	for !r.IsApplied(idx) {
		if time.Now().After(deadline) {
			return "", errors.New("timed out waiting for commit")
		}
		time.Sleep(300 * time.Millisecond)
	}
	appliedTerm, _ := r.GetLogEntryTerm(idx)
	if term != appliedTerm {
		return "", errors.New("command didn't applied")
	}
	return key, nil
}

func (kv *KVStore) Snapshot() map[string]string {
	snapshot := make(map[string]string)
	kv.mu.Lock()
	for key, value := range kv.data {
		snapshot[key] = value
	}
	kv.mu.Unlock()
	return snapshot
}
