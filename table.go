package flexmem

import (
	"errors"
	"fmt"
	"sync"
)

type memTable struct {
	lock    *sync.RWMutex
	records map[string]interface{}

	name string
}

func newMemTable() *memTable {
	mt := new(memTable)
	mt.lock = new(sync.RWMutex)
	mt.records = map[string]interface{}{}
	return mt
}

func (m *memTable) GetID(data interface{}) (string, error) {
	return "", errors.New("error on obtaining id for " + m.name)
}

func (m *memTable) Get(key string) (interface{}, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	data, ok := m.records[key]
	return data, ok
}

func (m *memTable) GetWithDefault(key string, def interface{}) (interface{}, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	data, ok := m.records[key]
	if !ok {
		return def, ok
	}
	return data, ok
}

func (m *memTable) Set(key string, data interface{}, upsert bool) error {
	m.lock.RLock()
	_, ok := m.records[key]
	m.lock.RUnlock()

	if ok && !upsert {
		return fmt.Errorf("record already exists with key '%s'", key)
	}

	m.lock.Lock()
	m.records[key] = data
	m.lock.Unlock()

	return nil
}

func (m *memTable) Delete(key string) {
	m.lock.Lock()
	delete(m.records, key)
	m.lock.Unlock()
}

type ScanFunc func(key string, record interface{}) (bool, interface{})

func (m *memTable) Scan(fn ScanFunc) <-chan interface{} {
	c := make(chan interface{})

	go func() {
		m.lock.RLock()
		records := m.records
		m.lock.RUnlock()
		for k, r := range records {
			if fn == nil {
				c <- r
			} else {
				if ok, ret := fn(k, r); ok {
					c <- ret
				}
			}
		}
		close(c)
	}()

	return c
}

func (m *memTable) ScanP(fn ScanFunc) <-chan interface{} {
	c := make(chan interface{})

	wg := new(sync.WaitGroup)
	wg.Add(len(m.records))
	m.lock.RLock()
	for k, r := range m.records {
		go func(k string, r interface{}, wg *sync.WaitGroup) {
			defer wg.Done()
			if ok, ret := fn(k, r); ok {
				c <- ret
			}
		}(k, r, wg)
	}
	m.lock.Unlock()
	wg.Wait()
	close(c)

	return c
}

func (m *memTable) RecordsAsArray() []interface{} {
	res := make([]interface{}, len(m.records))
	i := 0
	for _, rec := range m.records {
		res[i] = rec
		i++
	}
	return res
}
