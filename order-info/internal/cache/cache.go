package cache

import (
	"sync"

	"github.com/MaksProg/order-service/internal/model"
)

type Orders struct {
	mu sync.RWMutex
	m map[string]*model.Order
}

func New() *Orders{
	return &Orders{m: make(map[string]*model.Order)}
}

func (c *Orders) Get(id string) (*model.Order, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    o, ok := c.m[id]
    return o, ok
}

func (c *Orders) Set(o *model.Order){
	c.mu.Lock();defer c.mu.Unlock()
	c.m[o.OrderUID] = o
}