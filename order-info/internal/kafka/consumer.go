package kafka

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
	"github.com/MaksProg/order-service/internal/cache"
	"github.com/MaksProg/order-service/internal/db"
	"github.com/MaksProg/order-service/internal/model"
)

type Consumer struct {
	reader *kafka.Reader
	store  *db.Store
	cache  *cache.Orders
}

func NewConsumer(brokers []string, topic, group string, store *db.Store, c *cache.Orders) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  group,
		MaxBytes: 10e6,
	})
	return &Consumer{reader: r, store: store, cache: c}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		m, err := c.reader.ReadMessage(ctx)
		if err != nil {
			return err
		}
		var o model.Order
		if err := json.Unmarshal(m.Value, &o); err != nil {
			log.Printf("invalid message: %v", err)
			continue 
		}
		if o.OrderUID == "" {
			log.Printf("invalid order: missing order_uid")
			continue
		}
	
		if err := c.store.InsertOrder(ctx, &o); err != nil {
			log.Printf("db error: %v", err)
			continue
		}

		c.cache.Set(&o)
	}
}

func (c *Consumer) Close() error { return c.reader.Close() }
