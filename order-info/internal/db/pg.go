package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/MaksProg/order-service/internal/model"
)

type Store struct {
	Pool *pgxpool.Pool
}

func New(ctx context.Context, host string, port int, user, pass, name string) (*Store, error) {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", user, pass, host, port, name)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &Store{Pool: pool}, nil
}

func (s *Store) Close() { s.Pool.Close() }

func (s *Store) InsertOrder(ctx context.Context, o *model.Order) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil { return err }
	defer func() {
		if err != nil { _ = tx.Rollback(ctx) } else { _ = tx.Commit(ctx) }
	}()

	_, err = tx.Exec(ctx, `
		INSERT INTO orders (order_uid, track_number, entry, locale, internal_signature, customer_id,
		                    delivery_service, shardkey, sm_id, date_created, oof_shard)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (order_uid) DO NOTHING
	`, o.OrderUID, o.TrackNumber, o.Entry, o.Locale, o.InternalSignature, o.CustomerID,
		o.DeliveryService, o.ShardKey, o.SmID, o.DateCreated, o.OofShard)
	if err != nil { return err }

	_, err = tx.Exec(ctx, `
		INSERT INTO deliveries (order_uid, name, phone, zip, city, address, region, email)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
	`, o.OrderUID, o.Delivery.Name, o.Delivery.Phone, o.Delivery.Zip, o.Delivery.City,
		o.Delivery.Address, o.Delivery.Region, o.Delivery.Email)
	if err != nil { return err }

	_, err = tx.Exec(ctx, `
		INSERT INTO payments (order_uid, transaction, request_id, currency, provider, amount,
		                      payment_dt, bank, delivery_cost, goods_total, custom_fee)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
	`, o.OrderUID, o.Payment.Transaction, o.Payment.RequestID, o.Payment.Currency, o.Payment.Provider,
		o.Payment.Amount, o.Payment.PaymentDT, o.Payment.Bank, o.Payment.DeliveryCost, o.Payment.GoodsTotal, o.Payment.CustomFee)
	if err != nil { return err }

	for _, it := range o.Items {
		_, err = tx.Exec(ctx, `
			INSERT INTO items (order_uid, chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		`, o.OrderUID, it.ChrtID, it.TrackNumber, it.Price, it.RID, it.Name, it.Sale, it.Size,
			it.TotalPrice, it.NmID, it.Brand, it.Status)
		if err != nil { return err }
	}
	return nil
}

func (s *Store) GetOrder(ctx context.Context, id string) (*model.Order, error) {
	var o model.Order
	err := s.Pool.QueryRow(ctx, `
		SELECT order_uid, track_number, entry, locale, internal_signature, customer_id,
		       delivery_service, shardkey, sm_id, date_created, oof_shard
		FROM orders WHERE order_uid=$1
	`, id).Scan(&o.OrderUID, &o.TrackNumber, &o.Entry, &o.Locale, &o.InternalSignature, &o.CustomerID,
		&o.DeliveryService, &o.ShardKey, &o.SmID, &o.DateCreated, &o.OofShard)
	if err != nil { return nil, err }

	err = s.Pool.QueryRow(ctx, `
		SELECT name, phone, zip, city, address, region, email
		FROM deliveries WHERE order_uid=$1
	`, id).Scan(&o.Delivery.Name, &o.Delivery.Phone, &o.Delivery.Zip, &o.Delivery.City,
		&o.Delivery.Address, &o.Delivery.Region, &o.Delivery.Email)
	if err != nil { return nil, err }

	rows, err := s.Pool.Query(ctx, `
		SELECT transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee
		FROM payments WHERE order_uid=$1
	`, id)
	if err != nil { return nil, err }
	defer rows.Close()
	if rows.Next() {
		err = rows.Scan(&o.Payment.Transaction, &o.Payment.RequestID, &o.Payment.Currency, &o.Payment.Provider,
			&o.Payment.Amount, &o.Payment.PaymentDT, &o.Payment.Bank, &o.Payment.DeliveryCost, &o.Payment.GoodsTotal, &o.Payment.CustomFee)
		if err != nil { return nil, err }
	}

	items, err := s.Pool.Query(ctx, `
		SELECT chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status
		FROM items WHERE order_uid=$1
	`, id)
	if err != nil { return nil, err }
	defer items.Close()
	for items.Next() {
		var it model.Item
		if err := items.Scan(&it.ChrtID, &it.TrackNumber, &it.Price, &it.RID, &it.Name, &it.Sale, &it.Size,
			&it.TotalPrice, &it.NmID, &it.Brand, &it.Status); err != nil { return nil, err }
		o.Items = append(o.Items, it)
	}
	return &o, nil
}
