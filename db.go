package main

import (
	"context"
	"database/sql"
	"time"
)
import _ "github.com/go-sql-driver/mysql"

type DB struct {
	q *sql.DB
}

func OpenDB(url string) (*DB, error) {
	q, err := sql.Open("mysql", url)
	if err != nil {
		return nil, err
	}

	if err := q.Ping(); err != nil {
		return nil, err
	}

	return &DB{
		q: q,
	}, nil
}

func (db *DB) Close() error {
	return db.q.Close()
}

func (db *DB) AddFeedToChat(ctx context.Context, chatID int64, feed Feed) error {
	tx, err := db.q.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	var feedID int64
	if err := tx.QueryRowContext(ctx, "SELECT id FROM feeds WHERE url=?", feed.URL).Scan(&feedID); err != nil {
		res, err := tx.ExecContext(ctx, "INSERT INTO feeds (url,title) VALUES (?,?)", feed.URL, feed.Title)
		if err != nil {
			tx.Rollback()
			return err
		}

		feedID, err = res.LastInsertId()
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO updates (chatID, feedID, channel, lastUpdate) VALUES (?, ?, NULL, ?)", chatID, feedID, time.Now().Unix())

	if err == nil {
		err = tx.Commit()
	} else {
		tx.Rollback()
	}

	return err
}

type Feed struct {
	ID    int64
	Title string
	URL   string
}

func (db *DB) FeedByURL(ctx context.Context, url string) (f Feed, err error) {
	f.URL = url
	err = db.q.QueryRowContext(ctx, "SELECT id,title WHERE url=?", url).Scan(&f.ID, &f.Title)
	return
}

func (db *DB) Feeds(ctx context.Context) (<-chan Feed, error) {
	rows, err := db.q.QueryContext(ctx, "SELECT id,url FROM feeds")
	if err != nil {
		return nil, err
	}

	ch := make(chan Feed)
	go func() {
		defer close(ch)

		for rows.Next() {
			var id int64
			var url string
			if err := rows.Scan(&id, &url); err != nil {
				break
			}

			ch <- Feed{
				ID:  id,
				URL: url,
			}
		}
	}()

	return ch, nil
}

type Sub struct {
	ChatID int64

	LastUpdate time.Time
}

func (db *DB) Subs(ctx context.Context, feedID int64, latestUpdate *time.Time) (<-chan Sub, error) {
	rows, err := db.q.QueryContext(ctx, "SELECT chatID, lastUpdate FROM updates WHERE updates.lastUpdate < ?", latestUpdate.Unix())
	if err != nil {
		return nil, err
	}

	ch := make(chan Sub)
	go func() {
		defer close(ch)

		for rows.Next() {
			var chatID, lastUpdate int64
			if err := rows.Scan(&chatID, &lastUpdate); err != nil {
				break
			}

			ch <- Sub{
				ChatID:     chatID,
				LastUpdate: time.Unix(lastUpdate, 0),
			}
		}
	}()

	return ch, nil
}

func (db *DB) UpdateSub(ctx context.Context, chatID, feedID int64, t time.Time) error {
	_, err := db.q.ExecContext(ctx, "UPDATE updates SET lastUpdate=? WHERE chatID=? AND feedID=?", t.Unix(), chatID, feedID)
	return err
}
