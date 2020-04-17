package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type DB struct {
	q *sql.DB

	MaxFeedsPerChat int
}

var ErrMaxFeedsInChat = errors.New("chat is already at maximum feeds")

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

	var feedsInChat int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM feeds WHERE chatID=?", chatID).Scan(&feedsInChat); err != nil {
		tx.Rollback()
		return err
	} else if db.MaxFeedsPerChat != 0 && feedsInChat >= db.MaxFeedsPerChat {
		tx.Rollback()
		return ErrMaxFeedsInChat
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

	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (db *DB) FeedsByChat(ctx context.Context, chatID int64) (<-chan Feed, error) {
	rows, err := db.q.QueryContext(ctx, "SELECT ROW_NUMBER() OVER (),feeds.title,feeds.url FROM updates JOIN feeds on updates.feedID = feeds.id WHERE updates.chatID = ? ORDER BY nr", chatID)
	if err != nil {
		return nil, err
	}

	ch := make(chan Feed)
	go func() {
		defer close(ch)

		for rows.Next() {
			var feed Feed

			if err := rows.Scan(&feed.ID, &feed.Title, &feed.URL); err != nil {
				rows.Close()
				break
			}

			ch <- feed
		}
	}()

	return ch, nil
}

func (db *DB) RemoveFeedFromChat(ctx context.Context, chatID, feedNum int64) error {
	var feedID int64
	row := db.q.QueryRowContext(ctx, fmt.Sprintf("SELECT feeds.id FROM updates JOIN feeds on updates.feedID = feeds.id WHERE updates.chatID = ? ORDER BY nr LIMIT %d, 1", feedNum-1), chatID)
	if err := row.Scan(&feedID); err != nil {
		return err
	}

	_, err := db.q.ExecContext(ctx, "DELETE FROM updates WHERE chatID=? AND feedID=?", chatID, feedID)
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
				rows.Close()
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
	rows, err := db.q.QueryContext(ctx, "SELECT chatID, lastUpdate FROM updates WHERE feedID=? AND updates.lastUpdate < ?", feedID, latestUpdate.Unix())
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
