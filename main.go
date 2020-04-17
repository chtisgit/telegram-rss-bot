package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	tgbotapi "github.com/chtisgit/telegram-bot-api"
	"github.com/mmcdole/gofeed"
)

type Message struct {
	ChatID int64
	Text   string
}

const waitBetweenUpdatesTime = time.Hour
const updateTimeout = time.Minute

func update(parentCtx context.Context, db *DB, out chan<- Message) (anyErr error) {
	ctx, cancel := context.WithTimeout(parentCtx, updateTimeout)
	defer cancel()

	fp := gofeed.NewParser()

	feeds, err := db.Feeds(ctx)
	if err != nil {
		logrus.WithError(err).Error("update: get feeds")
		return err
	}

	for info := range feeds {
		url := "https:" + info.URL
		logrus.WithError(err).WithField("Feed", url).Debug("update: load feed")

		feed, err := fp.ParseURLWithContext(url, ctx)
		if err != nil {
			logrus.WithError(err).WithField("Feed", url).Error("update: error with feed (parsing)")

			if ctx.Err() != nil {
				return ctx.Err()
			}

			continue
		}

		updated := feed.UpdatedParsed
		if updated == nil {
			logrus.WithError(err).WithField("Feed", url).Error("update: no timestamp")
			continue
		}

		subs, err := db.Subs(ctx, info.ID, updated)
		if err != nil {
			logrus.WithError(err).WithField("Feed", url).Error("update: getting chat IDs")

			if ctx.Err() != nil {
				return ctx.Err()
			}

			continue
		}

		logrus.WithFields(logrus.Fields{
			"#Chats": len(subs),
			"Feed":   info.URL,
		}).Debug("update: chats that need update")

		for sub := range subs {
			newItems := []*gofeed.Item{}
			for _, item := range feed.Items {
				if item.PublishedParsed != nil && item.PublishedParsed.After(sub.LastUpdate) {
					newItems = append(newItems, item)
				}
			}

			if len(newItems) == 0 {
				continue
			}

			logrus.WithFields(logrus.Fields{
				"Chat ID":      sub.ChatID,
				"New Items":    len(newItems),
				"Chat updated": sub.LastUpdate,
				"Feed updated": updated,
			}).Debug("update: new items for chat")

			sort.Slice(newItems, func(i, j int) bool {
				return newItems[i].PublishedParsed.Before(*newItems[j].PublishedParsed)
			})

			for _, item := range newItems {
				out <- Message{
					ChatID: sub.ChatID,
					Text:   item.Title + "\n" + item.Description + "\n\nLink: " + item.Link,
				}

				anyErr = db.UpdateSub(ctx, sub.ChatID, info.ID, *item.PublishedParsed)
				logrus.WithError(anyErr).Error("update: UpdateSub")

				if ctx.Err() != nil {
					return ctx.Err()
				}
			}
		}
	}

	return
}

func periodicUpdate(ctx context.Context, db *DB, out chan<- Message) {
	wait := time.NewTimer(waitBetweenUpdatesTime)

	for {
		logrus.Info("periodic update started")

		err := update(ctx, db, out)
		if err != nil && err == ctx.Err() {
			logrus.WithContext(ctx).Error("update took too long.")
		}

		logrus.Info("periodic update ended")

		if !wait.Stop() {
			<-wait.C
		}
		wait.Reset(waitBetweenUpdatesTime)

		select {
		case <-ctx.Done():
			if !wait.Stop() {
				<-wait.C
			}

			return
		case <-wait.C:
		}
	}
}

const helptext = `This bot can serve you in the following ways:

/addfeed <url>  ... Adds an RSS/Atom feed to this chat
/feeds ... Lists the feeds that are assigned to this chat
/removefeed <id> ... Remove a particular feed from this chat (use the number from feeds command)
`

func addFeed(ctx context.Context, db *DB, user tgbotapi.User, chatID int64, feedURL string) tgbotapi.Chattable {
	logrus.WithFields(logrus.Fields{
		"Username": user.UserName,
		"Name":     user.FirstName + " " + user.LastName,
		"User ID":  user.ID,
		"Chat ID":  chatID,
		"Feed URL": feedURL,
	}).Debug("/addfeed command")

	fp := gofeed.NewParser()

	u, err := url.Parse(feedURL)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"Feed URL": feedURL,
		}).Warn("cannot parse URL")

		return tgbotapi.NewMessage(chatID, "Your feed is fishy.")
	}

	u.Scheme = ""
	url := u.String()

	title := ""
	info, err := db.FeedByURL(ctx, url)
	if err != nil {
		// try to fetch the feed via HTTPS
		u.Scheme = "https"

		feed, err := fp.ParseURLWithContext(u.String(), ctx)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"Feed URL":  feedURL,
				"HTTPS URL": u.String(),
			}).Warn("cannot fetch feed")

			return tgbotapi.NewMessage(chatID, "I cannot fetch your feed using HTTPS :(")
		}

		title = feed.Title
	} else {
		title = info.Title
	}

	err = db.AddFeedToChat(ctx, int64(user.ID), chatID, Feed{
		Title: title,
		URL:   url,
	})

	msg := tgbotapi.NewMessage(chatID, "")
	switch err {
	case nil:
		msg.Text = fmt.Sprintf("Feed \"%s\" was added to this chat.", title)

	case ErrMaxFeedsInChat:
		msg.Text = "You cannot add more feeds to this chat."

		logrus.WithFields(logrus.Fields{
			"Username": user.UserName,
			"User ID":  user.ID,
			"Chat ID":  chatID,
		}).Error("maximum feeds in chat reached")

	case ErrMaxActiveFeedsByUser, ErrMaxTotalFeedsByUser:
		msg.Text = "I think you have added enough feeds for now."

		logrus.WithFields(logrus.Fields{
			"Username": user.UserName,
			"User ID":  user.ID,
		}).WithError(err).Error("maximum feeds by user reached")

	default:
		msg.Text = "Backend error"

		logrus.WithFields(logrus.Fields{
			"Username": user.UserName,
			"User ID":  user.ID,
		}).WithError(err).Error("unknown error in AddFeedToChat")
	}

	return msg
}

func main() {
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	logrus.SetLevel(logrus.DebugLevel)

	db, err := OpenDB(dbSource)
	if err != nil {
		logrus.WithError(err).Fatalln("cannot open DB")
	}

	defer db.Close()

	db.MaxFeedsPerChat = 10
	db.MaxTotalFeedsByUser = 200
	db.MaxActiveFeedsByUser = 20
	db.Prepare()

	bot, err := tgbotapi.NewBotAPI(apiKey)
	if err != nil {
		logrus.WithError(err).Fatalln("bot api error")
	}

	logrus.WithField("Bot User", bot.Self.UserName).Info("Authorized")

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updateCh, err := bot.GetUpdatesChan(u)

	sendCh := make(chan Message)

	osSignals := make(chan os.Signal, 1)

	signal.Notify(osSignals, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	go periodicUpdate(ctx, db, sendCh)

	logrus.Info("Ready")
	for {
		select {
		case <-ctx.Done():
			logrus.Info("shutting down")
			return

		case sig := <-osSignals:
			logrus.Info("received signal %s", sig)
			cancel()

		case msg := <-sendCh:
			bot.Send(tgbotapi.NewMessage(msg.ChatID, msg.Text))

		case update := <-updateCh:
			if update.Message == nil {
				continue
			}

			if !update.Message.IsCommand() {
				continue
			}

			cmd := update.Message.Command()
			args := update.Message.CommandArguments()
			chatID := update.Message.Chat.ID
			user := update.Message.From

			logrus.WithFields(logrus.Fields{
				"User ID":  user.ID,
				"Username": user.UserName,
				"Cmd":      cmd,
				"Args":     args,
			}).Debug("received command")

			switch cmd {
			case "help":
				bot.Send(tgbotapi.NewMessage(chatID, helptext))

			case "addfeed":
				if user.UserName != "realchtis" {
					bot.Send(tgbotapi.NewMessage(chatID, "You may not do this."))
					break
				}

				args = strings.TrimSpace(args)
				if args == "" {
					bot.Send(tgbotapi.NewMessage(chatID, "copy the URL of the feed after the command"))
					break
				}

				go func() {
					msg := addFeed(ctx, db, *user, chatID, args)
					if msg != nil {
						bot.Send(msg)
					}
				}()

			case "feeds":
				feeds, err := db.FeedsByChat(ctx, chatID)
				if err != nil {
					logrus.WithError(err).WithField("Chat ID", chatID).Error("enumerating feeds of chat")
					bot.Send(tgbotapi.NewMessage(chatID, "Backend error"))
					break
				}

				text := "Feeds in this chat:\n"
				anyFeeds := false
				for feed := range feeds {
					text += fmt.Sprintf("[%d] %s (url %s)\n", feed.ID, feed.Title, feed.URL)
					anyFeeds = true
				}

				if !anyFeeds {
					text = "No feeds in this chat."
				}

				bot.Send(tgbotapi.NewMessage(chatID, text))

			case "removefeed":
				num, err := strconv.ParseInt(args, 10, 64)
				if err != nil {
					bot.Send(tgbotapi.NewMessage(chatID, "Please provide the ID of the feed to remove"))
					break
				}

				if err := db.RemoveFeedFromChat(ctx, chatID, num); err != nil {
					logrus.WithError(err).WithFields(logrus.Fields{
						"Chat ID": chatID,
						"#":       num,
					}).Error("remove feed from chat failed")

					bot.Send(tgbotapi.NewMessage(chatID, "Backend error"))
					break
				}

				bot.Send(tgbotapi.NewMessage(chatID, "Feed was removed."))
			default:
				bot.Send(tgbotapi.NewMessage(chatID, "I don't know that command"))
			}
		}
	}
}
