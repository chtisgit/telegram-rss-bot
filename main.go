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

const configfilePath = "/etc/telegram-rss-bot.toml"
const waitBetweenUpdatesTime = time.Hour
const updateTimeout = time.Minute * 20

type sendFunc func(chatID int64, text string)

var firstSecond = time.Unix(0, 0)

func feedError(ctx context.Context, db *DB, feed *Feed, send sendFunc) {
	if n, err := db.RecentFeedErrors(ctx, time.Now().Add(-time.Hour*12), feed.ID); err != nil {
		return
	} else if n >= 9 {
		logrus.WithField("Feed", feed.URL).Error("too many errors, dropping feed")

		var chatIDs []int64
		subs, err := db.Subs(ctx, feed.ID, &firstSecond)
		if err != nil {
			logrus.WithError(err).WithField("Feed", feed.URL).Error("failed to fetch subs for feed")
		} else {
			for sub := range subs {
				chatIDs = append(chatIDs, sub.ChatID)
			}
		}

		if err := db.DropFeed(ctx, feed.ID); err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"Errors/12h": n,
				"Feed":       feed.URL,
			}).Error("cannot drop feed")
			return
		}

		go func() {
			for _, chatID := range chatIDs {
				send(chatID, fmt.Sprintf("Your feed \"%s\" was removed because it could not be loaded multiple times.", feed.Title))
			}
		}()
	}
}

func update(parentCtx context.Context, db *DB, send sendFunc) (anyErr error) {
	ctx, cancel := context.WithTimeout(parentCtx, updateTimeout)
	defer cancel()

	fp := gofeed.NewParser()

	updateCount := 0
	defer logrus.Infof("update: Sent %d feed updates to chats.", updateCount)

	feeds, err := db.Feeds(ctx)
	if err != nil {
		logrus.WithError(err).Error("update: get feeds")
		return err
	}

	for info := range feeds {
		url := "https:" + info.URL
		logrus.WithField("Feed", url).Debug("update: load feed")

		feed, err := fp.ParseURLWithContext(url, ctx)
		if err != nil {
			logrus.WithError(err).WithField("Feed", url).Error("update: error with feed (parsing)")

			if ctx.Err() != nil {
				return ctx.Err()
			}

			feedError(ctx, db, &info, send)

			continue
		}

		updated := feed.UpdatedParsed
		if updated == nil {
			updated = &firstSecond
			for _, item := range feed.Items {
				pub := item.PublishedParsed
				if pub != nil && pub.After(*updated) {
					updated = pub
				}
			}

			if updated == &firstSecond {
				logrus.WithError(err).WithField("Feed", url).Error("update: no timestamps")
				feedError(ctx, db, &info, send)
				continue
			}
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
				send(sub.ChatID, fmt.Sprintf("%s\n%s\n\nLink: %s", item.Title, item.Description, item.Link))
				updateCount++

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

func periodicUpdate(ctx context.Context, db *DB, send sendFunc) {
	tick := time.NewTicker(waitBetweenUpdatesTime)
	defer tick.Stop()

	for {
		logrus.Info("periodic update started")

		err := update(ctx, db, send)
		if err != nil && err == ctx.Err() {
			logrus.WithContext(ctx).Error("update took too long.")
		}

		logrus.Info("periodic update ended")

		select {
		case <-ctx.Done():
			return
		case <-tick.C:
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

	cfg, err := loadConfigFile(configfilePath)
	if err != nil {
		logrus.WithError(err).WithField("path", configfilePath).Fatalln("Cannot open config file")
	}

	db, err := OpenDB(cfg.DB.Source)
	if err != nil {
		logrus.WithError(err).Fatalln("cannot open DB")
	}

	defer db.Close()

	db.MaxFeedsPerChat = cfg.Bot.MaxFeedsPerChat
	db.MaxTotalFeedsByUser = cfg.Bot.MaxTotalFeedsByUser
	db.MaxActiveFeedsByUser = cfg.Bot.MaxActiveFeedsByUser
	db.Prepare()

	bot, err := tgbotapi.NewBotAPI(cfg.Bot.APIKey)
	if err != nil {
		logrus.WithError(err).Fatalln("bot api error")
	}

	logrus.WithField("Bot User", bot.Self.UserName).Info("Authorized")

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updateCh, err := bot.GetUpdatesChan(u)

	sendCh := make(chan tgbotapi.Chattable)
	send := func(chatID int64, text string) {
		sendCh <- tgbotapi.NewMessage(chatID, text)
	}

	osSignals := make(chan os.Signal, 1)

	signal.Notify(osSignals, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	go periodicUpdate(ctx, db, send)

	if len(cfg.Bot.UserWhitelist) == 0 {
		logrus.Info("No whitelist active")
	} else {
		logrus.Info("Whitelisting these users: ", cfg.Bot.UserWhitelist)
	}

	logrus.Info("Ready")
	for {
		select {
		case <-ctx.Done():
			logrus.Info("shutting down")
			return

		case sig := <-osSignals:
			logrus.Infof("received signal %s", sig)
			cancel()

		case c := <-sendCh:
			bot.Send(c)

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
			fullName := fmt.Sprint(user.FirstName, " ", user.LastName)

			logrus.WithFields(logrus.Fields{
				"User ID":  user.ID,
				"Username": user.UserName,
				"Cmd":      cmd,
				"Args":     args,
			}).Debug("received command")

			if cfg.Bot.LogRequests {
				if n, err := db.RecentRequests(ctx, time.Now().Add(-time.Minute*5), int64(user.ID)); err != nil {
					logrus.WithError(err).Error("recent requests select error")
				} else if n > 25 {
					logrus.WithFields(logrus.Fields{
						"User":     fullName,
						"Username": user.UserName,
					}).Error("many requests coming from user. ignoring.")
					continue
				}

				err := db.LogRequest(ctx, fullName, update.Message.Text, int64(user.ID))
				if err != nil {
					logrus.WithError(err).Warn("cannot log request")
				}
			}

			switch cmd {
			case "help":
				bot.Send(tgbotapi.NewMessage(chatID, helptext))

			case "addfeed":
				if !cfg.IsWhitelisted(user.UserName) {
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
					text += fmt.Sprintf("[%d] %s (url https:%s)\n", feed.ID, feed.Title, feed.URL)
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
