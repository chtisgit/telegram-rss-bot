package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	tgbotapi "github.com/chtisgit/telegram-bot-api"
	"github.com/mmcdole/gofeed"
)

type Message struct {
	ChatID int64
	Text   string
}

func checkPeriodic(ctx context.Context, db *DB, out chan<- Message) {
	const waitTime = time.Hour
	wait := time.NewTimer(waitTime)

	fp := gofeed.NewParser()

	for {
		feeds, err := db.Feeds(ctx)
		if err != nil {
			log.Println("error: checkPeriodic: ", err)
			goto sel
		}

		log.Println("periodic check started")
		for info := range feeds {
			log.Println("load feed ", info.URL)

			feed, err := fp.ParseURL(info.URL)
			if err != nil {
				log.Println("error with feed ", info.URL)
				continue
			}

			updated := feed.UpdatedParsed
			if updated == nil {
				log.Println("error with feed ", info.URL, ": no timestamp")
				continue
			}

			subs, err := db.Subs(ctx, info.ID, updated)
			if err != nil {
				log.Println("error: getting chat ids: ", err)
				continue
			}

			log.Printf("%d chats subscribed to this feed\n", len(subs))
			for sub := range subs {
				newItems := []*gofeed.Item{}
				for _, item := range feed.Items {
					if item.PublishedParsed != nil && item.PublishedParsed.After(sub.LastUpdate) {
						newItems = append(newItems, item)
					}
				}

				log.Printf("for chat %d, there are %d items published after the last update on %s\n", sub.ChatID, len(newItems), sub.LastUpdate)

				sort.Slice(newItems, func(i, j int) bool {
					return newItems[i].PublishedParsed.Before(*newItems[j].PublishedParsed)
				})

				for _, item := range newItems {
					out <- Message{
						ChatID: sub.ChatID,
						Text:   item.Title + "\n" + item.Description + "\n\nLink: " + item.Link,
					}
					db.UpdateSub(ctx, sub.ChatID, info.ID, *item.PublishedParsed)
				}
			}
		}
		log.Println("periodic check ended")

	sel:
		if !wait.Stop() {
			<-wait.C
		}
		wait.Reset(waitTime)

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

func main() {
	db, err := OpenDB(dbSource)
	if err != nil {
		log.Fatalln("error: db: ", err)
	}

	db.MaxFeedsPerChat = 10

	bot, err := tgbotapi.NewBotAPI(apiKey)
	if err != nil {
		log.Fatalln("error: bot api: ", err)
	}

	log.Printf("Authorized on account %s", bot.Self.UserName)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updateCh, err := bot.GetUpdatesChan(u)

	fp := gofeed.NewParser()

	sendCh := make(chan Message)

	go checkPeriodic(context.Background(), db, sendCh)

	for {
		select {
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

			log.Printf("user %s wrote command %s %s", user.UserName, cmd, args)
			msg := tgbotapi.NewMessage(chatID, "")
			switch cmd {
			case "help":
				msg.Text = helptext
			case "addfeed":
				if user.UserName != "realchtis" {
					bot.Send(tgbotapi.NewMessage(chatID, "You may not do this."))
					break
				}

				url := strings.TrimSpace(args)
				if url == "" {
					msg.Text = "copy the URL of the feed after the command"
					break
				}

				title := ""
				info, err := db.FeedByURL(context.Background(), url)
				if err != nil {
					feed, err := fp.ParseURL(url)
					if err != nil {
						msg.Text = "error while fetching feed: " + err.Error()
						break
					}

					title = feed.Title
				} else {
					title = info.Title
				}

				err = db.AddFeedToChat(context.Background(), int64(user.ID), chatID, Feed{
					Title: title,
					URL:   url,
				})
				switch err {
				case nil:
					msg.Text = fmt.Sprintf("Feed \"%s\" was added to this chat.", title)

				case ErrMaxFeedsInChat:
					msg.Text = "You cannot add more feeds to this chat."
					log.Printf("error: maximum reached for user %s (%d): %s", user.UserName, user.ID, err)

				case ErrMaxActiveFeedsByUser, ErrMaxTotalFeedsByUser:
					log.Printf("error: maximum reached for user %s (%d): %s", user.UserName, user.ID, err)
					msg.Text = "I think you have added enough feeds for now."

				default:
					log.Println("error: add feed to chat (user %s [%d]): ", user.UserName, user.ID, err)
					msg.Text = "Backend error"
				}

			case "feeds":
				feeds, err := db.FeedsByChat(context.Background(), chatID)
				if err != nil {
					log.Println("error: enumerate feeds: ", err)
					msg.Text = "Backend error"
					break
				}

				msg.Text = "Feeds in this chat:\n"
				anyFeeds := false
				for feed := range feeds {
					msg.Text += fmt.Sprintf("[%d] %s (url %s)\n", feed.ID, feed.Title, feed.URL)
					anyFeeds = true
				}

				if !anyFeeds {
					msg.Text = "No feeds in this chat."
				}

			case "removefeed":
				num, err := strconv.ParseInt(args, 10, 64)
				if err != nil {
					msg.Text = "Please provide the ID of the feed to remove"
					break
				}

				if err := db.RemoveFeedFromChat(context.Background(), chatID, num); err != nil {
					log.Println("error: removing feed: ", err)
					msg.Text = "Backend error"
					break
				}

				msg.Text = "Feed was removed."
			default:
				msg.Text = "I don't know that command"
			}

			bot.Send(msg)
		}
	}
}
