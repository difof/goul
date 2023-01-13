package tgbot

import (
	"context"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"log"
	"sync"
)

// Bot is a Telegram bot using go-telegram-bot-api and long polling.
type Bot struct {
	tg      *tgbotapi.BotAPI
	timeout int

	handlers    map[UpdateType][]*UpdateHandler
	middlewares []UpdateCallback

	closed chan struct{}
	wg     sync.WaitGroup
}

func NewBot(token string, timeout int) (bot *Bot, err error) {
	bot = &Bot{
		timeout:     timeout,
		handlers:    make(map[UpdateType][]*UpdateHandler),
		middlewares: make([]UpdateCallback, 0),
		closed:      make(chan struct{}),
	}

	bot.tg, err = tgbotapi.NewBotAPI(token)
	if err != nil {
		return
	}

	bot.populateUpdateTypes()

	return
}

// Client returns the Telegram client.
func (b *Bot) Client() *tgbotapi.BotAPI {
	return b.tg
}

// On registers a handler for a specific update type.
func (b *Bot) On(updateType UpdateType, handler UpdateCallback, filters ...UpdateFilter) *UpdateHandler {
	h := NewUpdateHandler(handler, filters...)
	b.handlers[updateType] = append(b.handlers[updateType], h)
	return h
}

// Off unregisters a handler for a specific update type.
func (b *Bot) Off(updateType UpdateType, handler *UpdateHandler) {
	handlers := b.handlers[updateType]
	for i, h := range handlers {
		if h.ID.String() == handler.ID.String() {
			b.handlers[updateType] = append(handlers[:i], handlers[i+1:]...)
			break
		}
	}
}

// Use adds a middleware to the bot.
func (b *Bot) Use(handler UpdateCallback) {
	b.middlewares = append(b.middlewares, handler)
}

// Start starts polling for updates.
func (b *Bot) Start(ctx context.Context, offset int) {
	u := tgbotapi.NewUpdate(offset)
	u.Timeout = b.timeout

	updates := b.tg.GetUpdatesChan(u)

	for {
		select {
		case <-ctx.Done():
			b.tg.StopReceivingUpdates()
			b.wg.Wait()
			close(b.closed)
			return
		case update := <-updates:
			b.wg.Add(1)
			go b.handle(ctx, NewWrappedUpdate(update))
		}
	}
}

// Wait waits for all handlers to finish after closing context.
func (b *Bot) Wait() {
	<-b.closed
}

// handle handles an update.
func (b *Bot) handle(ctx context.Context, update *WrappedUpdate) {
	defer b.wg.Done()

	uctx := NewUpdateContext(b, update, ctx)

	for _, middleware := range b.middlewares {
		err := middleware(uctx)
		if err != nil {
			log.Println("middleware error:", err)
			return
		}
	}

	for _, handler := range b.handlers[update.Type] {
		ok, err := handler.ApplyFilters(update)
		if err != nil {
			log.Printf("error applying filters on update [%s]: %v", update, err)
			continue
		}

		if !ok {
			continue
		}

		if err = handler.Callback(uctx); err != nil {
			log.Printf("error handling update [%s]: %v", update, err)
		}

		if uctx.propagationStopped {
			break
		}
	}
}

// populateUpdateTypes populates the update types for handlers.
func (b *Bot) populateUpdateTypes() {
	for i := 0; i <= int(UpdateTypeChatJoinRequest); i++ {
		b.handlers[UpdateType(i)] = []*UpdateHandler{}
	}
}
