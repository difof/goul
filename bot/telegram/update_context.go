package telegram

import (
	"context"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

type UpdateContext struct {
	*WrappedUpdate

	context            context.Context
	bot                *Bot
	propagationStopped bool
}

// NewUpdateContext creates a new update context.
func NewUpdateContext(bot *Bot, update *WrappedUpdate, ctx context.Context) UpdateContext {
	return UpdateContext{
		WrappedUpdate: update,
		bot:           bot,
		context:       ctx,
	}
}

// Context returns the context of the update.
func (ctx UpdateContext) Context() context.Context {
	return ctx.context
}

// Client returns the Telegram client.
func (ctx UpdateContext) Client() *tgbotapi.BotAPI {
	return ctx.bot.tg
}

// StopPropagation stops the propagation of the update to the next handlers.
func (ctx UpdateContext) StopPropagation() {
	ctx.propagationStopped = true
}
