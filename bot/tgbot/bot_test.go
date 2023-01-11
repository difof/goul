package tgbot

import (
	"context"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"os"
	"testing"
	"time"
)

var token string

func init() {
	token = os.Getenv("TOKEN")
}

func TestBot_WebApp(t *testing.T) {
	bot, err := NewBot(token, 60)
	if err != nil {
		t.Fatal("failed to create bot", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()

	bot.On(UpdateTypeMessage, func(ctx UpdateContext) error {
		msg := tgbotapi.NewMessage(ctx.Update.Message.Chat.ID, "Hello, World!")
		msg.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("Button 1", "1"),
				tgbotapi.NewInlineKeyboardButtonData("Button 2", "2")),
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("Button 3", "3")))

		if _, err := ctx.Client().Send(msg); err != nil {
			return err
		}

		return nil
	}, FilterByChatType(ChatTypePrivate))

	bot.On(UpdateTypeCallbackQuery, func(ctx UpdateContext) error {
		cb := tgbotapi.NewCallback(ctx.Update.CallbackQuery.ID, ctx.Update.CallbackQuery.Data)
		cb.ShowAlert = true
		cb.Text = "You clicked " + ctx.Update.CallbackQuery.Data

		if _, err := ctx.Client().Request(cb); err != nil {
			return err
		}

		return nil
	})

	bot.Start(ctx, 0)
}
