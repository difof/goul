package tgbot

import (
	"fmt"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"strings"
)

type UpdateType int

const (
	UpdateTypeMessage UpdateType = iota
	UpdateTypeEditedMessage
	UpdateTypeChannelPost
	UpdateTypeEditedChannelPost
	UpdateTypeInlineQuery
	UpdateTypeChosenInlineResult
	UpdateTypeCallbackQuery
	UpdateTypeShippingQuery
	UpdateTypePreCheckoutQuery
	UpdateTypePoll
	UpdateTypePollAnswer
	UpdateTypeMyChatMember
	UpdateTypeChatMember
	UpdateTypeChatJoinRequest
)

// String returns the string representation of the update type.
func (u UpdateType) String() string {
	switch u {
	case UpdateTypeMessage:
		return "message"
	case UpdateTypeEditedMessage:
		return "edited_message"
	case UpdateTypeChannelPost:
		return "channel_post"
	case UpdateTypeEditedChannelPost:
		return "edited_channel_post"
	case UpdateTypeInlineQuery:
		return "inline_query"
	case UpdateTypeChosenInlineResult:
		return "chosen_inline_result"
	case UpdateTypeCallbackQuery:
		return "callback_query"
	case UpdateTypeShippingQuery:
		return "shipping_query"
	case UpdateTypePreCheckoutQuery:
		return "pre_checkout_query"
	case UpdateTypePoll:
		return "poll"
	case UpdateTypePollAnswer:
		return "poll_answer"
	case UpdateTypeMyChatMember:
		return "my_chat_member"
	case UpdateTypeChatMember:
		return "chat_member"
	case UpdateTypeChatJoinRequest:
		return "chat_join_request"
	default:
		return "unknown"
	}
}

type WrappedUpdate struct {
	// Update is the original update.
	tgbotapi.Update

	// Type is the type of the update.
	Type UpdateType
}

// NewWrappedUpdate creates a new wrapped update.
func NewWrappedUpdate(update tgbotapi.Update) (wrappedUpdate *WrappedUpdate) {
	wrappedUpdate = &WrappedUpdate{
		Update: update,
	}

	switch {
	case update.Message != nil:
		wrappedUpdate.Type = UpdateTypeMessage
	case update.EditedMessage != nil:
		wrappedUpdate.Type = UpdateTypeEditedMessage
	case update.ChannelPost != nil:
		wrappedUpdate.Type = UpdateTypeChannelPost
	case update.EditedChannelPost != nil:
		wrappedUpdate.Type = UpdateTypeEditedChannelPost
	case update.InlineQuery != nil:
		wrappedUpdate.Type = UpdateTypeInlineQuery
	case update.ChosenInlineResult != nil:
		wrappedUpdate.Type = UpdateTypeChosenInlineResult
	case update.CallbackQuery != nil:
		wrappedUpdate.Type = UpdateTypeCallbackQuery
	case update.ShippingQuery != nil:
		wrappedUpdate.Type = UpdateTypeShippingQuery
	case update.PreCheckoutQuery != nil:
		wrappedUpdate.Type = UpdateTypePreCheckoutQuery
	case update.Poll != nil:
		wrappedUpdate.Type = UpdateTypePoll
	case update.PollAnswer != nil:
		wrappedUpdate.Type = UpdateTypePollAnswer
	case update.MyChatMember != nil:
		wrappedUpdate.Type = UpdateTypeMyChatMember
	case update.ChatMember != nil:
		wrappedUpdate.Type = UpdateTypeChatMember
	case update.ChatJoinRequest != nil:
		wrappedUpdate.Type = UpdateTypeChatJoinRequest
	}

	return
}

// String returns the string representation of the wrapped update.
func (w *WrappedUpdate) String() string {
	sb := strings.Builder{}

	chat := w.FromChat()
	user := w.SentFrom()

	if chat != nil {
		sb.WriteString(fmt.Sprintf("in chat %s (%d) ", chat.Title, chat.ID))
	}

	if user != nil {
		sb.WriteString(fmt.Sprintf("from user %s (%d) ", user, user.ID))
	}

	return fmt.Sprintf("%s (%d) %s", w.Type.String(), w.UpdateID, strings.TrimSuffix(sb.String(), " "))
}
