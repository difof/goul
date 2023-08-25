package telegram

type UpdateFilter func(c *UpdateContext) (bool, error)

type ChatType string

const (
	ChatTypePrivate    ChatType = "private"
	ChatTypeGroup      ChatType = "group"
	ChatTypeSuperGroup ChatType = "supergroup"
	ChatTypeChannel    ChatType = "channel"
)

// FilterByChatType returns a filter that filters updates by chat type.
func FilterByChatType(t ChatType) UpdateFilter {
	return func(c *UpdateContext) (bool, error) {
		return c.FromChat().Type == string(t), nil
	}
}

// FilterByCommand returns a filter that filters updates by command.
func FilterByCommand(command string) UpdateFilter {
	return func(c *UpdateContext) (bool, error) {
		if !c.Message.IsCommand() {
			return false, nil
		}

		return c.Message.Command() == command, nil
	}
}

// AnyFilter combines multiple filters into one, which returns true if any of the filters return true.
func AnyFilter(filters ...UpdateFilter) UpdateFilter {
	return func(c *UpdateContext) (bool, error) {
		for _, filter := range filters {
			ok, err := filter(c)

			if err != nil {
				return false, err
			}

			if ok {
				return true, nil
			}
		}

		return false, nil
	}
}
