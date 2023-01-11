package tgbot

import "github.com/gofrs/uuid"

// UpdateCallback is a function that handles an update.
type UpdateCallback func(ctx UpdateContext) error

//type UpdateFilter func(update *WrappedUpdate) (bool, error)

type UpdateHandler struct {
	ID       uuid.UUID
	Callback UpdateCallback
	Filters  []UpdateFilter
}

func NewUpdateHandler(handler UpdateCallback, filters ...UpdateFilter) *UpdateHandler {
	return &UpdateHandler{
		ID:       uuid.Must(uuid.NewV4()),
		Callback: handler,
		Filters:  filters,
	}
}

// ApplyFilters applies all filters to the update.
func (h *UpdateHandler) ApplyFilters(update *WrappedUpdate) (bool, error) {
	for _, filter := range h.Filters {
		ok, err := filter(update)

		if err != nil {
			return false, err
		}

		if !ok {
			return false, nil
		}
	}

	return true, nil
}
