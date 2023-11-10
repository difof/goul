package web

import "encoding/json"

type UserInitData struct {
	Id              int64  `json:"id"`
	FirstName       string `json:"first_name"`
	LastName        string `json:"last_name"`
	Username        string `json:"username"`
	LanguageCode    string `json:"language_code"`
	AllowsWriteToPm bool   `json:"allows_write_to_pm"`
}

func NewUserInitData(data string) (u *UserInitData, err error) {
	u = &UserInitData{}
	err = json.Unmarshal([]byte(data), u)
	return
}

// String
func (u *UserInitData) String() string {
	b, _ := json.MarshalIndent(u, "", "  ")
	return string(b)
}
