package types

type Event struct {
	ID        string `json:"id"`
	Timestamp int64  `json:"ts"`
	Message   string `json:"msg"`
	Source    string `json:"source,omitempty"`
}
