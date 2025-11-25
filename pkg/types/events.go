package types

type Event struct {
	Timestamp int64  `json:"ts"`
	Message   string `json:"msg"`
	Source    string `json:"source,omitempty"`
}
