package scheduling

type Proxy struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Login    string `json:"login"`
	Password string `json:"password"`
	Type     string `json:"type"`
}
