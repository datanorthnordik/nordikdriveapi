package chat

type ttsJSON struct {
	Text   string `json:"text"`
	Answer string `json:"answer"`
}

type TTSAudio struct {
	MimeType string
	Data     []byte
}

const (
	ttsModel      = "gemini-2.5-flash-tts"
	ttsVoiceName  = "Algenib"
	ttsStyleInstr = "empathetic and more human like"
	ttsLanguage   = "English (United States)"
)

const (
	ttsSampleRate    = 24000
	ttsChannels      = 1
	ttsBitsPerSample = 16
)
