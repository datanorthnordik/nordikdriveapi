package chat

const (
	chatFastModel    = "gemini-2.5-flash"
	chatQualityModel = "gemini-2.5-pro"
)

func selectChatModelPlan(_ ChatDebugMetrics, _ bool) (string, string) {
	return chatFastModel, chatQualityModel
}
