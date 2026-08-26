package chat

const (
	chatFastModel         = "gemini-2.5-flash"
	chatQualityModel      = "gemini-2.5-pro"
	routedAnswerFastModel = "gemini-3.5-flash-lite"
)

func selectChatModelPlan(_ ChatDebugMetrics, _ bool) (string, string) {
	return chatFastModel, chatQualityModel
}
