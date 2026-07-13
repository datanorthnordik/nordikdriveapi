package chat

const (
	chatFastModel    = "gemini-2.5-flash"
	chatQualityModel = "gemini-2.5-pro"
)

func selectChatModelPlan(debug ChatDebugMetrics, hasAudio bool) (string, string) {
	if hasAudio {
		return chatFastModel, chatQualityModel
	}

	if debug.Strategy == "structured_retrieval" &&
		debug.RetrievalMode == "compact_dataset" &&
		debug.RowsSelected >= 15 &&
		debug.PromptBytes >= 1200 {
		return chatQualityModel, chatFastModel
	}

	return chatFastModel, chatQualityModel
}
