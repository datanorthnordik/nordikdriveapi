package chat

import (
	"net/http"
	"nordik-drive-api/internal/util"
	"strings"

	"github.com/gin-gonic/gin"
)

type ChatController struct {
	ChatService *ChatService
}

func NewChatController(cs *ChatService) *ChatController {
	return &ChatController{ChatService: cs}
}

func (cc *ChatController) Chat(c *gin.Context) {
	question := c.PostForm("question")
	filename := c.PostForm("filename")
	audioFile, _ := c.FormFile("audio")
	communities := c.PostFormArray("communities")

	communities = util.ParseCommaSeparatedCommunities(communities)

	if filename == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "filename is required"})
		return
	}

	answer, err := cc.ChatService.Chat(question, audioFile, filename, communities)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"answer": answer})
}

func (cc *ChatController) TTS(c *gin.Context) {
	text := strings.TrimSpace(c.PostForm("text"))
	if text == "" {
		text = strings.TrimSpace(c.PostForm("answer"))
	}

	if text == "" {
		var body ttsJSON
		if err := c.ShouldBindJSON(&body); err == nil {
			if body.Text != "" {
				text = strings.TrimSpace(body.Text)
			} else {
				text = strings.TrimSpace(body.Answer)
			}
		}
	}

	if text == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "text is required"})
		return
	}

	audio, err := cc.ChatService.TTS(text)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Return binary audio so frontend can fetch -> Blob -> cache & replay
	c.Data(http.StatusOK, audio.MimeType, audio.Data)
}
