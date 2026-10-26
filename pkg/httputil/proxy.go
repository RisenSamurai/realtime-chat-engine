package httputil

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// ProxyJSONRequest forwards an incoming JSON payload to an upstream microservice,
// validates the HTTP status, and returns the parsed generic response.
func ProxyJSONRequest[Req any, Res any](c *gin.Context, method, serviceURL string, succMsg string) {
	var requestData Req
	if err := c.ShouldBindJSON(&requestData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request payload"})
		return
	}

	jsonData, err := json.Marshal(requestData)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to process request data"})
		return
	}

	// 1. Формируем запрос к микросервису
	req, err := http.NewRequest(method, serviceURL, bytes.NewBuffer(jsonData))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create microservice request"})
		return
	}
	req.Header.Set("Content-Type", "application/json")

	// 2. Выполняем запрос с таймаутом
	client := &http.Client{Timeout: time.Second * 10}
	resp, err := client.Do(req)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Microservice is unreachable: " + err.Error()})
		return
	}
	defer resp.Body.Close()

	// 3. Читаем и обрабатываем ответ
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Unable to read microservice response"})
		return
	}

	if resp.StatusCode != http.StatusOK {
		c.JSON(resp.StatusCode, gin.H{
			"error":   "Microservice rejected the request",
			"details": string(bodyBytes),
		})
		return
	}

	// 4. Парсим успешный ответ в дженерик-тип
	var serviceResponse Res
	if err := json.Unmarshal(bodyBytes, &serviceResponse); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse microservice response"})
		return
	}

	// 5. Отдаем результат
	c.JSON(http.StatusOK, gin.H{
		"message": succMsg,
		"data":    serviceResponse,
	})
}
