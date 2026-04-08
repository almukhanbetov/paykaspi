package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

// Константы
const (
	MQTT_BROKER  = "tcp://127.0.0.1:45001"
	MQTT_USER    = "vds_admin"
	MQTT_PASS    = "Asetaset_97"
	HTTP_TIMEOUT = 12 * time.Second
)

type Config struct {
	MysqlDSN    string
	MqttBroker  string
	MqttUser    string
	MqttPass    string
	HttpTimeout time.Duration
}

func LoadConfig() *Config {
	cfg := &Config{
		MysqlDSN:    os.Getenv("MYSQL_DSN"),
		MqttBroker:  os.Getenv("MQTT_BROKER"),
		MqttUser:    os.Getenv("MQTT_USER"),
		MqttPass:    os.Getenv("MQTT_PASS"),
		HttpTimeout: 12 * time.Second,
	}

	// проверки
	if cfg.MysqlDSN == "" {
		log.Fatal("MYSQL_DSN not set")
	}
	if cfg.MqttBroker == "" {
		log.Fatal("MQTT_BROKER not set")
	}

	return cfg
}

var (
	db  *sql.DB
	mq  mqtt.Client
	mux sync.RWMutex

	// Мапы для хранения ожидающих ответов
	checkResponses = make(map[string]chan map[string]interface{})
	payResponses   = make(map[string]chan map[string]interface{})
)

// Структуры для запросов и ответов
type CheckRequest struct {
	Command string `json:"command"`
	TxnID   string `json:"txn_id"`
	Account string `json:"account"`
}

type CheckResponse struct {
	TxnID   string   `json:"txn_id"`
	Result  int      `json:"result"`
	Comment string   `json:"comment,omitempty"`
	Account string   `json:"account,omitempty"`
	BIN     string   `json:"bin,omitempty"`
	Tariffs []Tariff `json:"tariffs,omitempty"`
}

// Изменено: Account теперь int64, удалены ID и TxnDate
type PayRequest struct {
	Command string  `json:"command"`
	TxnID   string  `json:"txn_id"`
	Account int64   `json:"account"` // Изменено с string на int64
	Sum     float64 `json:"sum"`
	// ID и TxnDate удалены, так как они не отправляются в MQTT
}

type PayResponse struct {
	TxnID   string `json:"txn_id"`
	PrvTxn  int64  `json:"prv_txn,omitempty"`
	Result  int    `json:"result"`
	BIN     string `json:"bin,omitempty"`
	Comment string `json:"comment,omitempty"`
}

type Tariff struct {
	ID   string  `json:"id"`
	Name string  `json:"name"`
	Sum  float64 `json:"sum"`
}

func initDB(cfg *Config) error {
	var err error

	maxRetries := 10
	delay := 3 * time.Second

	for i := 1; i <= maxRetries; i++ {
		log.Printf("Connecting to DB (attempt %d/%d)...", i, maxRetries)

		db, err = sql.Open("mysql", cfg.MysqlDSN)
		if err != nil {
			log.Println("DB open error:", err)
		} else {
			err = db.Ping()
			if err == nil {
				log.Println("Connected to DB successfully")
				break
			}
			log.Println("DB ping error:", err)
		}

		if i < maxRetries {
			log.Printf("Retrying in %v...\n", delay)
			time.Sleep(delay)
		}
	}

	if err != nil {
		return fmt.Errorf("could not connect to DB after %d attempts: %w", maxRetries, err)
	}

	// создаём таблицу
	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS payments (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		txn_id VARCHAR(64) UNIQUE,
		account VARCHAR(64),
		sum DECIMAL(10,2),
		result INT,
		comment TEXT,
		created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		INDEX idx_txn_id (txn_id),
		INDEX idx_account (account)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`)
	return err
}

func initMQTT() error {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(MQTT_BROKER)
	opts.SetUsername(MQTT_USER)
	opts.SetPassword(MQTT_PASS)
	opts.SetClientID(fmt.Sprintf("payment_app_%d", time.Now().Unix()))

	// Важно: Используем MQTT v3.1.1 для совместимости с библиотекой v1.5.1
	// opts.SetProtocolVersion(4) // 4 = MQTT v3.1.1

	opts.SetCleanSession(false) // хранить сессию и сообщения
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)

	// Настройка keepalive для предотвращения отключений
	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(10 * time.Second)

	mq = mqtt.NewClient(opts)
	if token := mq.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	// Подписываемся на топики ответов с QoS 1
	if token := mq.Subscribe("payment/check", 1, handleCheckResponse); token.Wait() && token.Error() != nil {
		log.Printf("Error subscribing to payment/check: %v", token.Error())
	}

	if token := mq.Subscribe("payment/pay", 1, handlePayResponse); token.Wait() && token.Error() != nil {
		log.Printf("Error subscribing to payment/pay: %v", token.Error())
	}

	log.Println("MQTT connected and subscribed")
	return nil
}

// Обработчики MQTT сообщений
func handleCheckResponse(client mqtt.Client, msg mqtt.Message) {
	log.Printf("Received message on topic %s: %s", msg.Topic(), string(msg.Payload()))

	var response map[string]interface{}
	if err := json.Unmarshal(msg.Payload(), &response); err != nil {
		log.Println("JSON unmarshal error:", err)
		return
	}

	txnID, ok := response["txn_id"].(string)
	if !ok {
		log.Println("No txn_id in response")
		return
	}

	log.Printf("Processing check response for txn_id: %s", txnID)

	mux.RLock()
	ch, exists := checkResponses[txnID]
	mux.RUnlock()

	if exists {
		select {
		case ch <- response:
			log.Printf("Check response sent to channel for txn_id: %s", txnID)
		default:
			log.Printf("Channel blocked for txn_id: %s", txnID)
		}
	} else {
		log.Printf("No waiting channel for check txn_id: %s", txnID)
	}
}

func handlePayResponse(client mqtt.Client, msg mqtt.Message) {
	log.Printf("Received message on topic %s: %s", msg.Topic(), string(msg.Payload()))

	var response map[string]interface{}
	if err := json.Unmarshal(msg.Payload(), &response); err != nil {
		log.Println("JSON unmarshal error:", err)
		return
	}

	txnID, ok := response["txn_id"].(string)
	if !ok {
		log.Println("No txn_id in response")
		return
	}

	log.Printf("Processing pay response for txn_id: %s", txnID)

	mux.RLock()
	ch, exists := payResponses[txnID]
	mux.RUnlock()

	if exists {
		select {
		case ch <- response:
			log.Printf("Pay response sent to channel for txn_id: %s", txnID)
		default:
			log.Printf("Channel blocked for txn_id: %s", txnID)
		}
	} else {
		log.Printf("No waiting channel for pay txn_id: %s", txnID)
	}
}

// Функция получения тарифов
func getTariffsByAccount(account string) ([]map[string]interface{}, error) {
	rows, err := db.Query(`
		SELECT t.tariff_id, t.name, t.sum
		FROM tariffs t
		JOIN devices d ON d.type = t.type
		WHERE d.account = ?
		ORDER BY t.sum ASC
	`, account)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tariffs []map[string]interface{}

	for rows.Next() {
		var id, name string
		var sum float64

		if err := rows.Scan(&id, &name, &sum); err != nil {
			return nil, err
		}

		tariffs = append(tariffs, map[string]interface{}{
			"id":   id,
			"name": name,
			"sum":  sum,
		})
	}
	return tariffs, nil
}

// Проверка существования account в devices
func checkAccountExists(account string) (bool, string, error) {
	var count int
	var bin string

	// Проверяем существование account
	err := db.QueryRow("SELECT COUNT(*), COALESCE(bin, '') FROM devices WHERE account = ?", account).Scan(&count, &bin)
	if err != nil {
		return false, "", err
	}

	return count > 0, bin, nil
}

// Проверка существования txn_id в payments
func checkTxnIDExists(txnID string) (bool, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM payments WHERE txn_id = ?", txnID).Scan(&count)
	return count > 0, err
}

// Сохранение платежа в БД
func savePayment(txnID, account string, sum float64, result int, comment string) (int64, error) {
	res, err := db.Exec(
		"INSERT INTO payments (txn_id, account, sum, result, comment) VALUES (?, ?, ?, ?, ?)",
		txnID, account, sum, result, comment,
	)
	if err != nil {
		return 0, err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}

	return id, nil
}

// HTTP обработчик
func paymentHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	command := r.URL.Query().Get("command")
	txnID := r.URL.Query().Get("txn_id")
	account := r.URL.Query().Get("account")

	log.Printf("Received request: command=%s, txn_id=%s, account=%s", command, txnID, account)

	if command == "" || txnID == "" || account == "" {
		http.Error(w, `{"error":"Missing parameters"}`, http.StatusBadRequest)
		return
	}

	switch command {
	case "check":
		handleCheckCommand(w, r, txnID, account)
	case "pay":
		handlePayCommand(w, r, txnID, account)
	default:
		http.Error(w, `{"error":"Unknown command"}`, http.StatusBadRequest)
	}
}

func handleCheckCommand(w http.ResponseWriter, r *http.Request, txnID, account string) {
	// Проверяем account в БД
	exists, bin, err := checkAccountExists(account)
	if err != nil {
		log.Printf("Error checking account: %v", err)
		response := CheckResponse{
			TxnID:   txnID,
			Result:  1,
			Comment: "Ошибка базы данных",
		}
		json.NewEncoder(w).Encode(response)
		return
	}

	if !exists {
		log.Printf("Account not found: %s", account)
		response := CheckResponse{
			TxnID:   txnID,
			Result:  1,
			Comment: "Аккаунт не найден",
		}
		json.NewEncoder(w).Encode(response)
		return
	}

	// Создаем канал для ответа от ESP
	responseChan := make(chan map[string]interface{}, 1)

	mux.Lock()
	checkResponses[txnID] = responseChan
	mux.Unlock()

	// Очищаем канал и удаляем из мапы при выходе
	defer func() {
		mux.Lock()
		delete(checkResponses, txnID)
		mux.Unlock()
		close(responseChan)
		log.Printf("Cleaned up check channel for txn_id: %s", txnID)
	}()

	// Отправляем запрос в MQTT
	request := CheckRequest{
		Command: "check",
		TxnID:   txnID,
		Account: account,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		log.Printf("Error marshaling check request: %v", err)
		response := CheckResponse{
			TxnID:   txnID,
			Result:  1,
			Comment: "Ошибка формирования запроса",
		}
		json.NewEncoder(w).Encode(response)
		return
	}

	log.Printf("Publishing check request to payment/app for txn_id: %s", txnID)

	// ИСПРАВЛЕНО: Используем стандартный Publish вместо PublishWithProperties
	token := mq.Publish("payment/app", 1, false, jsonData)
	if !token.WaitTimeout(5 * time.Second) {
		log.Printf("Timeout publishing to MQTT for txn_id: %s", txnID)
	}

	// Ждем ответа от ESP
	select {
	case espResponse := <-responseChan:
		log.Printf("Received ESP response for check txn_id: %s", txnID)

		// Проверяем result от ESP
		resultVal, ok := espResponse["result"]
		if !ok {
			log.Printf("No result in ESP response for txn_id: %s", txnID)
			response := CheckResponse{
				TxnID:   txnID,
				Result:  1,
				Comment: "Неверный ответ от устройства",
			}
			json.NewEncoder(w).Encode(response)
			return
		}

		// Преобразуем result в int
		var result int
		switch v := resultVal.(type) {
		case float64:
			result = int(v)
		case int:
			result = v
		default:
			result = 1
		}

		if result == 0 {
			// Получаем тарифы
			tariffs, err := getTariffsByAccount(account)
			if err != nil {
				log.Printf("Error getting tariffs: %v", err)
				response := CheckResponse{
					TxnID:   txnID,
					Result:  1,
					Comment: "Ошибка получения тарифов",
				}
				json.NewEncoder(w).Encode(response)
				return
			}

			// Формируем финальный ответ
			var tariffList []Tariff
			for _, t := range tariffs {
				id, _ := t["id"].(string)
				name, _ := t["name"].(string)
				sum, _ := t["sum"].(float64)

				tariffList = append(tariffList, Tariff{
					ID:   id,
					Name: name,
					Sum:  sum,
				})
			}

			response := CheckResponse{
				TxnID:   txnID,
				Result:  0,
				Comment: "Найдено в системе",
				Account: account,
				BIN:     bin,
				Tariffs: tariffList,
			}
			log.Printf("Sending successful check response for txn_id: %s", txnID)
			json.NewEncoder(w).Encode(response)
		} else {
			response := CheckResponse{
				TxnID:   txnID,
				Result:  1,
				Comment: "Ошибка проверки устройства",
			}
			json.NewEncoder(w).Encode(response)
		}

	case <-time.After(HTTP_TIMEOUT):
		log.Printf("Timeout waiting for ESP check response for txn_id: %s", txnID)
		response := CheckResponse{
			TxnID:   txnID,
			Result:  1,
			Comment: "Таймаут ожидания ответа от устройства",
		}
		json.NewEncoder(w).Encode(response)
	}
}

func handlePayCommand(w http.ResponseWriter, r *http.Request, txnID, account string) {
	// Проверяем дублирование txn_id
	exists, err := checkTxnIDExists(txnID)
	if err != nil {
		log.Printf("Error checking txn_id: %v", err)
		response := PayResponse{
			TxnID:   txnID,
			Result:  1,
			Comment: "Ошибка базы данных",
		}
		json.NewEncoder(w).Encode(response)
		return
	}

	if exists {
		log.Printf("Duplicate txn_id: %s", txnID)
		response := map[string]interface{}{
			"txn_id":  txnID,
			"result":  3,
			"comment": "txn_id повтор",
		}
		json.NewEncoder(w).Encode(response)
		return
	}

	// Получаем остальные параметры
	sumStr := r.URL.Query().Get("sum")

	// Проверяем account в БД
	accountExists, bin, err := checkAccountExists(account)
	if err != nil {
		log.Printf("Error checking account: %v", err)
		response := PayResponse{
			TxnID:   txnID,
			Result:  1,
			Comment: "Ошибка базы данных",
		}
		json.NewEncoder(w).Encode(response)
		return
	}

	if !accountExists {
		log.Printf("Account not found for payment: %s", account)
		response := PayResponse{
			TxnID:   txnID,
			Result:  1,
			Comment: "Аккаунт не найден",
		}
		json.NewEncoder(w).Encode(response)
		return
	}

	// Парсим сумму
	var sum float64
	if sumStr != "" {
		if s, err := strconv.ParseFloat(sumStr, 64); err == nil {
			sum = s
		} else {
			log.Printf("Error parsing sum: %v", err)
			response := PayResponse{
				TxnID:   txnID,
				Result:  1,
				Comment: "Неверная сумма",
			}
			json.NewEncoder(w).Encode(response)
			return
		}
	}

	// Создаем канал для ответа от ESP
	responseChan := make(chan map[string]interface{}, 1)

	mux.Lock()
	payResponses[txnID] = responseChan
	mux.Unlock()

	// Очищаем канал и удаляем из мапы при выходе
	defer func() {
		mux.Lock()
		delete(payResponses, txnID)
		mux.Unlock()
		close(responseChan)
		log.Printf("Cleaned up pay channel for txn_id: %s", txnID)
	}()

	// ИСПРАВЛЕНО: Отправляем запрос в MQTT только с нужными полями
	accountInt, _ := strconv.ParseInt(account, 10, 64)
	request := PayRequest{
		Command: "pay",
		TxnID:   txnID,
		Account: accountInt,
		Sum:     sum,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		log.Printf("Error marshaling pay request: %v", err)
		response := PayResponse{
			TxnID:   txnID,
			Result:  1,
			Comment: "Ошибка формирования запроса",
		}
		json.NewEncoder(w).Encode(response)
		return
	}

	log.Printf("Publishing pay request to payment/app for txn_id: %s", txnID)
	log.Printf("JSON payload: %s", string(jsonData))

	// ИСПРАВЛЕНО: Используем стандартный Publish вместо PublishWithProperties
	token := mq.Publish("payment/app", 1, false, jsonData)
	if !token.WaitTimeout(5 * time.Second) {
		log.Printf("Timeout publishing to MQTT for txn_id: %s", txnID)
	}

	// Ждем ответа от ESP
	select {
	case espResponse := <-responseChan:
		log.Printf("Received ESP response for pay txn_id: %s", txnID)

		// Проверяем result от ESP
		resultVal, ok := espResponse["result"]
		if !ok {
			log.Printf("No result in ESP response for txn_id: %s", txnID)

			// Сохраняем неудачный платеж
			prvTxn, err := savePayment(txnID, account, sum, 1, "Неверный ответ от устройства")
			if err != nil {
				log.Printf("Error saving payment: %v", err)
				response := PayResponse{
					TxnID:   txnID,
					Result:  1,
					Comment: "Ошибка сохранения платежа",
				}
				json.NewEncoder(w).Encode(response)
				return
			}

			response := PayResponse{
				TxnID:   txnID,
				PrvTxn:  prvTxn,
				Result:  1,
				Comment: "Неверный ответ от устройства",
			}
			json.NewEncoder(w).Encode(response)
			return
		}

		// Преобразуем result в int
		var result int
		switch v := resultVal.(type) {
		case float64:
			result = int(v)
		case int:
			result = v
		default:
			result = 1
		}

		if result == 0 {
			// Сохраняем успешный платеж в БД
			prvTxn, err := savePayment(txnID, account, sum, 0, "OK")
			if err != nil {
				log.Printf("Error saving payment: %v", err)
				response := PayResponse{
					TxnID:   txnID,
					Result:  1,
					Comment: "Ошибка сохранения платежа",
				}
				json.NewEncoder(w).Encode(response)
				return
			}

			response := PayResponse{
				TxnID:   txnID,
				PrvTxn:  prvTxn,
				Result:  0,
				BIN:     bin,
				Comment: "OK",
			}
			log.Printf("Payment successful for txn_id: %s, prv_txn: %d", txnID, prvTxn)
			json.NewEncoder(w).Encode(response)
		} else {
			// Сохраняем неудачный платеж
			comment := "Ошибка оплаты"
			if msg, ok := espResponse["comment"].(string); ok && msg != "" {
				comment = msg
			}

			prvTxn, err := savePayment(txnID, account, sum, result, comment)
			if err != nil {
				log.Printf("Error saving payment: %v", err)
				response := PayResponse{
					TxnID:   txnID,
					Result:  1,
					Comment: "Ошибка сохранения платежа",
				}
				json.NewEncoder(w).Encode(response)
				return
			}

			response := PayResponse{
				TxnID:   txnID,
				PrvTxn:  prvTxn,
				Result:  result,
				Comment: comment,
			}
			json.NewEncoder(w).Encode(response)
		}

	case <-time.After(HTTP_TIMEOUT):
		log.Printf("Timeout waiting for ESP pay response for txn_id: %s", txnID)

		// НЕ сохраняем платеж в БД при таймауте от ESP
		// Просто возвращаем ошибку
		response := PayResponse{
			TxnID:   txnID,
			Result:  1,
			Comment: "Таймаут ожидания ответа от устройства",
		}
		json.NewEncoder(w).Encode(response)
	}
}

func main() {
	// Настройка логгера
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	godotenv.Load()
	cfg := LoadConfig()
	// Инициализация БД
	log.Println("Initializing database...")
	if err := initDB(cfg); err != nil {
		log.Fatal("DB init error:", err)
	}
	defer db.Close()
	log.Println("Database initialized successfully")

	// Инициализация MQTT
	log.Println("Initializing MQTT...")
	if err := initMQTT(); err != nil {
		log.Fatal("MQTT init error:", err)
	}

	// Тестовое сообщение
	token := mq.Publish("payment/app", 1, false, "hello smart24.kz")
	if token.Wait() && token.Error() != nil {
		log.Printf("Test publish error: %v", token.Error())
	}

	defer mq.Disconnect(250)
	log.Println("MQTT initialized successfully")

	// Настройка HTTP сервера
	http.HandleFunc("/payment_app.cgi", paymentHandler)

	log.Println("Server starting on :8180")
	if err := http.ListenAndServe(":8180", nil); err != nil {
		log.Fatal("HTTP server error:", err)
	}
}
