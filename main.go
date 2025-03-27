package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
	"github.com/segmentio/kafka-go"
	"gopkg.in/gomail.v2"
)

// Kafka configuration
const (
	kafkaBroker = "localhost:9092" // Change this to your Kafka broker
	topic = "mail"
	groupID     = "mail_consumer"
)

// SMTP configuration
const (
	smtpHost = "smtp.gmail.com" // Change to your SMTP server
	smtpPort = 587                // SMTP port (587 for TLS, 465 for SSL)
	smtpUser = "ktm.mail2025@gmail.com"
	// todo - think of how can we securely store the password
	smtpPass = ""
)

type Mail struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func consumeKafka() {
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	// Ideally this will never be executed as we want the consumtion to run forever. But in case of panic
	defer kafkaReader.Close()

	fmt.Println("Kafka consumer started...")
	for {

		// read next msg from kafka
		kafkaMsg, err := kafkaReader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			// No more msg. So just goto sleep and don't eat the CPU cycles
			time.Sleep(10 * time.Second)
			continue
		}

		log.Printf("Read msg from kafka: %s\n", string(kafkaMsg.Value))

		var mail Mail
		if err := json.Unmarshal(kafkaMsg.Value, &mail); err != nil {
			log.Printf("Error parsing JSON: %v", err)
			continue
		}

		sendMail(mail)
	}
}

func sendMail(mail Mail) {
	message := gomail.NewMessage()
	message.SetHeader("From", smtpUser)
	message.SetHeader("To", mail.To)
	message.SetHeader("Subject", mail.Subject)
	message.SetBody("text/plain", mail.Body)

	d := gomail.NewDialer(smtpHost, smtpPort, smtpUser, smtpPass)

	if err := d.DialAndSend(message); err != nil {
		log.Printf("Failed to send email: %v", err)
	} else {
		fmt.Println("Email sent successfully!")
	}
}

func main() {
	go consumeKafka()

	// Keep the main function running forever
	select {}
}
