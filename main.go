package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"gopkg.in/gomail.v2"
)

// Kafka configuration
const (
	kafkaBroker = "localhost:9092" // Change this to your Kafka broker
	topic       = "mail_topic"     // Kafka topic name
	groupID     = "mail_consumer_group"
)

// SMTP configuration
const (
	smtpHost = "smtp.gmail.com" // Change to your SMTP server
	smtpPort = 587                // SMTP port (587 for TLS, 465 for SSL)
	smtpUser = "ktm.mail2025@gmail.com"
	smtpPass = ""
)

// Kafka consumer function
func consumeKafka() {
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	fmt.Println("Kafka consumer started...")
	for {
		message, err := kafkaReader.ReadMessage(context.Background())
		log.Printf("Tried reading from Kafka")
		if err != nil {
			log.Printf("Error reading message: %v", err)
			// No more msg. So just goto sleep and don't eat the CPU cycles
			time.Sleep(2 * time.Second)
			continue
		}

		fmt.Printf("Received message: %s\n", string(message.Value))

		// Send email with message content
		sendEmail("ktm.mail2025@gmail.com", "Kafka Email Notification", string(message.Value))
	}
}

// Function to send an email
func sendEmail(to, subject, body string) {
	message := gomail.NewMessage()
	message.SetHeader("From", smtpUser)
	message.SetHeader("To", to)
	message.SetHeader("Subject", subject)
	message.SetBody("text/plain", body)

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
