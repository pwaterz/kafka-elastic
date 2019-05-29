package main

import (
	"strings"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

var (
	log        = logrus.New()
	logMain    = log.WithField("component", "main")
	logKafka   = log.WithField("component", "kafka")
	logElastic = log.WithField("component", "elastic")
)

// LogFormatter is an extension of the default log formatter for logrus, which strips trailing carriage returns from log messages.
type LogFormatter struct {
	parentFormatter logrus.TextFormatter
}

// Format formats log messages, stripping trailing carriage returns.
func (logFormatter *LogFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	entry.Message = strings.TrimSpace(entry.Message)
	return logFormatter.parentFormatter.Format(entry)
}

func init() {
	log.Formatter = &LogFormatter{}
	sarama.Logger = logKafka
}
