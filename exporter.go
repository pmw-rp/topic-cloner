package main

import (
	"context"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kadm"
	"regexp"
)

func getTopicRegex() string {
	result := ""
	patterns := config.Strings("source.topics")
	for _, pattern := range patterns {
		if result == "" {
			result = "^" + pattern + "$"
		} else {
			result = result + "|" + "^" + pattern + "$"
		}
	}
	return result
}

func ExportTopics(adm *kadm.Client) []byte {

	regex := getTopicRegex()
	log.Debugf("Using topic regex: %v", regex)
	pattern, err := regexp.Compile(regex)
	if err != nil {
		log.Fatalf("Unable to compile topic regex: %v", err)
	}

	ctx := context.Background()
	topicDetails, err := adm.ListTopicsWithInternal(ctx)
	if err != nil {
		log.Fatalf("Unable to list topics from source cluster: %v", err)
	}

	topicConfigs := make(map[string]TopicConf, 0)

	for s, detail := range topicDetails {
		if pattern.MatchString(s) {
			topicConfigs[s] = TopicConf{
				Name:       s,
				Partitions: int32(len(detail.Partitions)),
				Replicas:   int16(detail.Partitions.NumReplicas()),
			}
		}
	}

	topics := make([]string, len(topicConfigs))
	i := 0
	for k := range topicConfigs {
		topics[i] = k
		i++
	}

	resourceConfigs, err := adm.DescribeTopicConfigs(ctx, topics...)
	if err != nil {
		log.Fatalf("Unable to describe topic configs from source: %v", err)
	}

	for _, config := range resourceConfigs {
		topicConfig := topicConfigs[config.Name]

		if topicConfig.Configs == nil {
			topicConfig.Configs = make(map[string]*string, 10)
		}

		for _, c := range config.Configs {
			topicConfig.Configs[c.Key] = c.Value
		}

		topicConfigs[config.Name] = topicConfig
	}

	data, err := json.Marshal(topicConfigs)
	if err != nil {
		log.Fatalf("Unable to marshall topic configs into json: %v", err)
	}

	return data
}
