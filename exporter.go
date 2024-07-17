package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"regexp"
)

func ExportTopics() []byte {

	seeds := []string{"seed-fa015309.certnoj7m575jtvbg730.fmc.prd.cloud.redpanda.com:9092"}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.DialTLSConfig(new(tls.Config)),
		kgo.SASL(scram.Auth{
			User: "pmw",
			Pass: "s6ldDzK3AUinQtQ5dejbiGhYtrDPjI",
		}.AsSha256Mechanism()))

	if err != nil {
		panic(err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	defer admin.Close()

	pattern, err := regexp.Compile("owlshop-.*")
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	topicDetails, err := admin.ListTopicsWithInternal(ctx)
	if err != nil {
		panic(err)
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

	resourceConfigs, err := admin.DescribeTopicConfigs(ctx, topics...)
	if err != nil {
		panic(err)
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
		panic(err)
	}

	return data
}
