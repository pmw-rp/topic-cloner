package main

import (
	"context"
	"encoding/json"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func ImportTopics(data []byte) {

	topicConfigs := make(map[string]TopicConf, 0)

	err := json.Unmarshal(data, &topicConfigs)
	if err != nil {
		panic(err)
	}

	topics := make([]string, len(topicConfigs))
	i := 0
	for k := range topicConfigs {
		topics[i] = k
		i++
	}

	seeds := []string{"127.0.0.1:9092"}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
	)

	if err != nil {
		panic(err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	defer admin.Close()

	ctx := context.Background()
	topicsThatAlreadyExist, err := admin.ListTopicsWithInternal(ctx, topics...)

	for _, topic := range topics {
		if topicsThatAlreadyExist.Has(topic) {
			println("Ignoring topic since it exists")
		} else {
			conf := topicConfigs[topic]
			_, err := admin.CreateTopic(ctx, conf.Partitions /*conf.Replicas*/, 1, conf.Configs, conf.Name)
			if err != nil {
				panic(err)
			}
		}
	}
}
