package main

import (
	"context"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kadm"
)

func ImportTopics(admin *kadm.Client, data []byte) {

	topicConfigs := make(map[string]TopicConf, 0)

	err := json.Unmarshal(data, &topicConfigs)
	if err != nil {
		log.Fatalf("Unable to unmarshall topic data: %v", err)
	}

	topics := make([]string, len(topicConfigs))
	i := 0
	for k := range topicConfigs {
		topics[i] = k
		i++
	}

	ctx := context.Background()
	topicsThatAlreadyExist, err := admin.ListTopicsWithInternal(ctx, topics...)

	setRep := config.Exists("destination.replication_factor")
	rep := -1
	if setRep {
		rep = config.Int("destination.replication_factor")
		log.Warnf("Destination cluster has replication factor hardcoded in config to %v", rep)
	}

	for _, topic := range topics {
		if topicsThatAlreadyExist.Has(topic) {
			log.Infof("Not importing topic since it already exists on destination: %v", topic)
		} else {
			conf := topicConfigs[topic]
			if setRep {
				_, err = admin.CreateTopic(ctx, conf.Partitions, int16(rep), conf.Configs, conf.Name)
			} else {
				_, err = admin.CreateTopic(ctx, conf.Partitions, conf.Replicas, conf.Configs, conf.Name)
			}

			if err != nil {
				log.Fatalf("Unable to create topic on destination: %v", err)
			} else {
				log.Infof("Imported topic: %v", topic)
			}
		}
	}
}
