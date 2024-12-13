package main

import (
	"context"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kadm"
)

func updateTopic(ctx context.Context, admin *kadm.Client, new, old TopicConf) []error {
	errors := make([]error, 0)

	if old.Replicas != new.Replicas {
		errors = append(errors, fmt.Errorf("not able to change replica count to %v on existing topic %v", new.Replicas, new.Name))
	}

	if new.Partitions > old.Partitions {
		log.Infof("increasing partition count for topic %v (%v -> %v)", new.Name, old.Partitions, new.Partitions)
		_, err := admin.UpdatePartitions(ctx, int(new.Partitions), new.Name)
		if err != nil {
			errors = append(errors, fmt.Errorf("unable to increase partitions to %v on topic %v: %w", new.Partitions, new.Name, err))
		}
	} else {
		if new.Partitions < old.Partitions {
			errors = append(errors, fmt.Errorf("unable to reduce partitions to %v on topic %v", new.Partitions, new.Name))
		}
	}

	alterConfigs := make([]kadm.AlterConfig, 0)
	for key, newValue := range new.Configs {
		if oldValue, ok := old.Configs[key]; ok {
			if newValue != oldValue {
				alterConfigs = append(alterConfigs, kadm.AlterConfig{Name: key, Value: newValue})
			}
		}
	}
	if len(alterConfigs) > 0 {
		log.Infof("updating topic %v with the following configs", new.Name)
		for _, alterConfig := range alterConfigs {
			log.Infof("   %v = %v", alterConfig.Name, alterConfig.Value)
		}
		_, err := admin.ValidateAlterTopicConfigs(ctx, alterConfigs, new.Name)
		if err != nil {
			errors = append(errors, fmt.Errorf("unable to validate alter topic configs due to %w", err))
		} else {
			_, err := admin.AlterTopicConfigs(ctx, alterConfigs, new.Name)
			if err != nil {
				errors = append(errors, fmt.Errorf("unable to alter topic configs due to %w", err))
			}
		}
	}

	return errors
}

func loadTopics(data []byte) map[string]TopicConf {
	topics := make(map[string]TopicConf)
	err := json.Unmarshal(data, &topics)
	if err != nil {
		panic(fmt.Errorf("unable to unmarshal json topic data %w", err))
	}
	return topics
}

func ImportTopics(admin *kadm.Client, data []byte) {

	topicsToBeImported := loadTopics(data)
	topicsThatAlreadyExist := loadTopics(ExportTopics(admin))

	ctx := context.Background()

	setRep := config.Exists("destination.replication_factor")
	if setRep {
		log.Warnf("destination cluster has replication factor hardcoded in config to %v", config.Int("destination.replication_factor"))
	}

	updateExistingTopics := config.Bool("destination.update_existing_topics")

	for _, topicToBeImported := range topicsToBeImported {
		if topicThatAlreadyExists, ok := topicsThatAlreadyExist[topicToBeImported.Name]; ok {
			if updateExistingTopics {
				log.Infof("updating topic since it already exists on destination: %v", topicToBeImported)
				errors := updateTopic(ctx, admin, topicToBeImported, topicThatAlreadyExists)
				for _, err := range errors {
					log.Warnf(err.Error())
				}
			} else {
				log.Infof("not importing topic since it already exists on destination: %v", topicToBeImported)
			}
		} else {
			conf := topicsToBeImported[topicToBeImported.Name]
			var replication int16
			if setRep {
				replication = int16(config.Int("destination.replication_factor"))
			} else {
				replication = conf.Replicas
			}

			_, err := admin.CreateTopic(ctx, conf.Partitions, replication, conf.Configs, conf.Name)

			if err != nil {
				log.Fatalf("unable to create topic %v on destination: %v", conf.Name, err)
			} else {
				log.Infof("imported topic: %v", conf.Name)
			}
		}
	}
}
