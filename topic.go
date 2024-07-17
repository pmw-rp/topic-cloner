package main

type TopicConf struct {
	Name       string
	Partitions int32
	Replicas   int16
	Configs    map[string]*string
}
