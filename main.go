package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"io"
	"os"
	"strings"
)

// Creates a new admin client to communicate with a cluster.
//
// The `prefix` must be set to either `source` or `destination` as it
// determines what settings are read from the configuration.
func initClient(prefix string) *kadm.Client {

	var err error
	servers := config.String(
		fmt.Sprintf("%s.bootstrap_servers", prefix))

	var opts []kgo.Opt
	opts = append(opts,
		kgo.SeedBrokers(strings.Split(servers, ",")...),
	)

	tlsPath := fmt.Sprintf("%s.tls", prefix)
	if config.Exists(tlsPath) {
		tlsConfig := TLSConfig{}
		err = config.Unmarshal(tlsPath, &tlsConfig)
		if err != nil {
			log.Fatalf("Unable to unmarshall TLS config: %v", err)
		}
		opts = TLSOpt(&tlsConfig, opts)
	}
	saslPath := fmt.Sprintf("%s.sasl", prefix)
	if config.Exists(saslPath) {
		saslConfig := SASLConfig{}
		err = config.Unmarshal(saslPath, &saslConfig)
		if err != nil {
			log.Fatalf("Unable to unmarshall SASL config: %v", err)
		}
		opts = SASLOpt(&saslConfig, opts)
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("Unable to load client: %v", err)
	}
	// Check connectivity to cluster
	if err = client.Ping(context.Background()); err != nil {
		log.Fatalf("Unable to ping %s cluster: %s",
			prefix, err.Error())
	}

	adm := kadm.NewClient(client)
	brokers, err := adm.ListBrokers(context.Background())
	if err != nil {
		log.Fatalf("Unable to list brokers: %v", err)
	}
	log.Infof("Created %s client", prefix)
	for _, broker := range brokers {
		brokerJson, _ := json.Marshal(broker)
		log.Debugf("%s broker: %s", prefix, string(brokerJson))
	}

	return adm
}

// Reads a file into memory in a byte slice
func read(name string) []byte {
	log.Infof("Reading topic data from file: %v", name)
	file, err := os.Open(name)
	if err != nil {
		log.Fatalf("Unable to read file %v: %v", name, err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Errorf("Unable to close file")
		}
	}(file)

	// Get the file size
	stat, err := file.Stat()
	if err != nil {
		log.Fatalf("Unable to get the size of file %v: %v", name, err)
	}

	// Read the file into a byte slice
	bs := make([]byte, stat.Size())
	_, err = bufio.NewReader(file).Read(bs)
	if err != nil && err != io.EOF {
		log.Fatalf("Unable to create reader for file %v: %v", name, err)
	}
	return bs
}

// Writes a byte slice to a file
func write(data []byte, name string) {
	err := os.WriteFile(name, data, os.FileMode(0600))
	if err != nil {
		log.Fatalf("Unable to write topic data to file %v: %v", name, err)
	} else {
		log.Infof("Wrote topic data to file: %v", name)
	}
}

func main() {
	configFile := flag.String(
		"config", "config.yaml", "path to cloner config file")
	logLevelStr := flag.String(
		"loglevel", "info", "logging level")
	flag.Parse()

	logLevel, _ := log.ParseLevel(*logLevelStr)
	log.SetLevel(logLevel)

	InitConfig(configFile)

	var topics []byte
	if config.Exists("source.file") {
		topics = read(config.String("source.file"))
	} else {
		source := initClient("source")
		defer source.Close()
		topics = ExportTopics(source)
	}

	if config.Exists("destination.file") {
		write(topics, config.String("destination.file"))
	} else {
		destination := initClient("destination")
		defer destination.Close()
		ImportTopics(destination, topics)
	}
}
