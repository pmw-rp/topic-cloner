package main

import (
	"crypto/tls"
	"strconv"
	"strings"
	"sync"

	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/tlscfg"

	log "github.com/sirupsen/logrus"
)

var (
	lock   = &sync.Mutex{}
	config = koanf.New(".")
)

type SASLConfig struct {
	SaslMethod   string `koanf:"sasl_method"`
	SaslUsername string `koanf:"sasl_username"`
	SaslPassword string `koanf:"sasl_password"`
}

type TLSConfig struct {
	Enabled        bool   `koanf:"enabled"`
	ClientKeyFile  string `koanf:"client_key"`
	ClientCertFile string `koanf:"client_cert"`
	CaFile         string `koanf:"ca_cert"`
}

// InitConfig Initialize the agent configuration from the provided .yaml file
func InitConfig(path *string) {
	lock.Lock()
	defer lock.Unlock()

	//config.Load(defaultConfig, nil)
	log.Infof("Reading config file: %s", *path)
	if err := config.Load(file.Provider(*path), yaml.Parser()); err != nil {
		log.Errorf("Error loading config: %v", err)
	}
	validate()
	log.Debugf(config.Sprint())
}

// Validate the config
func validate() {
	if !config.Exists("source.bootstrap_servers") && !config.Exists("source.file") {
		panic("Config must define a file or a cluster as a source")
	}
	if config.Exists("source.bootstrap_servers") && config.Exists("source.file") {
		panic("Config must define a single source - file or cluster")
	}
	if !config.Exists("destination.bootstrap_servers") && !config.Exists("destination.file") {
		panic("Config must define a file or a cluster as a destination")
	}
	if config.Exists("destination.bootstrap_servers") && config.Exists("destination.file") {
		panic("Config must define a single destination - file or cluster")
	}
}

// TLSOpt Initializes the necessary TLS configuration options
func TLSOpt(tlsConfig *TLSConfig, opts []kgo.Opt) []kgo.Opt {
	if tlsConfig.Enabled {
		if tlsConfig.CaFile != "" ||
			tlsConfig.ClientCertFile != "" ||
			tlsConfig.ClientKeyFile != "" {
			tc, err := tlscfg.New(
				tlscfg.MaybeWithDiskCA(
					tlsConfig.CaFile, tlscfg.ForClient),
				tlscfg.MaybeWithDiskKeyPair(
					tlsConfig.ClientCertFile, tlsConfig.ClientKeyFile),
			)
			if err != nil {
				log.Fatalf("Unable to create TLS config: %v", err)
			}
			opts = append(opts, kgo.DialTLSConfig(tc))
		} else {
			opts = append(opts, kgo.DialTLSConfig(new(tls.Config)))
		}
	}
	return opts
}

// SASLOpt Initializes the necessary SASL configuration options
func SASLOpt(config *SASLConfig, opts []kgo.Opt) []kgo.Opt {
	if config.SaslMethod != "" ||
		config.SaslUsername != "" ||
		config.SaslPassword != "" {

		if config.SaslMethod == "" ||
			config.SaslUsername == "" ||
			config.SaslPassword == "" {
			log.Fatalln("All of SaslMethod, SaslUsername, SaslPassword " +
				"must be specified if any are")
		}
		method := strings.ToLower(config.SaslMethod)
		method = strings.ReplaceAll(method, "-", "")
		method = strings.ReplaceAll(method, "_", "")
		switch method {
		case "plain":
			opts = append(opts, kgo.SASL(plain.Auth{
				User: config.SaslUsername,
				Pass: config.SaslPassword,
			}.AsMechanism()))
		case "scramsha256":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: config.SaslUsername,
				Pass: config.SaslPassword,
			}.AsSha256Mechanism()))
		case "scramsha512":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: config.SaslUsername,
				Pass: config.SaslPassword,
			}.AsSha512Mechanism()))
		case "awsmskiam":
			opts = append(opts, kgo.SASL(aws.Auth{
				AccessKey: config.SaslUsername,
				SecretKey: config.SaslPassword,
			}.AsManagedStreamingIAMMechanism()))
		default:
			log.Fatalf("Unrecognized sasl method: %s", method)
		}
	}
	return opts
}

// MaxVersionOpt Set the maximum Kafka protocol version to try
func MaxVersionOpt(version string, opts []kgo.Opt) []kgo.Opt {
	ver := strings.ToLower(version)
	ver = strings.ReplaceAll(ver, "v", "")
	ver = strings.ReplaceAll(ver, ".", "")
	ver = strings.ReplaceAll(ver, "_", "")
	verNum, _ := strconv.Atoi(ver)
	switch verNum {
	case 330:
		opts = append(opts, kgo.MaxVersions(kversion.V3_3_0()))
	case 320:
		opts = append(opts, kgo.MaxVersions(kversion.V3_2_0()))
	case 310:
		opts = append(opts, kgo.MaxVersions(kversion.V3_1_0()))
	case 300:
		opts = append(opts, kgo.MaxVersions(kversion.V3_0_0()))
	case 280:
		opts = append(opts, kgo.MaxVersions(kversion.V2_8_0()))
	case 270:
		opts = append(opts, kgo.MaxVersions(kversion.V2_7_0()))
	case 260:
		opts = append(opts, kgo.MaxVersions(kversion.V2_6_0()))
	default:
		opts = append(opts, kgo.MaxVersions(kversion.Stable()))
	}
	return opts
}
