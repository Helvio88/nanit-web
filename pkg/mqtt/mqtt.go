package mqtt

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/indiefan/home_assistant_nanit/pkg/utils"
	"github.com/rs/zerolog/log"
)

type SendLightCommandHandler func(nightLightState bool)
type SendStandbyCommandHandler func(standbyState bool)

// Connection - MQTT context
type Connection struct {
	Opts                      Opts
	StateManager              *baby.StateManager
	client                    MQTT.Client
	sendLightCommandHandler   SendLightCommandHandler
	sendStandbyCommandHandler SendStandbyCommandHandler
	publishedDiscoveries      map[string]bool
}

// NewConnection - constructor
func NewConnection(opts Opts) *Connection {
	return &Connection{
		Opts: opts,
		publishedDiscoveries: make(map[string]bool),
	}
}

// Run - runs the mqtt connection handler
func (conn *Connection) Run(manager *baby.StateManager, ctx utils.GracefulContext) {
	conn.StateManager = manager

	opts := MQTT.NewClientOptions()
	opts.AddBroker(conn.Opts.BrokerURL)
	opts.SetClientID(conn.Opts.TopicPrefix)
	opts.SetUsername(conn.Opts.Username)
	opts.SetPassword(conn.Opts.Password)
	opts.SetCleanSession(false)

	conn.client = MQTT.NewClient(opts)

	utils.RunWithPerseverance(func(attempt utils.AttemptContext) {
		runMqtt(conn, attempt)
	}, ctx, utils.PerseverenceOpts{
		RunnerID:       "mqtt",
		ResetThreshold: 2 * time.Second,
		Cooldown: []time.Duration{
			2 * time.Second,
			10 * time.Second,
			1 * time.Minute,
		},
	})
}

func (conn *Connection) RegisterLightHandler(sendLightCommandHandler SendLightCommandHandler) {
	conn.sendLightCommandHandler = sendLightCommandHandler
}

func (conn *Connection) subscribeToLightCommand() {
	commandTopic := fmt.Sprintf("%v/babies/+/night_light/switch", conn.Opts.TopicPrefix)
	log.Debug().
		Str("topic", commandTopic).
		Msg("Subscribing to command topic")

	lightMessageHandler := func(mqttConn MQTT.Client, msg MQTT.Message) {
		// Extract baby UID and command from topic
		parts := strings.Split(msg.Topic(), "/")
		if len(parts) < 4 {
			log.Error().Str("topic", msg.Topic()).Msg("Invalid command topic format")
			return
		}

		babyUID := parts[2]
		command := parts[4]

		// Validate baby UID
		if err := baby.EnsureValidBabyUID(babyUID); err != nil {
			log.Error().Err(err).Str("topic", msg.Topic()).Msg("Invalid baby UID in MQTT light topic")
			return
		}

		// Handle different commands
		switch command {
		case "switch":
			enabled := string(msg.Payload()) == "true"
			log.Debug().
				Str("baby", babyUID).
				Bool("enabled", enabled).
				Str("payload", string(msg.Payload())).
				Msg("Received light command")

			conn.sendLightCommandHandler(enabled)
		default:
			log.Warn().Str("command", command).Msg("Unknown command received")
		}
	}

	if token := conn.client.Subscribe(commandTopic, 0, lightMessageHandler); token.Wait() && token.Error() != nil {
		log.Error().Err(token.Error()).Str("topic", commandTopic).Msg("Failed to subscribe to command topic")
	}
}

func (conn *Connection) RegisterStandyHandler(sendStandbyCommandHandler SendStandbyCommandHandler) {
	conn.sendStandbyCommandHandler = sendStandbyCommandHandler
}

func (conn *Connection) subscribeToStandbyCommand() {
	commandTopic := fmt.Sprintf("%v/babies/+/standby/switch", conn.Opts.TopicPrefix)
	log.Debug().
		Str("topic", commandTopic).
		Msg("Subscribing to command topic")

	standbyMessageHandler := func(mqttConn MQTT.Client, msg MQTT.Message) {
		// Extract baby UID and command from topic
		parts := strings.Split(msg.Topic(), "/")
		if len(parts) < 4 {
			log.Error().Str("topic", msg.Topic()).Msg("Invalid command topic format")
			return
		}

		babyUID := parts[2]
		command := parts[4]

		// Validate baby UID
		if err := baby.EnsureValidBabyUID(babyUID); err != nil {
			log.Error().Err(err).Str("topic", msg.Topic()).Msg("Invalid baby UID in MQTT standby topic")
			return
		}

		// Handle different commands
		switch command {
		case "switch":
			enabled := string(msg.Payload()) == "true"
			log.Debug().
				Str("baby", babyUID).
				Bool("enabled", enabled).
				Str("payload", string(msg.Payload())).
				Msg("Received standby command")

			conn.sendStandbyCommandHandler(enabled)
		default:
			log.Warn().Str("command", command).Msg("Unknown command received")
		}
	}

	if token := conn.client.Subscribe(commandTopic, 0, standbyMessageHandler); token.Wait() && token.Error() != nil {
		log.Error().Err(token.Error()).Str("topic", commandTopic).Msg("Failed to subscribe to command topic")
	}
}

// publishDiscovery publishes Home Assistant MQTT discovery messages for sensors and switches
func (conn *Connection) publishDiscovery(babyUID string, state baby.State) {
	// Build device info
	device := map[string]interface{}{
		"identifiers": []string{babyUID},
		"name":        fmt.Sprintf("Nanit %s", babyUID),
		"manufacturer": "Nanit",
	}

	if state.DeviceInfo != nil {
		if state.DeviceInfo.FirmwareVersion != nil {
			device["sw_version"] = *state.DeviceInfo.FirmwareVersion
		}
		if state.DeviceInfo.HardwareVersion != nil {
			device["model"] = *state.DeviceInfo.HardwareVersion
		}
	}

	// Helper to publish retained JSON to discovery topic
	publishConfig := func(topic string, payload interface{}) {
		b, err := json.Marshal(payload)
		if err != nil {
			log.Error().Err(err).Str("topic", topic).Msg("Failed to marshal discovery payload")
			return
		}

		token := conn.client.Publish(topic, 0, true, b)
		if token.Wait(); token.Error() != nil {
			log.Error().Err(token.Error()).Str("topic", topic).Msg("Failed to publish discovery payload")
		} else {
			log.Info().Str("topic", topic).Msg("Published Home Assistant discovery payload")
		}
	}

	nodePrefix := fmt.Sprintf("%s_%s", conn.Opts.TopicPrefix, babyUID)

	// Temperature sensor
	tempTopic := fmt.Sprintf("homeassistant/sensor/%s_temperature/config", nodePrefix)
	tempPayload := map[string]interface{}{
		"name":              fmt.Sprintf("Nanit %s Temperature", babyUID),
		"unique_id":         fmt.Sprintf("%s_temperature", nodePrefix),
		"state_topic":       fmt.Sprintf("%s/babies/%s/temperature", conn.Opts.TopicPrefix, babyUID),
		"unit_of_measurement": "°C",
		"device_class":      "temperature",
		"device":            device,
	}
	publishConfig(tempTopic, tempPayload)

	// Humidity sensor
	humTopic := fmt.Sprintf("homeassistant/sensor/%s_humidity/config", nodePrefix)
	humPayload := map[string]interface{}{
		"name":        fmt.Sprintf("Nanit %s Humidity", babyUID),
		"unique_id":   fmt.Sprintf("%s_humidity", nodePrefix),
		"state_topic": fmt.Sprintf("%s/babies/%s/humidity", conn.Opts.TopicPrefix, babyUID),
		"unit_of_measurement": "%",
		"device_class": "humidity",
		"device":      device,
	}
	publishConfig(humTopic, humPayload)

	// Night light switch
	nlTopic := fmt.Sprintf("homeassistant/switch/%s_night_light/config", nodePrefix)
	nlPayload := map[string]interface{}{
		"name":         fmt.Sprintf("Nanit %s Night Light", babyUID),
		"unique_id":    fmt.Sprintf("%s_night_light", nodePrefix),
		"state_topic":  fmt.Sprintf("%s/babies/%s/night_light", conn.Opts.TopicPrefix, babyUID),
		"command_topic": fmt.Sprintf("%s/babies/%s/night_light/switch", conn.Opts.TopicPrefix, babyUID),
		"payload_on":   "true",
		"payload_off":  "false",
		"device":       device,
	}
	publishConfig(nlTopic, nlPayload)

	// Standby switch
	stTopic := fmt.Sprintf("homeassistant/switch/%s_standby/config", nodePrefix)
	stPayload := map[string]interface{}{
		"name":         fmt.Sprintf("Nanit %s Standby", babyUID),
		"unique_id":    fmt.Sprintf("%s_standby", nodePrefix),
		"state_topic":  fmt.Sprintf("%s/babies/%s/standby", conn.Opts.TopicPrefix, babyUID),
		"command_topic": fmt.Sprintf("%s/babies/%s/standby/switch", conn.Opts.TopicPrefix, babyUID),
		"payload_on":   "true",
		"payload_off":  "false",
		"device":       device,
	}
	publishConfig(stTopic, stPayload)
}

func runMqtt(conn *Connection, attempt utils.AttemptContext) {

	if token := conn.client.Connect(); token.Wait() && token.Error() != nil {
		log.Error().Str("broker_url", conn.Opts.BrokerURL).Err(token.Error()).Msg("Unable to connect to MQTT broker")
		attempt.Fail(token.Error())
		return
	}

	log.Info().Str("broker_url", conn.Opts.BrokerURL).Msg("Successfully connected to MQTT broker")

	unsubscribe := conn.StateManager.Subscribe(func(babyUID string, state baby.State) {
		// Publish Home Assistant MQTT discovery for this baby once
		if _, ok := conn.publishedDiscoveries[babyUID]; !ok {
			conn.publishDiscovery(babyUID, state)
			conn.publishedDiscoveries[babyUID] = true
		}

		publish := func(key string, value interface{}) {
			topic := fmt.Sprintf("%v/babies/%v/%v", conn.Opts.TopicPrefix, babyUID, key)
			log.Trace().Str("topic", topic).Interface("value", value).Msg("MQTT publish")

			token := conn.client.Publish(topic, 0, false, fmt.Sprintf("%v", value))
			if token.Wait(); token.Error() != nil {
				log.Error().Err(token.Error()).Msgf("Unable to publish %v update", key)
			}
		}

		for key, value := range state.AsMap(false) {
			publish(key, value)
		}

		if state.StreamState != nil && *state.StreamState != baby.StreamState_Unknown {
			publish("is_stream_alive", *state.StreamState == baby.StreamState_Alive)
		}
	})

	// Subscribe to accept light mqtt messages
	conn.subscribeToLightCommand()
	conn.subscribeToStandbyCommand()

	// Wait until interrupt signal is received
	<-attempt.Done()

	log.Debug().Msg("Closing MQTT connection on interrupt")
	unsubscribe()
	conn.client.Disconnect(250)
}
