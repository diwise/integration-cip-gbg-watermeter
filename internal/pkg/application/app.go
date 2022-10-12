package application

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

//go:generate moq -rm -out app_mock.go . App

type App interface {
	NotificationReceived(ctx context.Context, n Notification) error
}

type app struct {
}

type Entity struct {
	Id   string `json:"id"`
	Type string `json:"type"`
}

type Notification struct {
	Entity
	SubscriptionId string            `json:"subscriptionId"`
	NotifiedAt     string            `json:"notifiedAt"`
	Entities       []json.RawMessage `json:"data"`
}

func NewApp() App {
	return &app{}
}

type NotificationHandler = func(ctx context.Context, n Notification) error

func (a *app) NotificationReceived(ctx context.Context, n Notification) error {
	log := logging.GetFromContext(ctx)

	for _, e := range n.Entities {
		entity := Entity{}
		json.Unmarshal(e, &entity)

		switch strings.ToLower(entity.Type) {
		case "waterconsumptionobserved":
			return handleWaterConsumptionObserved(ctx, e, db)
		default:
			log.Info().Msgf("unsupported type %s", n.Type)
			return nil
		}
	}

	return nil
}
