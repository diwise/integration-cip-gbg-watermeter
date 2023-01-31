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

func (a *app) NotificationReceived(ctx context.Context, n Notification) error {
	log := logging.GetFromContext(ctx)

	log.Debug().Msgf("notification received with %d entities", len(n.Entities))

	for i, e := range n.Entities {
		entity := Entity{}
		err := json.Unmarshal(e, &entity)
		if err != nil {
			log.Error().Err(err).Msgf("unable to unmarshal entity [%d] in notification", i)
			return err
		}

		switch strings.ToLower(entity.Type) {
		case "waterconsumptionobserved":
			return handleWaterConsumptionObserved(ctx, e, db)
		case "indoorenvironmentobserved":
			return handleIndoorEnvironmentObserved(ctx, e, db)
		case "weatherobserved":
			return handleWeatherObserved(ctx, e, db)
		default:
			log.Debug().Msgf("unsupported type %s", entity.Type)
			return nil
		}
	}

	return nil
}
