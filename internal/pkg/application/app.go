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
	storage Storage
}

func New(s Storage) App {
	return &app{
		storage: s,
	}
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
			return a.handleWaterConsumptionObserved(ctx, e)
		case "indoorenvironmentobserved":
			return a.handleIndoorEnvironmentObserved(ctx, e)
		case "weatherobserved":
			return a.handleWeatherObserved(ctx, e)
		default:
			log.Debug().Msgf("unsupported type %s", entity.Type)
			return nil
		}
	}

	return nil
}

func (a app) handleIndoorEnvironmentObserved(ctx context.Context, j json.RawMessage) error {
	log := logging.GetFromContext(ctx)
	ieo := IndoorEnvironmentObserved{}
	err := json.Unmarshal(j, &ieo)
	if err != nil {
		log.Error().Err(err).Msg("failed to unmarshal notification entity into indoorEnvironmentObserved")
	}

	log.Debug().Msgf("handle %s", ieo.Id)

	return a.storage.StoreIndoorEnvironmentObserved(ctx, ieo)
}

func (a app) handleWeatherObserved(ctx context.Context, j json.RawMessage) error {
	log := logging.GetFromContext(ctx)
	wo := WeatherObserved{}
	err := json.Unmarshal(j, &wo)
	if err != nil {
		log.Error().Err(err).Msg("failed to unmarshal notification entity into weatherObserved")
	}

	log.Debug().Msgf("handle %s", wo.Id)

	return a.storage.StoreWeatherObserved(ctx, wo)
}

func (a app) handleWaterConsumptionObserved(ctx context.Context, j json.RawMessage) error {
	log := logging.GetFromContext(ctx)
	wco := WaterConsumptionObserved{}
	err := json.Unmarshal(j, &wco)
	if err != nil {
		log.Error().Err(err).Msg("failed to unmarshal notification entity into waterConsumptionObserved")
	}

	log.Debug().Msgf("handle %s", wco.Id)

	return a.storage.StoreWaterConsumptionObserved(ctx, wco)
}
