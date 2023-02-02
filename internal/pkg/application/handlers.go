package application

import (
	"context"
	"encoding/json"

	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

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
