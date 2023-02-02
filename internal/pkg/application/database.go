package application

import (
	"context"
	"fmt"

	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/jackc/pgx/v4"
	"github.com/rs/zerolog"
)

//go:generate moq -rm -out database_mock.go . Storage

type Storage interface {
	StoreWaterConsumptionObserved(ctx context.Context, w waterConsumptionObserved) error
	StoreWeatherObserved(ctx context.Context, w weatherObserved) error
	StoreIndoorEnvironmentObserved(ctx context.Context, i indoorEnvironmentObserved) error
}

type storage struct {
	connUrl string
	source  string
	schema  string
}

func NewStorage() (Storage, error) {
	log := zerolog.Logger{}

	s := &storage{}

	s.source = env.GetVariableOrDefault(log, "WCO_SOURCE", "Göteborgs Stads kretslopp och vattennämnd")
	s.schema = env.GetVariableOrDefault(log, "DB_SCHEMA", "geodata_vattenmatare")

	var pgUser = env.GetVariableOrDefault(log, "PG_USER", "")
	var pgPassword = env.GetVariableOrDefault(log, "PG_PASSWORD", "")
	var pgHostname = env.GetVariableOrDefault(log, "PG_HOSTNAME", "")
	var pgPort = env.GetVariableOrDefault(log, "PG_PORT", "5432")
	var pgDatabaseName = env.GetVariableOrDefault(log, "PG_DATABASE", "")

	s.connUrl = fmt.Sprintf("postgres://%s:%s@%s:%s/%s", pgUser, pgPassword, pgHostname, pgPort, pgDatabaseName)

	_, err := pgx.ParseConfig(s.connUrl)
	if err != nil {
		return s, err
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, s.connUrl)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	return s, nil
}

func (s *storage) StoreWaterConsumptionObserved(ctx context.Context, wco waterConsumptionObserved) error {
	var x, y float64 = 0.0, 0.0
	if wco.Location.Value.Coordinates != nil && len(wco.Location.Value.Coordinates) > 1 {
		x = wco.Location.Value.Coordinates[0]
		y = wco.Location.Value.Coordinates[1]
	}

	sql := fmt.Sprintf(`INSERT INTO %s.waterConsumptionObserved ("id", "waterConsumption", "unitCode", "observedAt", "location", "source", "createdAt") VALUES ('%s', '%0.2f', '%s', '%s', ST_MakePoint(%0.6f,%0.6f), '%s', current_timestamp) ON CONFLICT DO NOTHING;`, s.schema, wco.Id, wco.WaterConsumption.Value, wco.WaterConsumption.UnitCode, wco.WaterConsumption.ObservedAt, x, y, s.source)

	return s.exec(ctx, sql)
}

func (s *storage) StoreWeatherObserved(ctx context.Context, wo weatherObserved) error {
	var x, y float64 = 0.0, 0.0
	if wo.Location.Value.Coordinates != nil && len(wo.Location.Value.Coordinates) > 1 {
		x = wo.Location.Value.Coordinates[0]
		y = wo.Location.Value.Coordinates[1]
	}

	t := wo.Temperature.Value
	observedAt := ""
	if wo.Temperature.ObservedAt != "" {
		observedAt = wo.Temperature.ObservedAt
	}

	sql := fmt.Sprintf(`INSERT INTO %s.weatherObserved ("id", "temperature", "observedAt", "location", "source", "createdAt") VALUES ('%s', '%0.2f', '%s', ST_MakePoint(%0.6f,%0.6f), '%s', current_timestamp) ON CONFLICT DO NOTHING;`, s.schema, wo.Id, t, observedAt, x, y, s.source)

	return s.exec(ctx, sql)
}

func (s *storage) StoreIndoorEnvironmentObserved(ctx context.Context, ieo indoorEnvironmentObserved) error {
	var x, y float64 = 0.0, 0.0
	if ieo.Location.Value.Coordinates != nil && len(ieo.Location.Value.Coordinates) > 1 {
		x = ieo.Location.Value.Coordinates[0]
		y = ieo.Location.Value.Coordinates[1]
	}

	t := ieo.Temperature.Value
	h := ieo.Humidity.Value
	observedAt := ""
	if ieo.Temperature.ObservedAt != "" {
		observedAt = ieo.Temperature.ObservedAt
	} else if ieo.Humidity.ObservedAt != "" {
		observedAt = ieo.Humidity.ObservedAt
	}

	sql := fmt.Sprintf(`INSERT INTO %s.indoorEnvironmentObserved ("id", "temperature", "humidity", "observedAt", "location", "source", "createdAt") VALUES ('%s', '%0.2f', '%0.2f', '%s', ST_MakePoint(%0.6f,%0.6f), '%s', current_timestamp) ON CONFLICT DO NOTHING;`, s.schema, ieo.Id, t, h, observedAt, x, y, s.source)

	return s.exec(ctx, sql)
}

func (s *storage) exec(ctx context.Context, sql string) error {
	log := logging.GetFromContext(ctx)

	log.Debug().Msg(sql)

	conn, err := pgx.Connect(ctx, s.connUrl)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, sql)
	if err != nil {
		return err
	}

	return nil
}
