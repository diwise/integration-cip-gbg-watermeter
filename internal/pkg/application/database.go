package application

import (
	"context"
	"fmt"

	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

//go:generate moq -rm -out database_mock.go . Storage

type Storage interface {
	StoreWaterConsumptionObserved(ctx context.Context, w WaterConsumptionObserved) error
	StoreWeatherObserved(ctx context.Context, w WeatherObserved) error
	StoreIndoorEnvironmentObserved(ctx context.Context, i IndoorEnvironmentObserved) error
}

type storage struct {
	connUrl string
	source  string
	schema  string
}

func NewStorage() (Storage, error) {
	log := zerolog.Logger{}

	var pgUser = env.GetVariableOrDefault(log, "PG_USER", "")
	var pgPassword = env.GetVariableOrDefault(log, "PG_PASSWORD", "")
	var pgHostname = env.GetVariableOrDefault(log, "PG_HOSTNAME", "")
	var pgPort = env.GetVariableOrDefault(log, "PG_PORT", "5432")
	var pgDatabaseName = env.GetVariableOrDefault(log, "PG_DATABASE", "")

	s := &storage{
		source:  env.GetVariableOrDefault(log, "WCO_SOURCE", "Göteborgs Stads kretslopp och vattennämnd"),
		schema:  env.GetVariableOrDefault(log, "DB_SCHEMA", "geodata_vattenmatare"),
		connUrl: fmt.Sprintf("postgres://%s:%s@%s:%s/%s", pgUser, pgPassword, pgHostname, pgPort, pgDatabaseName),
	}

	ctx := context.Background()
	dbpool, err := pgxpool.New(ctx, s.connUrl)
	if err != nil {
		return nil, err
	}
	defer dbpool.Close()

	return s, nil
}

func (s *storage) StoreWaterConsumptionObserved(ctx context.Context, wco WaterConsumptionObserved) error {
	var x, y float64 = 0.0, 0.0
	if wco.Location.Value.Coordinates != nil && len(wco.Location.Value.Coordinates) > 1 {
		x = wco.Location.Value.Coordinates[0]
		y = wco.Location.Value.Coordinates[1]
	}

	sql := fmt.Sprintf(`INSERT INTO %s.waterConsumptionObserved ("id", "waterConsumption", "unitCode", "observedAt", "location", "source", "createdAt") VALUES ($1, $2, $3, $4, ST_MakePoint($5,$6), $7, current_timestamp) ON CONFLICT DO NOTHING;`, s.schema)

	return s.exec(ctx, sql, wco.Id, wco.WaterConsumption.Value, wco.WaterConsumption.UnitCode, wco.WaterConsumption.ObservedAt, x, y, s.source)
}

func (s *storage) StoreWeatherObserved(ctx context.Context, wo WeatherObserved) error {
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

	sql := fmt.Sprintf(`INSERT INTO %s.weatherObserved ("id", "temperature", "observedAt", "location", "source", "createdAt") VALUES ($1, $2, $3, ST_MakePoint($4,$5), $6, current_timestamp) ON CONFLICT DO NOTHING;`, s.schema)

	return s.exec(ctx, sql, wo.Id, t, observedAt, x, y, s.source)
}

func (s *storage) StoreIndoorEnvironmentObserved(ctx context.Context, ieo IndoorEnvironmentObserved) error {
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

	sql := fmt.Sprintf(`INSERT INTO %s.indoorEnvironmentObserved ("id", "temperature", "humidity", "observedAt", "location", "source", "createdAt") VALUES ($1, $2, $3, $4, ST_MakePoint($5,$6), $7, current_timestamp) ON CONFLICT DO NOTHING;`, s.schema)

	return s.exec(ctx, sql, ieo.Id, t, h, observedAt, x, y, s.source)
}

func (s *storage) exec(ctx context.Context, sql string, arguments ...any) error {
	log := logging.GetFromContext(ctx)

	log.Debug().Msg(sql)

	dbpool, err := pgxpool.New(ctx, s.connUrl)
	if err != nil {
		return err
	}
	defer dbpool.Close()

	_, err = dbpool.Exec(ctx, sql, arguments...)
	if err != nil {
		return err
	}

	return nil
}

/*
-- TABLE

CREATE SCHEMA geodata_vattenmatare;

CREATE TABLE geodata_vattenmatare.waterConsumptionObserved
(
    "id" text COLLATE pg_catalog."default" NOT NULL,
    "waterConsumption" numeric,
    "unitCode" text COLLATE pg_catalog."default",
    "observedAt" timestamp,
    "source" text,
    "location" geometry(Geometry, 4326),
	"createdAt" timestamp,
    CONSTRAINT pkey_wco PRIMARY KEY("id", "observedAt")
);

CREATE VIEW geodata_vattenmatare."latestWaterConsumptionObserved"
 AS select distinct on ("id") "id", "waterConsumption", "unitCode", "source", "location", "observedAt"
from geodata_vattenmatare.waterconsumptionobserved
order by id, "observedAt" desc;

ALTER TABLE geodata_vattenmatare."latestWaterConsumptionObserved"
    OWNER TO postgres;



CREATE TABLE geodata_vattenmatare.indoorEnvironmentObserved
(
    "id" text COLLATE pg_catalog."default" NOT NULL,
    "temperature" numeric,
	"humidity" numeric,
    "observedAt" timestamp,
    "source" text,
    "location" geometry(Geometry, 4326),
	"createdAt" timestamp,
    CONSTRAINT pkey_ieo PRIMARY KEY("id", "observedAt")
);

CREATE VIEW geodata_vattenmatare."latestIndoorEnvironmentObserved"
 AS select distinct on ("id") "id", "temperature", "humidity", "source", "location", "observedAt"
from geodata_vattenmatare.indoorEnvironmentObserved
order by id, "observedAt" desc;

ALTER TABLE geodata_vattenmatare."latestIndoorEnvironmentObserved"
    OWNER TO postgres;



CREATE TABLE geodata_vattenmatare.weatherObserved
(
    "id" text COLLATE pg_catalog."default" NOT NULL,
    "temperature" numeric,
    "observedAt" timestamp,
    "source" text,
    "location" geometry(Geometry, 4326),
	"createdAt" timestamp,
    CONSTRAINT pkey_wo PRIMARY KEY("id", "observedAt")
);

CREATE VIEW geodata_vattenmatare."latestWeatherObserved"
 AS select distinct on ("id") "id", "temperature", "source", "location", "observedAt"
from geodata_vattenmatare.weatherObserved
order by id, "observedAt" desc;

ALTER TABLE geodata_vattenmatare."latestWeatherObserved"
    OWNER TO postgres;

*/
