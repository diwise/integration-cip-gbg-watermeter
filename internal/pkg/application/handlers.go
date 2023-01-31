package application

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/jackc/pgx/v4"
	"github.com/rs/zerolog"
)

var pgConnUrl string
var source string
var schema string

func init() {
	pgConnUrl = ""
	source = env.GetVariableOrDefault(zerolog.Logger{}, "WCO_SOURCE", "Göteborgs Stads kretslopp och vattennämnd")
	schema = env.GetVariableOrDefault(zerolog.Logger{}, "DB_SCHEMA", "geodata_vattenmatare")
}

type StoreFunc func(ctx context.Context, log zerolog.Logger, exec func(tx pgx.Tx) error) error

func db(ctx context.Context, log zerolog.Logger, exec func(tx pgx.Tx) error) error {
	if pgConnUrl == "" {
		pgConnUrl = env.GetVariableOrDie(log, "PG_CONNECTION_URL", "url to postgres database, i.e. postgres://username:password@hostname:5433/database_name")
	}

	conn, err := pgx.Connect(ctx, pgConnUrl)
	if err != nil {
		log.Fatal().Err(err).Msg("unable to connect to database")
	}
	defer conn.Close(ctx)

	err = conn.BeginFunc(ctx, exec)

	return err
}

type property struct {
	Value      float64 `json:"value"`
	UnitCode   string  `json:"unitCode,omitempty"`
	ObservedAt string  `json:"observedAt"`
	ObservedBy struct {
		Type   string `json:"type"`
		Object string `json:"object"`
	} `json:"observedBy"`
}

type point struct {
	Type  string `json:"Type"`
	Value struct {
		Type        string    `json:"type"`
		Coordinates []float64 `json:"coordinates"`
	} `json:"value"`
}

type waterConsumptionObserved struct {
	Id               string   `json:"id"`
	Type             string   `json:"type"`
	WaterConsumption property `json:"waterConsumption"`
	Location         point    `json:"location"`
}

type indoorEnvironmentObserved struct {
	Id          string   `json:"id"`
	Type        string   `json:"type"`
	Temperature property `json:"temperature,omitempty"`
	Humidity    property `json:"humidity,omitempty"`
	Location    point    `json:"location"`
}

type weatherObserved struct {
	Id          string   `json:"id"`
	Type        string   `json:"type"`
	Temperature property `json:"temperature,omitempty"`
	Location    point    `json:"location"`
}

func handleIndoorEnvironmentObserved(ctx context.Context, j json.RawMessage, store StoreFunc) error {
	log := logging.GetFromContext(ctx)
	ieo := indoorEnvironmentObserved{}
	err := json.Unmarshal(j, &ieo)
	if err != nil {
		log.Error().Err(err).Msg("failed to unmarshal notification entity into indoorEnvironmentObserved")
	}

	log.Debug().Msgf("handle %s", ieo.Id)

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

	err = store(ctx, log, func(tx pgx.Tx) error {
		insert := fmt.Sprintf(`INSERT INTO %s.indoorEnvironmentObserved ("id", "temperature", "humidity", "observedAt", "location", "source", "createdAt") VALUES ('%s', '%0.2f', '%0.2f', '%s', ST_MakePoint(%0.6f,%0.6f), '%s', current_timestamp) ON CONFLICT DO NOTHING;`, schema, ieo.Id, t, h, observedAt, x, y, source)

		log.Debug().Msg(insert)

		_, err := tx.Exec(ctx, insert)
		if err != nil {
			log.Error().Err(err).Msg("failed to insert or update data in database")
		}

		return err
	})

	return err
}

func handleWeatherObserved(ctx context.Context, j json.RawMessage, store StoreFunc) error {
	log := logging.GetFromContext(ctx)
	wo := weatherObserved{}
	err := json.Unmarshal(j, &wo)
	if err != nil {
		log.Error().Err(err).Msg("failed to unmarshal notification entity into weatherObserved")
	}

	log.Debug().Msgf("handle %s", wo.Id)

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

	err = store(ctx, log, func(tx pgx.Tx) error {
		insert := fmt.Sprintf(`INSERT INTO %s.weatherObserved ("id", "temperature", "observedAt", "location", "source", "createdAt") VALUES ('%s', '%0.2f', '%s', ST_MakePoint(%0.6f,%0.6f), '%s', current_timestamp) ON CONFLICT DO NOTHING;`, schema, wo.Id, t, observedAt, x, y, source)

		log.Debug().Msg(insert)

		_, err := tx.Exec(ctx, insert)
		if err != nil {
			log.Error().Err(err).Msg("failed to insert or update data in database")
		}

		return err
	})

	return err
}

func handleWaterConsumptionObserved(ctx context.Context, j json.RawMessage, store StoreFunc) error {
	log := logging.GetFromContext(ctx)
	wco := waterConsumptionObserved{}
	err := json.Unmarshal(j, &wco)
	if err != nil {
		log.Error().Err(err).Msg("failed to unmarshal notification entity into waterConsumptionObserved")
	}

	log.Debug().Msgf("handle %s", wco.Id)

	var x, y float64 = 0.0, 0.0
	if wco.Location.Value.Coordinates != nil && len(wco.Location.Value.Coordinates) > 1 {
		x = wco.Location.Value.Coordinates[0]
		y = wco.Location.Value.Coordinates[1]
	}

	err = store(ctx, log, func(tx pgx.Tx) error {
		insert := fmt.Sprintf(`INSERT INTO %s.waterConsumptionObserved ("id", "waterConsumption", "unitCode", "observedAt", "location", "source", "createdAt") VALUES ('%s', '%0.2f', '%s', '%s', ST_MakePoint(%0.6f,%0.6f), '%s', current_timestamp) ON CONFLICT DO NOTHING;`, schema, wco.Id, wco.WaterConsumption.Value, wco.WaterConsumption.UnitCode, wco.WaterConsumption.ObservedAt, x, y, source)

		log.Debug().Msg(insert)

		_, err := tx.Exec(ctx, insert)
		if err != nil {
			log.Error().Err(err).Msg("failed to insert or update data in database")
		}

		return err
	})

	return err
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
    CONSTRAINT pkey PRIMARY KEY("id", "observedAt")
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
    CONSTRAINT pkey PRIMARY KEY("id", "observedAt")
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
    CONSTRAINT pkey PRIMARY KEY("id", "observedAt")
);

CREATE VIEW geodata_vattenmatare."latestWeatherObserved"
 AS select distinct on ("id") "id", "temperature", "source", "location", "observedAt"
from geodata_vattenmatare.weatherObserved
order by id, "observedAt" desc;

ALTER TABLE geodata_vattenmatare."latestWeatherObserved"
    OWNER TO postgres;	

*/
