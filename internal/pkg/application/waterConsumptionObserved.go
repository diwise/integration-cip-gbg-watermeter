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

type waterConsumptionObserved struct {
	Id               string   `json:"id"`
	Type             string   `json:"type"`
	WaterConsumption property `json:"waterConsumption"`
	Location         point    `json:"location"`
}
type property struct {
	Value      float64 `json:"value"`
	UnitCode   string  `json:"unitCode"`
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

var pgConnUrl string = ""
var source string = ""

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

	if source == "" {
		source = env.GetVariableOrDefault(log, "WCO_SOURCE", "Göteborgs Stads kretslopp och vattennämnd")
	}

	err = store(ctx, log, func(tx pgx.Tx) error {
		insert := fmt.Sprintf(`INSERT INTO geodata_vattenmatare.waterConsumptionObserved ("id", "waterConsumption", "unitCode", "observedAt", "location", "source") VALUES ('%s', '%0.1f', '%s', '%s', ST_MakePoint(%0.1f,%0.1f), '%s') ON CONFLICT DO NOTHING;`, wco.Id, wco.WaterConsumption.Value, wco.WaterConsumption.UnitCode, wco.WaterConsumption.ObservedAt, x, y, source)

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
    CONSTRAINT pkey PRIMARY KEY("id", "observedAt")
);

CREATE VIEW geodata_vattenmatare."latestWaterConsumptionObserved"
 AS select distinct on ("id") "id", "waterConsumption", "unitCode", "source", "location", "observedAt"
from geodata_vattenmatare.waterconsumptionobserved
order by id, "observedAt" desc;

ALTER TABLE geodata_vattenmatare."latestWaterConsumptionObserved"
    OWNER TO postgres;
*/
