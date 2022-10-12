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
		log.Error().Err(err).Msg("failed to unmarshal notification entity")
	}

	err = store(ctx, log, func(tx pgx.Tx) error {
		insert := fmt.Sprintf("INSERT INTO geodata_cip.waterConsumptionObserved (id, volume, unitCode, observedAt) VALUES ('%s', '%0.1f', '%s', '%s') ON CONFLICT DO NOTHING;", wco.Id, wco.WaterConsumption.Value, wco.WaterConsumption.UnitCode, wco.WaterConsumption.ObservedAt)
		_, err := tx.Exec(ctx, insert)

		return err
	})

	return err
}

/*
CREATE TABLE geodata_cip.waterConsumptionObserved
(
	"id" string NOT NULL,
	"volume" numeric,
	"unitCode" text COLLATE pg_catalog."default",
	"observedAt" timestamp,
	"geom" geometry(Geometry,3007),
	CONSTRAINT waterConsumptionObserved_pkey PRIMARY KEY (id, observedAt)
)
*/
