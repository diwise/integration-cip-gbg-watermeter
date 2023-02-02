package application

import (
	"context"
	"encoding/json"

	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

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

func (a app) handleIndoorEnvironmentObserved(ctx context.Context, j json.RawMessage) error {
	log := logging.GetFromContext(ctx)
	ieo := indoorEnvironmentObserved{}
	err := json.Unmarshal(j, &ieo)
	if err != nil {
		log.Error().Err(err).Msg("failed to unmarshal notification entity into indoorEnvironmentObserved")
	}

	log.Debug().Msgf("handle %s", ieo.Id)

	return a.storage.StoreIndoorEnvironmentObserved(ctx, ieo)
}

func (a app) handleWeatherObserved(ctx context.Context, j json.RawMessage) error {
	log := logging.GetFromContext(ctx)
	wo := weatherObserved{}
	err := json.Unmarshal(j, &wo)
	if err != nil {
		log.Error().Err(err).Msg("failed to unmarshal notification entity into weatherObserved")
	}

	log.Debug().Msgf("handle %s", wo.Id)

	return a.storage.StoreWeatherObserved(ctx, wo)
}

func (a app) handleWaterConsumptionObserved(ctx context.Context, j json.RawMessage) error {
	log := logging.GetFromContext(ctx)
	wco := waterConsumptionObserved{}
	err := json.Unmarshal(j, &wco)
	if err != nil {
		log.Error().Err(err).Msg("failed to unmarshal notification entity into waterConsumptionObserved")
	}

	log.Debug().Msgf("handle %s", wco.Id)

	return a.storage.StoreWaterConsumptionObserved(ctx, wco)
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
