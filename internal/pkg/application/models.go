package application

import "encoding/json"

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

type Property struct {
	Value      float64 `json:"value"`
	UnitCode   string  `json:"unitCode,omitempty"`
	ObservedAt string  `json:"observedAt"`
	ObservedBy struct {
		Type   string `json:"type"`
		Object string `json:"object"`
	} `json:"observedBy"`
}

type Point struct {
	Type  string `json:"Type"`
	Value struct {
		Type        string    `json:"type"`
		Coordinates []float64 `json:"coordinates"`
	} `json:"value"`
}

type WaterConsumptionObserved struct {
	Entity
	WaterConsumption Property `json:"waterConsumption"`
	Location         Point    `json:"location"`
}

type IndoorEnvironmentObserved struct {
	Entity
	Temperature Property `json:"temperature,omitempty"`
	Humidity    Property `json:"humidity,omitempty"`
	Location    Point    `json:"location"`
}

type WeatherObserved struct {
	Entity
	Temperature Property `json:"temperature,omitempty"`
	Location    Point    `json:"location"`
}
