package application

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/matryer/is"
)

func TestWaterConsumptionObserved(t *testing.T) {
	is, a, _ := setupTest(t)

	err := a.handleWaterConsumptionObserved(context.Background(), createNotification().Entities[0])

	is.NoErr(err)
}

func TestIndoorEnvironmentObserved(t *testing.T) {
	is, a, _ := setupTest(t)

	err := a.handleIndoorEnvironmentObserved(context.Background(), createNotification().Entities[1])

	is.NoErr(err)
}

func TestWeatherObserved(t *testing.T) {
	is, a, _ := setupTest(t)

	err := a.handleWeatherObserved(context.Background(), createNotification().Entities[2])

	is.NoErr(err)
}

func createNotification() Notification {
	n := Notification{}
	err := json.Unmarshal([]byte(notifications), &n)
	if err != nil {
		panic(err)
	}

	return n
}

func setupTest(t *testing.T) (*is.I, app, Storage) {
	is := is.New(t)

	s := &StorageMock{
		StoreIndoorEnvironmentObservedFunc: func(ctx context.Context, i indoorEnvironmentObserved) error {
			return nil
		},
		StoreWaterConsumptionObservedFunc: func(ctx context.Context, w waterConsumptionObserved) error {
			return nil
		},
		StoreWeatherObservedFunc: func(ctx context.Context, w weatherObserved) error {
			return nil
		},
	}
	a := app{
		storage: s,
	}

	return is, a, s
}

const notifications string = `
{
	"id": "urn:ngsi-ld:Notification:419ef219-06f9-40cb-95eb-97d877036dcf",
	"type": "Notification",
	"subscriptionId": "notimplemented",
	"notifiedAt": "2022-06-02T08:34:05.237466Z",
	"data": [
		{
			"id": "urn:ngsi-ld:Consumer:Consumer01",
			"type": "WaterConsumptionObserved",
			"acquisitionStageFailure": {
				"type": "Property",
				"observedBy": {
					"type": "Relationship",
					"object": "urn:ngsi-ld:Device:01"
				},
				"value": 0,
				"observedAt": "2021-05-23T23:14:16.000Z"
			},
			"alarmFlowPersistence": {
				"type": "Property",
				"observedBy": {
					"type": "Relationship",
					"object": "urn:ngsi-ld:Device:01"
				},
				"value": "Nothing to report",
				"observedAt": "2021-05-23T23:14:16.000Z"
			},
			"alarmInProgress": {
				"type": "Property",
				"observedBy": {
					"type": "Relationship",
					"object": "urn:ngsi-ld:Device:01"
				},
				"value": 1,
				"observedAt": "2021-05-23T23:14:16.000Z"
			},
			"alarmMetrology": {
				"type": "Property",
				"observedBy": {
					"type": "Relationship",
					"object": "urn:ngsi-ld:Device:01"
				},
				"value": 1,
				"observedAt": "2021-05-23T23:14:16.000Z"
			},
			"alarmStopsLeaks": {
				"type": "Property",
				"observedBy": {
					"type": "Relationship",
					"object": "urn:ngsi-ld:Device:01"
				},
				"value": 0,
				"observedAt": "2021-05-23T23:14:16.000Z"
			},
			"alarmSystem": {
				"type": "Property",
				"observedBy": {
					"type": "Relationship",
					"object": "urn:ngsi-ld:Device:01"
				},
				"value": 1,
				"observedAt": "2021-05-23T23:14:16.000Z"
			},
			"alarmTamper": {
				"type": "Property",
				"observedBy": {
					"type": "Relationship",
					"object": "urn:ngsi-ld:Device:01"
				},
				"value": 0,
				"observedAt": "2021-05-23T23:14:16.000Z"
			},
			"alarmWaterQuality": {
				"type": "Property",
				"observedBy": {
					"type": "Relationship",
					"object": "urn:ngsi-ld:Device:01"
				},
				"value": 0,
				"observedAt": "2021-05-23T23:14:16.000Z"
			},
			"location": {
				"type": "GeoProperty",
				"value": {
					"type": "Point",
					"coordinates": [
						-4.128871,
						50.95822
					]
				}
			},
			"maxFlow": {
				"type": "Property",
				"observedBy": {
					"type": "Relationship",
					"object": "urn:ngsi-ld:Device:01"
				},
				"value": 620,
				"observedAt": "2021-05-23T23:14:16.000Z",
				"unitCode": "E32"
			},
			"minFlow": {
				"type": "Property",
				"observedBy": {
					"type": "Relationship",
					"object": "urn:ngsi-ld:Device:01"
				},
				"value": 1,
				"observedAt": "2021-05-23T23:14:16.000Z",
				"unitCode": "E32"
			},
			"moduleTampered": {
				"type": "Property",
				"observedBy": {
					"type": "Relationship",
					"object": "urn:ngsi-ld:Device:01"
				},
				"value": 1,
				"observedAt": "2021-05-23T23:14:16.000Z"
			},
			"persistenceFlowDuration": {
				"type": "Property",
				"observedBy": {
					"type": "Relationship",
					"object": "urn:ngsi-ld:Device:01"
				},
				"value": "3h < 6h",
				"observedAt": "2021-05-23T23:14:16.000Z",
				"unitCode": "HUR"
			},
			"waterConsumption": {
				"type": "Property",
				"observedBy": {
					"type": "Relationship",
					"object": "urn:ngsi-ld:Device:01"
				},
				"value": 191051,
				"observedAt": "2021-05-23T23:14:16.000Z",
				"unitCode": "LTR"
			},
			"@context": [
				"https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/WaterSmartMeter/jsonld-contexts/waterSmartMeter-compound.jsonld",
				"https://raw.githubusercontent.com/smart-data-models/dataModel.WaterConsumption/master/context.jsonld"
			]
		},
		{
			"@context": [
				"https://raw.githubusercontent.com/diwise/context-broker/main/assets/jsonldcontexts/default-context.jsonld"
			],
			"dateObserved": {
				"type": "Property",
				"value": {
					"@type": "DateTime",
					"@value": "2023-01-31T12:44:47.439079092Z"
				}
			},
			"id": "urn:ngsi-ld:IndoorEnvironmentObserved:intern-01",
			"location": {
				"type": "GeoProperty",
				"value": {
					"type": "Point",
					"coordinates": [
						16,
						37
					]
				}
			},
			"temperature": {
				"type": "Property",
				"value": 21.4,
				"observedAt": "2023-01-31T12:45:18Z"
			},
			"humidity": {
				"type": "Property",
				"value": 21.4,
				"observedAt": "2023-01-31T12:45:18Z"
			},
		
			"type": "IndoorEnvironmentObserved"
		},
		{
			"@context": [
				"https://raw.githubusercontent.com/diwise/context-broker/main/assets/jsonldcontexts/default-context.jsonld"
			],
			"dateObserved": {
				"type": "Property",
				"value": {
					"@type": "DateTime",
					"@value": "2023-01-31T12:45:23.053016674Z"
				}
			},
			"id": "urn:ngsi-ld:WeatherObserved:intern-10a52aaa84c35730:2023-01-31T12:45:23.053016674Z",
			"location": {
				"type": "GeoProperty",
				"value": {
					"type": "Point",
					"coordinates": [
						17.285092,
						62.392013
					]
				}
			},
			"temperature": {
				"type": "Property",
				"value": 2.3,
				"observedAt": "2023-01-31T12:45:54Z"
			},
			"type": "WeatherObserved"
		}
	]
}
`
