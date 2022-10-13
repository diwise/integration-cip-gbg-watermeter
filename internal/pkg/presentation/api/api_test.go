package api

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/diwise/integration-cip-gbg-watermeter/internal/pkg/application"
	"github.com/go-chi/chi/v5"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func TestThatNotificationIsUnmarshaledCorrect(t *testing.T) {
	is, ts, _, _ := setupTest(t)
	defer ts.Close()

	req, _ := http.NewRequest("POST", ts.URL+"/v2/notify", bytes.NewBuffer([]byte(waterConsumptionObserved_notification)))
	req.Header.Add("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	is.NoErr(err) // http request failed
	defer resp.Body.Close()
}

func setupTest(t *testing.T) (*is.I, *httptest.Server, zerolog.Logger, api) {
	is := is.New(t)
	r := chi.NewRouter()
	ts := httptest.NewServer(r)
	log := log.Logger

	api := api{
		log: log,
		r:   r,
		app: &application.AppMock{
			NotificationReceivedFunc: func(ctx context.Context, n application.Notification) error {
				return nil
			},
		},
	}

	registerHandlers(r, log, api)

	return is, ts, log, api
}

const waterConsumptionObserved_notification string = `
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
		}
	]
}
`
