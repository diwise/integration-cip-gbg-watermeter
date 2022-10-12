package application

import (
	"context"
	"encoding/json"

	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

type App interface {
	NotificationReceived(ctx context.Context, n Notification) error
}

type app struct {
}

type Notification struct {
	Id             string          `json:"id"`
	Type           string          `json:"type"`
	SubscriptionId string          `json:"subscriptionId"`
	NotifiedAt     string          `json:"notifiedAt"`
	Data           json.RawMessage `json:"data"`
}

func NewApp() App {
	return &app{}
}

func (a *app) NotificationReceived(ctx context.Context, n Notification) error {

	log := logging.GetFromContext(ctx)

	switch n.Type {
	case "WaterConsumptionObserved":
		return handleWaterConsumptionObserved(ctx, n)
	default:
		log.Info().Msgf("unsupported type %s", n.Type)
		return nil
	}
}

func handleWaterConsumptionObserved(ctx context.Context, n Notification) error {
	log := logging.GetFromContext(ctx)
	log.Info().Msgf("unsupported type %s", n.Type)

	return nil
}
