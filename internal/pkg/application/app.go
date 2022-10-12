package application

import "context"

type App interface {
	NotificationReceived(ctx context.Context, n Notification) error
}

type app struct {
}

type Notification struct {
}

func NewApp() App {
	return &app{}
}

func (a *app) NotificationReceived(ctx context.Context, n Notification) error {
	return nil
}
