package main

import (
	"context"

	"github.com/diwise/service-chassis/pkg/infrastructure/buildinfo"
	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/metrics"
	"github.com/go-chi/chi/v5"

	"github.com/diwise/integration-cip-gbg-watermeter/internal/pkg/application"
	"github.com/diwise/integration-cip-gbg-watermeter/internal/pkg/presentation/api"
)

const serviceName string = "integration-cip-gbg-watermeter"

func main() {
	serviceVersion := buildinfo.SourceVersion()
	_, logger, cleanup := o11y.Init(context.Background(), serviceName, serviceVersion)
	defer cleanup()

	port := env.GetVariableOrDefault(logger, "SERVICE_PORT", "8080")

	app := application.NewApp()
	router := chi.NewRouter()
	api := api.NewApi(logger, router, app)

	metrics.AddHandlers(router)

	api.Start(port)
}
