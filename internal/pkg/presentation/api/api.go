package api

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
	"github.com/go-chi/chi/v5"
	"github.com/riandyrn/otelchi"
	"github.com/rs/cors"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"

	"github.com/diwise/integration-cip-gbg-watermeter/internal/pkg/application"
)

var tracer = otel.Tracer("integration-cip-gbg-watermeter/api")

type API interface {
	Start(port string) error
}

type api struct {
	log zerolog.Logger
	r   chi.Router
	app application.App
}

func (a *api) Start(port string) error {
	a.log.Info().Str("port", port).Msg("starting to listen for connections")

	return http.ListenAndServe(":"+port, a.r)
}

func NewApi(logger zerolog.Logger, r chi.Router, app application.App) API {
	a := newAPI(logger, r, app)

	return a
}

func newAPI(logger zerolog.Logger, r chi.Router, app application.App) *api {
	a := &api{
		log: logger,
		r:   r,
		app: app,
	}

	r.Use(cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowCredentials: true,
		Debug:            false,
	}).Handler)

	serviceName := "integration-cip-gbg-watermeter"

	r.Use(otelchi.Middleware(serviceName, otelchi.WithChiRoutes(r)))

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	r.Post("/v2/notify", notifyHandlerFunc(a))

	return a
}

func notifyHandlerFunc(a *api) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error

		ctx, span := tracer.Start(r.Context(), "notification-received")
		defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()

		_, ctx, log := o11y.AddTraceIDToLoggerAndStoreInContext(span, a.log, ctx)

		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()

		log.Info().Msg("attempting to process notification")

		n := application.Notification{}
		err = json.Unmarshal(body, &n)
		if err != nil {
			log.Error().Err(err).Msg("failed to handle message")

			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))

			return
		}

		err = a.app.NotificationReceived(ctx, n)
		if err != nil {
			log.Error().Err(err).Msg("failed to handle message")

			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))

			return
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}
}
