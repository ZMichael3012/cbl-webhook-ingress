// main starts the CBL webhook-ingress HTTP service:
// provider webhook intake with HMAC verification and publication via outbox.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/ZMichael3012/cbl-kit/ginmw"
	"github.com/ZMichael3012/cbl-kit/outbox"
	"github.com/ZMichael3012/cbl-kit/outbox/kafkapub"
	"github.com/ZMichael3012/cbl-kit/storagex"

	"github.com/cbl/cbl-webhook-ingress/internal/domain"
	"github.com/cbl/cbl-webhook-ingress/internal/transport"
	"github.com/cbl/cbl-webhook-ingress/internal/usecase"
)

// main brings up the webhook intake HTTP server, applies migrations
// and starts the outbox publication relay.
func main() {
	log, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer func() { _ = log.Sync() }()

	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		log.Error("PG_DSN is required")
		return
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Error("connect postgres failed", zap.Error(err))
		return
	}

	providers, err := domain.ParseProviders(os.Getenv("WEBHOOK_PROVIDERS"), os.Getenv)
	if err != nil {
		log.Error("parse WEBHOOK_PROVIDERS failed", zap.Error(err))
		return
	}
	for _, name := range domain.MissingAllowlist(providers) {
		log.Warn("provider ip allowlist is empty: any source address is accepted",
			zap.String("provider", name))
	}
	writeTimeout, err := domain.ParseWriteTimeout(os.Getenv("WEBHOOK_WRITE_TIMEOUT"))
	if err != nil {
		log.Error("parse WEBHOOK_WRITE_TIMEOUT failed", zap.Error(err))
		return
	}
	refreshInterval, err := domain.ParseAllowlistRefresh(os.Getenv("WEBHOOK_ALLOWLIST_REFRESH"))
	if err != nil {
		log.Error("parse WEBHOOK_ALLOWLIST_REFRESH failed", zap.Error(err))
		return
	}
	dynamic := domain.NewDynamicAllowlist()
	sources := allowlistSources(providers)
	if len(sources) > 0 && refreshInterval == 0 {
		// URL-only providers stay fail-closed without refreshes: that would
		// silently break their intake, so it is a startup error, not a warning.
		log.Error("dynamic allowlists are configured but WEBHOOK_ALLOWLIST_REFRESH is 0")
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	migrationsDir := getenv("MIGRATIONS_DIR", "./migrations")
	if err := storagex.Migrate(ctx, db, migrationsDir); err != nil {
		log.Error("apply migrations failed", zap.String("dir", migrationsDir), zap.Error(err))
		return
	}

	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		publisher, pubErr := kafkapub.New(strings.Split(brokers, ","))
		if pubErr != nil {
			log.Error("kafka publisher failed", zap.Error(pubErr))
			return
		}
		defer publisher.Close()
		relay := outbox.NewRelay(db, publisher, log)
		go func() {
			if runErr := relay.Run(ctx); runErr != nil && !errors.Is(runErr, context.Canceled) {
				log.Error("outbox relay stopped", zap.Error(runErr))
			}
		}()
	} else {
		log.Warn("KAFKA_BROKERS is empty: outbox relay disabled, events stay pending")
	}

	if len(sources) > 0 && refreshInterval > 0 {
		refresher := usecase.NewAllowlistRefresher(sources, dynamic, refreshInterval, log)
		// The first load runs before the intake starts: a URL-only provider is
		// fail-closed until the dynamic set is known (17.4).
		if refreshErr := refresher.Refresh(ctx); refreshErr != nil {
			log.Warn("initial dynamic allowlist refresh failed; the affected intake stays fail-closed until the first successful load",
				zap.Error(refreshErr))
		}
		go refresher.Run(ctx)
	}

	handler := usecase.NewHandler(db, "cbl-webhook-ingress", log)
	server := transport.NewServer(providers, handler, log,
		transport.WithWriteTimeout(writeTimeout),
		transport.WithDynamicAllowlist(dynamic),
	)

	httpAddr := getenv("HTTP_ADDR", ":8060")
	metricsAddr := getenv("METRICS_ADDR", ":9260")

	router := gin.New()
	if err := router.SetTrustedProxies(nil); err != nil {
		log.Error("configure trusted proxies failed", zap.Error(err))
		return
	}
	router.Use(ginmw.RequestID())
	router.Use(ginmw.AccessLog(log))
	router.Use(ginmw.Recovery(log))
	router.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	router.GET("/readyz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	server.Register(router)

	httpSrv := &http.Server{
		Addr:              httpAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	metricsSrv := &http.Server{Addr: metricsAddr, Handler: metricsHandler(), ReadHeaderTimeout: 5 * time.Second}

	go func() {
		log.Info("http server started", zap.String("addr", httpAddr))
		if serveErr := httpSrv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Error("http server failed", zap.Error(serveErr))
			stop()
		}
	}()
	go func() {
		log.Info("metrics server started", zap.String("addr", metricsAddr))
		if serveErr := metricsSrv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Error("metrics server failed", zap.Error(serveErr))
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutdown started")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if shutdownErr := httpSrv.Shutdown(shutdownCtx); shutdownErr != nil {
		log.Error("http shutdown failed", zap.Error(shutdownErr))
	}
	if shutdownErr := metricsSrv.Shutdown(shutdownCtx); shutdownErr != nil {
		log.Error("metrics shutdown failed", zap.Error(shutdownErr))
	}
	log.Info("shutdown finished")
}

// allowlistSources collects the dynamic allowlist URLs per provider.
func allowlistSources(providers map[string]domain.ProviderConfig) map[string]string {
	sources := make(map[string]string)
	for name, config := range providers {
		if url := strings.TrimSpace(config.AllowlistURL); url != "" {
			sources[name] = url
		}
	}
	return sources
}

// metricsHandler serves the metrics placeholder until task 0.7.
func metricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintln(w, "# metrics endpoint placeholder (task 0.7)")
	})
	return mux
}

// getenv returns the environment variable value or the fallback value.
func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
