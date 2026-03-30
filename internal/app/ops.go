package app

import (
	"context"
	"runtime"
	"time"
)

type BuildInfo struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildTime string `json:"build_time,omitempty"`
	GoVersion string `json:"go_version"`
}

type HealthReport struct {
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
	StartedAt time.Time `json:"started_at"`
	UptimeSec int64     `json:"uptime_sec"`
}

type ReadinessReport struct {
	Status    string         `json:"status"`
	Timestamp time.Time      `json:"timestamp"`
	Checks    map[string]any `json:"checks"`
}

func NewBuildInfo(version, commit, buildTime string) BuildInfo {
	return BuildInfo{
		Version:   fallbackString(version, "dev"),
		Commit:    fallbackString(commit, "unknown"),
		BuildTime: buildTime,
		GoVersion: runtime.Version(),
	}
}

func (s *Service) HealthReport() HealthReport {
	now := time.Now().UTC()
	return HealthReport{
		Status:    "ok",
		Timestamp: now,
		StartedAt: s.startedAt,
		UptimeSec: int64(now.Sub(s.startedAt).Seconds()),
	}
}

func (s *Service) ReadinessReport(ctx context.Context) ReadinessReport {
	now := time.Now().UTC()
	report := ReadinessReport{
		Status:    "ready",
		Timestamp: now,
		Checks: map[string]any{
			"store": map[string]any{
				"status": "ready",
			},
			"ntrip_listener": map[string]any{
				"status": "not_ready",
			},
		},
	}
	if err := s.CheckStore(ctx); err != nil {
		report.Status = "not_ready"
		report.Checks["store"] = map[string]any{
			"status": "not_ready",
			"error":  err.Error(),
		}
	}
	if s.IsNTRIPReady() {
		report.Checks["ntrip_listener"] = map[string]any{
			"status": "ready",
		}
	} else {
		report.Status = "not_ready"
	}
	return report
}

func fallbackString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
