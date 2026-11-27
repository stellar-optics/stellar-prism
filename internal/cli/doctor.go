package cli

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

type doctorFlags struct {
	stateFile    string
	kafkaBrokers []string
	kafkaTopic   string
	timescaleDSN string
	pgDSN        string
	cloudBucket  string
	cloudURL     string
	esURL        string
}

func (df *doctorFlags) register(cmd *cobra.Command) {
	cmd.Flags().StringVar(&df.stateFile, "state-file", ".prism.state",
		"state directory or file path to check permissions and locking")
	cmd.Flags().StringSliceVar(&df.kafkaBrokers, "kafka-brokers", nil,
		"Kafka broker addresses (host:port) to test connectivity")
	cmd.Flags().StringVar(&df.kafkaTopic, "kafka-topic", "",
		"Kafka topic for sink connectivity")
	cmd.Flags().StringVar(&df.timescaleDSN, "timescale-dsn", "",
		"PostgreSQL/TimescaleDB connection string to test connectivity")
	cmd.Flags().StringVar(&df.pgDSN, "pg-dsn", "",
		"alias for --timescale-dsn")
	cmd.Flags().StringVar(&df.cloudBucket, "cloud-bucket", "",
		"S3 or GCS cloud storage bucket to test")
	cmd.Flags().StringVar(&df.cloudURL, "cloud-url", "",
		"custom cloud storage endpoint URL")
	cmd.Flags().StringVar(&df.esURL, "sink-es-url", "",
		"Elasticsearch or OpenSearch endpoint to test connectivity")
}

type checkStatus int

const (
	statusSuccess checkStatus = iota
	statusWarning
	statusFailure
)

type checkResult struct {
	status  checkStatus
	title   string
	message string
}

func formatCheck(res checkResult, colorEnabled bool) string {
	var symbol string
	switch res.status {
	case statusSuccess:
		if colorEnabled {
			symbol = "\033[32m[✓]\033[0m"
		} else {
			symbol = "[✓]"
		}
	case statusWarning:
		if colorEnabled {
			symbol = "\033[33m[!]\033[0m"
		} else {
			symbol = "[!]"
		}
	case statusFailure:
		if colorEnabled {
			symbol = "\033[31m[✗]\033[0m"
		} else {
			symbol = "[✗]"
		}
	}

	if res.message != "" {
		return fmt.Sprintf("%s %s: %s", symbol, res.title, res.message)
	}
	return fmt.Sprintf("%s %s", symbol, res.title)
}

// NewDoctorCommand builds the standalone doctor command with default global flags.
func NewDoctorCommand(stdout, stderr io.Writer) *cobra.Command {
	g := &globalFlags{}
	return newDoctorCmd(g, stdout, stderr)
}

func newDoctorCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var df doctorFlags

	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Inspect RPC connectivity, configured sinks, state permissions and system limits",
		Long: `doctor runs diagnostic health checks against Soroban RPC nodes, configured sinks,
state directories, and system file descriptor limits to verify operational readiness.

It exits 0 if all required checks pass, or 1 if any critical checks fail.`,
		Example: `  # Test default or environment configuration
  prism doctor

  # Test against a specific RPC URL and testnet
  prism doctor --rpc-url https://soroban-testnet.stellar.org --network testnet

  # Test specific config file and sinks
  prism doctor --config /path/to/.prism.yaml
  prism doctor --timescale-dsn "$DATABASE_URL" --state-file .prism.state`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			// Resolve configuration if --config was provided or discovered
			cfg := NewConfig()
			if g.configFile != "" {
				cfg.SetConfigFile(g.configFile)
			}
			if resolved, err := cfg.ResolveConfig(); err == nil && resolved != nil {
				if len(g.rpcURLs) == 0 {
					if urls := resolved.RPCURLsFromConfig(nil, nil); len(urls) > 0 {
						g.rpcURLs = urls
					}
				}
				if df.timescaleDSN == "" {
					if v := resolved.Viper.GetString("timescale-dsn"); v != "" {
						df.timescaleDSN = v
					} else if v := resolved.Viper.GetString("pg-dsn"); v != "" {
						df.timescaleDSN = v
					}
				}
				if df.esURL == "" {
					if v := resolved.Viper.GetString("sink-es-url"); v != "" {
						df.esURL = v
					}
				}
				if len(df.kafkaBrokers) == 0 {
					if v := resolved.Viper.GetStringSlice("kafka-brokers"); len(v) > 0 {
						df.kafkaBrokers = v
					}
				}
				if df.cloudBucket == "" {
					if v := resolved.Viper.GetString("cloud-bucket"); v != "" {
						df.cloudBucket = v
					}
				}
			}

			// Also inspect environment defaults for DSN if empty
			if df.timescaleDSN == "" && df.pgDSN != "" {
				df.timescaleDSN = df.pgDSN
			}
			if df.timescaleDSN == "" {
				if envDSN := os.Getenv(envPGDSN); envDSN != "" {
					df.timescaleDSN = envDSN
				} else if envTS := os.Getenv("PRISM_TIMESCALE_DSN"); envTS != "" {
					df.timescaleDSN = envTS
				}
			}

			color, err := parseColorMode(g.color)
			if err != nil {
				return err
			}
			colorEnabled := color.enabled(stdout)

			var results []checkResult
			hasCriticalFailures := false

			// 1. RPC Connectivity Checks
			rpcResults := checkRPCConnectivity(ctx, g)
			for _, r := range rpcResults {
				results = append(results, r)
				if r.status == statusFailure {
					hasCriticalFailures = true
				}
			}

			// 2. Sinks Connectivity Checks
			sinkResults := checkSinksConnectivity(ctx, df)
			for _, r := range sinkResults {
				results = append(results, r)
				if r.status == statusFailure {
					hasCriticalFailures = true
				}
			}

			// 3. State Directory Permissions Checks
			stateResult := checkStatePermissions(df.stateFile)
			results = append(results, stateResult)
			if stateResult.status == statusFailure {
				hasCriticalFailures = true
			}

			// 4. System Limits Checks
			limitResult := checkSystemLimits()
			results = append(results, limitResult)
			if limitResult.status == statusFailure {
				hasCriticalFailures = true
			}

			// Output results
			for _, res := range results {
				fmt.Fprintln(stdout, formatCheck(res, colorEnabled))
			}

			fmt.Fprintln(stdout)
			if hasCriticalFailures {
				summary := "Doctor detected 1 or more critical issues."
				if colorEnabled {
					fmt.Fprintf(stdout, "\033[31m%s\033[0m\n", summary)
				} else {
					fmt.Fprintln(stdout, summary)
				}
				return &ExitCodeError{Code: 1, Message: summary}
			}

			summary := "All required checks passed."
			if colorEnabled {
				fmt.Fprintf(stdout, "\033[32m%s\033[0m\n", summary)
			} else {
				fmt.Fprintln(stdout, summary)
			}
			return nil
		},
	}

	df.register(cmd)
	return cmd
}

func checkRPCConnectivity(ctx context.Context, g *globalFlags) []checkResult {
	urls := rpc.ResolveURLs(g.rpcURLs)
	var results []checkResult

	for _, endpoint := range urls {
		title := fmt.Sprintf("RPC Node (%s)", endpoint)

		probeCtx, cancel := context.WithTimeout(ctx, g.timeout)
		client, err := newClient([]string{endpoint}, g.timeout)
		if err != nil {
			cancel()
			results = append(results, checkResult{
				status:  statusFailure,
				title:   title,
				message: fmt.Sprintf("failed to construct client: %v", err),
			})
			continue
		}

		var healthStatus string
		var latestLedger uint32
		var protocolVersion int
		var reportedNetwork string
		isHealthy := true

		// Probe getHealth
		if hc, ok := client.(rpc.HealthClient); ok {
			hResp, hErr := hc.GetHealth(probeCtx)
			if hErr != nil {
				isHealthy = false
				healthStatus = fmt.Sprintf("unhealthy (%v)", hErr)
			} else {
				healthStatus = hResp.Status
				latestLedger = hResp.LatestLedger
				if hResp.Status != rpc.HealthStatusHealthy {
					isHealthy = false
				}
			}
		}

		// Probe getNetwork
		if nc, ok := client.(rpc.NetworkClient); ok {
			nResp, nErr := nc.GetNetwork(probeCtx)
			if nErr != nil {
				if isHealthy {
					healthStatus += fmt.Sprintf(" (network error: %v)", nErr)
				}
			} else {
				protocolVersion = nResp.ProtocolVersion
				reportedNetwork = nResp.Passphrase
				if !g.skipNetworkCheck {
					expected, expErr := rpc.ExpectedPassphrase(g.network)
					if expErr == nil && reportedNetwork != expected {
						cancel()
						results = append(results, checkResult{
							status:  statusFailure,
							title:   title,
							message: fmt.Sprintf("network passphrase mismatch: expected %q, got %q", expected, reportedNetwork),
						})
						continue
					}
				}
			}
		}

		// If neither HealthClient nor NetworkClient, test GetLatestLedger
		if latestLedger == 0 {
			if lResp, lErr := client.GetLatestLedger(probeCtx); lErr == nil {
				latestLedger = lResp.Sequence
			}
		}

		cancel()

		if !isHealthy {
			results = append(results, checkResult{
				status:  statusFailure,
				title:   title,
				message: fmt.Sprintf("probe failed: %s", healthStatus),
			})
			continue
		}

		details := []string{"healthy"}
		if latestLedger > 0 {
			details = append(details, fmt.Sprintf("latest ledger: %d", latestLedger))
		}
		if protocolVersion > 0 {
			details = append(details, fmt.Sprintf("protocol: %d", protocolVersion))
		}
		if g.network != "" {
			details = append(details, fmt.Sprintf("network: %s", g.network))
		}

		results = append(results, checkResult{
			status:  statusSuccess,
			title:   title,
			message: strings.Join(details, ", "),
		})
	}

	return results
}

func checkSinksConnectivity(ctx context.Context, df doctorFlags) []checkResult {
	var results []checkResult
	configuredSinks := 0

	// Kafka Check
	if len(df.kafkaBrokers) > 0 || df.kafkaTopic != "" {
		configuredSinks++
		title := "Sink (Kafka)"
		if len(df.kafkaBrokers) == 0 {
			results = append(results, checkResult{
				status:  statusFailure,
				title:   title,
				message: "--kafka-topic configured without --kafka-brokers",
			})
		} else {
			allReachable := true
			var unreachable []string
			for _, broker := range df.kafkaBrokers {
				d := net.Dialer{Timeout: 3 * time.Second}
				conn, err := d.DialContext(ctx, "tcp", broker)
				if err != nil {
					allReachable = false
					unreachable = append(unreachable, fmt.Sprintf("%s (%v)", broker, err))
				} else {
					_ = conn.Close()
				}
			}
			if allReachable {
				results = append(results, checkResult{
					status:  statusSuccess,
					title:   title,
					message: fmt.Sprintf("%d broker(s) reachable", len(df.kafkaBrokers)),
				})
			} else {
				results = append(results, checkResult{
					status:  statusFailure,
					title:   title,
					message: fmt.Sprintf("cannot connect to brokers: %s", strings.Join(unreachable, ", ")),
				})
			}
		}
	}

	// PostgreSQL / TimescaleDB Check
	if df.timescaleDSN != "" {
		configuredSinks++
		title := "Sink (PostgreSQL/TimescaleDB)"
		db, err := sql.Open(pgDriverName, df.timescaleDSN)
		if err != nil {
			results = append(results, checkResult{
				status:  statusFailure,
				title:   title,
				message: fmt.Sprintf("failed to parse DSN: %v", err),
			})
		} else {
			defer db.Close()
			pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			if err := db.PingContext(pingCtx); err != nil {
				results = append(results, checkResult{
					status:  statusFailure,
					title:   title,
					message: fmt.Sprintf("ping failed: %v", err),
				})
			} else {
				results = append(results, checkResult{
					status:  statusSuccess,
					title:   title,
					message: "database ping successful",
				})
			}
		}
	}

	// Elasticsearch Check
	if df.esURL != "" {
		configuredSinks++
		title := "Sink (Elasticsearch)"
		client := &http.Client{Timeout: 3 * time.Second}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, df.esURL, nil)
		if err != nil {
			results = append(results, checkResult{
				status:  statusFailure,
				title:   title,
				message: fmt.Sprintf("invalid URL: %v", err),
			})
		} else {
			resp, err := client.Do(req)
			if err != nil {
				results = append(results, checkResult{
					status:  statusFailure,
					title:   title,
					message: fmt.Sprintf("endpoint unreachable: %v", err),
				})
			} else {
				_ = resp.Body.Close()
				if resp.StatusCode >= 200 && resp.StatusCode < 500 {
					results = append(results, checkResult{
						status:  statusSuccess,
						title:   title,
						message: fmt.Sprintf("endpoint reachable (HTTP %d)", resp.StatusCode),
					})
				} else {
					results = append(results, checkResult{
						status:  statusWarning,
						title:   title,
						message: fmt.Sprintf("unexpected HTTP response %d", resp.StatusCode),
					})
				}
			}
		}
	}

	// Cloud Storage Check (S3 / GCS)
	if df.cloudBucket != "" || df.cloudURL != "" {
		configuredSinks++
		title := "Sink (Cloud Storage)"
		if df.cloudURL != "" {
			client := &http.Client{Timeout: 3 * time.Second}
			req, err := http.NewRequestWithContext(ctx, http.MethodHead, df.cloudURL, nil)
			if err != nil {
				results = append(results, checkResult{
					status:  statusFailure,
					title:   title,
					message: fmt.Sprintf("invalid endpoint URL: %v", err),
				})
			} else {
				resp, err := client.Do(req)
				if err != nil {
					results = append(results, checkResult{
						status:  statusFailure,
						title:   title,
						message: fmt.Sprintf("endpoint unreachable: %v", err),
					})
				} else {
					_ = resp.Body.Close()
					results = append(results, checkResult{
						status:  statusSuccess,
						title:   title,
						message: fmt.Sprintf("endpoint reachable for bucket %q", df.cloudBucket),
					})
				}
			}
		} else {
			results = append(results, checkResult{
				status:  statusSuccess,
				title:   title,
				message: fmt.Sprintf("configured bucket %q", df.cloudBucket),
			})
		}
	}

	if configuredSinks == 0 {
		results = append(results, checkResult{
			status:  statusWarning,
			title:   "Sinks",
			message: "no external sinks configured (streaming to stdout)",
		})
	}

	return results
}

func checkStatePermissions(statePath string) checkResult {
	title := fmt.Sprintf("State Directory (%s)", statePath)
	targetDir := statePath
	if filepath.Ext(statePath) != "" {
		targetDir = filepath.Dir(statePath)
	}

	// 1. Ensure directory exists
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return checkResult{
			status:  statusFailure,
			title:   title,
			message: fmt.Sprintf("directory creation failed: %v", err),
		}
	}

	// 2. Read directory
	if _, err := os.ReadDir(targetDir); err != nil {
		return checkResult{
			status:  statusFailure,
			title:   title,
			message: fmt.Sprintf("directory not readable: %v", err),
		}
	}

	// 3. Write probe file
	probePath := filepath.Join(targetDir, fmt.Sprintf(".doctor_probe_%d", time.Now().UnixNano()))
	if err := os.WriteFile(probePath, []byte("probe"), 0o600); err != nil {
		return checkResult{
			status:  statusFailure,
			title:   title,
			message: fmt.Sprintf("directory not writable: %v", err),
		}
	}
	_ = os.Remove(probePath)

	// 4. File Lock check
	if err := checkDirectoryFileLock(targetDir); err != nil {
		return checkResult{
			status:  statusWarning,
			title:   title,
			message: fmt.Sprintf("readable, writable, but lock check warned: %v", err),
		}
	}

	return checkResult{
		status:  statusSuccess,
		title:   title,
		message: "readable, writable, file lock acquired",
	}
}

func checkSystemLimits() checkResult {
	title := "System Limits"
	msg, ok, err := checkSystemFileDescriptorLimits()
	if err != nil {
		return checkResult{
			status:  statusWarning,
			title:   title,
			message: fmt.Sprintf("unable to determine descriptor limits: %v", err),
		}
	}
	if !ok {
		return checkResult{
			status:  statusWarning,
			title:   title,
			message: msg,
		}
	}
	return checkResult{
		status:  statusSuccess,
		title:   title,
		message: msg,
	}
}
