package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Slach/clickhouse-timeline/pkg/config"
	"github.com/rs/zerolog/log"
)

type Client struct {
	config  config.Context
	db      *sql.DB
	version string
}

func NewClient(cfg config.Context, version string) *Client {
	return &Client{
		config:  cfg,
		version: version,
	}
}

func (c *Client) GetVersion() (string, error) {
	if c.db == nil {
		if err := c.connect(); err != nil {
			return "", err
		}
	}

	var version string
	err := c.db.QueryRow("SELECT version()").Scan(&version)
	if err != nil {
		log.Error().Err(err).Msg("failed to get ClickHouse version")
		return "", err
	}

	return version, nil
}

func (c *Client) connect() error {
	var tlsConfig *tls.Config

	// Enable TLS if secure is true or if certificates are provided
	if c.config.Secure || (c.config.TLSCert != "" && c.config.TLSKey != "") || c.config.TLSCa != "" || c.config.TLSVerify {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: !c.config.TLSVerify,
		}

		// Load client certificates if provided
		if c.config.TLSCert != "" && c.config.TLSKey != "" {
			cert, err := tls.LoadX509KeyPair(c.config.TLSCert, c.config.TLSKey)
			if err != nil {
				return fmt.Errorf("failed to load client certificate: %v", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		// Load CA certificate if provided
		if c.config.TLSCa != "" {
			caCert, err := os.ReadFile(c.config.TLSCa)
			if err != nil {
				return fmt.Errorf("failed to read CA certificate: %v", err)
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tlsConfig.RootCAs = caCertPool
		}
	}

	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", c.config.Host, c.config.Port)},
		Auth: clickhouse.Auth{
			Database: c.config.Database,
			Username: c.config.Username,
			Password: c.config.Password,
		},
		TLS: tlsConfig,
	}
	options.ClientInfo.Products = append(options.ClientInfo.Products, struct{ Name, Version string }{
		"clickhouse-timeline",
		c.version,
	})

	options.Protocol = clickhouse.Native
	if c.config.Protocol == "http" {
		options.Protocol = clickhouse.HTTP
	}

	db := clickhouse.OpenDB(options)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return err
	}

	c.db = db
	return nil
}

func (c *Client) Query(query string, args ...interface{}) (*sql.Rows, error) {
	log.Info().Msg(query)
	if len(args) > 0 {
		log.Info().Msgf("args=%#v", args)
	}
	rows, err := c.db.QueryContext(context.Background(), query, args...)
	if err != nil {
		log.Error().Stack().Err(err).Str("query", query).Interface("args", args).Msg("Query failed")
		return nil, err
	}
	return rows, nil
}

func (c *Client) QueryRow(query string, args ...interface{}) *sql.Row {
	log.Info().Msg(query)
	if len(args) > 0 {
		log.Info().Msgf("args=%#v", args)
	}
	row := c.db.QueryRowContext(context.Background(), query, args...)
	if row.Err() != nil {
		log.Error().Stack().Err(row.Err()).Str("query", query).Interface("args", args).Msg("QueryRow failed")
		return nil
	}
	return row
}

func (c *Client) Exec(query string, args ...interface{}) (sql.Result, error) {
	log.Info().Msg(query)
	if len(args) > 0 {
		log.Info().Msgf("args=%#v", args)
	}
	res, err := c.db.ExecContext(context.Background(), query, args...)
	if err != nil {
		log.Error().Stack().Err(err).Str("query", query).Interface("args", args).Msg("Exec failed")
		return nil, err
	}
	return res, nil
}

func (c *Client) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}
