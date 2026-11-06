package integration

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

const databaseEnv = "CATALOG_TEST_DATABASE_URL"

var schemaSequence atomic.Uint64

// Clock is the time source used by catalog integration scenarios. Service code
// can receive Now as a dependency so freshness boundaries stay deterministic.
type Clock interface {
	Now() time.Time
}

type FixedClock struct {
	current time.Time
}

func NewFixedClock(current time.Time) FixedClock {
	return FixedClock{current: current.UTC()}
}

func (clock FixedClock) Now() time.Time {
	return clock.current
}

// CatalogFixtures holds stable, contract-oriented identities and values shared
// by matching, review, tenant-isolation, and ranking integration scenarios.
type CatalogFixtures struct {
	OrganizationID        string
	ForeignOrganizationID string
	ManagerSubject        string
	EndUserSubject        string
	ForeignManagerSubject string
	Products              []ProductFixture
	Offers                []OfferFixture
	Now                   time.Time
}

type ProductFixture struct {
	ID, Name, GTIN, Brand, Model string
}

type OfferFixture struct {
	ID, OrganizationID, ProductID string
	Title, GTIN, Brand, Model     string
	PriceCents                    int64
	PaymentMethod, Availability   string
	ObservedAt                    time.Time
}

func NewCatalogFixtures(now time.Time) CatalogFixtures {
	now = now.UTC()
	organizationID := "10000000-0000-4000-8000-000000000001"
	foreignOrganizationID := "10000000-0000-4000-8000-000000000002"
	gtinProductID := "20000000-0000-4000-8000-000000000001"
	brandModelProductID := "20000000-0000-4000-8000-000000000002"
	similarTitleProductID := "20000000-0000-4000-8000-000000000003"
	foreignProductID := "20000000-0000-4000-8000-000000000004"
	return CatalogFixtures{
		OrganizationID:        organizationID,
		ForeignOrganizationID: foreignOrganizationID,
		ManagerSubject:        "manager-10000000-0000-4000-8000-000000000001",
		EndUserSubject:        "end-user-10000000-0000-4000-8000-000000000001",
		ForeignManagerSubject: "manager-10000000-0000-4000-8000-000000000002",
		Now:                   now,
		Products: []ProductFixture{
			{ID: gtinProductID, Name: "Acme AX-200 Display", GTIN: "7891234567895", Brand: "Acme", Model: "AX-200"},
			{ID: brandModelProductID, Name: "Northstar Router N-5", Brand: "Northstar", Model: "N-5"},
			{ID: similarTitleProductID, Name: "Acme AX-200 Display Case", Brand: "Acme", Model: "CASE-AX200"},
			{ID: foreignProductID, Name: "Foreign Acme AX-200 Display", GTIN: "7891234567895", Brand: "Acme", Model: "AX-200"},
		},
		Offers: []OfferFixture{
			{ID: "30000000-0000-4000-8000-000000000001", OrganizationID: organizationID, ProductID: gtinProductID, Title: "Acme AX-200 Display", GTIN: "7891234567895", Brand: "Acme", Model: "AX-200", PriceCents: 9000, PaymentMethod: "cash", Availability: "available", ObservedAt: now.Add(-time.Hour)},
			{ID: "30000000-0000-4000-8000-000000000002", OrganizationID: organizationID, Title: "Acme AX-200 Display", PriceCents: 9500, PaymentMethod: "cash", Availability: "available", ObservedAt: now.Add(-time.Hour)},
			{ID: "30000000-0000-4000-8000-000000000003", OrganizationID: organizationID, ProductID: brandModelProductID, Title: "Northstar N-5 Router", Brand: "Northstar", Model: "N-5", PriceCents: 8000, PaymentMethod: "cash", Availability: "available", ObservedAt: now.Add(-25 * time.Hour)},
			{ID: "30000000-0000-4000-8000-000000000004", OrganizationID: organizationID, ProductID: brandModelProductID, Title: "Northstar N-5 Router", Brand: "Northstar", Model: "N-5", PriceCents: 7000, PaymentMethod: "cash", Availability: "unavailable", ObservedAt: now.Add(-time.Hour)},
			{ID: "30000000-0000-4000-8000-000000000005", OrganizationID: organizationID, ProductID: brandModelProductID, Title: "Northstar N-5 Router", Brand: "Northstar", Model: "N-5", PriceCents: 6000, PaymentMethod: "cash", Availability: "unknown", ObservedAt: now.Add(-time.Hour)},
			{ID: "30000000-0000-4000-8000-000000000006", OrganizationID: foreignOrganizationID, ProductID: foreignProductID, Title: "Acme AX-200 Display", GTIN: "7891234567895", PriceCents: 9000, PaymentMethod: "cash", Availability: "available", ObservedAt: now.Add(-time.Hour)},
		},
	}
}

// openTestDB creates a private schema, applies every service-owned up
// migration, and removes the schema at test cleanup. The configured database
// must be disposable; no data in its public schema is read or modified.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dsn := os.Getenv(databaseEnv)
	if dsn == "" {
		if os.Getenv("CI") != "" {
			t.Fatalf("%s is required in CI; configure the catalog PostgreSQL service", databaseEnv)
		}
		t.Skipf("set %s to a disposable PostgreSQL database to run catalog integration scenarios", databaseEnv)
	}

	adminDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL test database: %v", err)
	}
	t.Cleanup(func() { _ = adminDB.Close() })

	schema := fmt.Sprintf("catalog_test_%d_%d", os.Getpid(), schemaSequence.Add(1))
	if _, err := adminDB.Exec(`CREATE SCHEMA "` + schema + `"`); err != nil {
		t.Fatalf("create isolated test schema: %v", err)
	}
	t.Cleanup(func() {
		if _, err := adminDB.Exec(`DROP SCHEMA "` + schema + `" CASCADE`); err != nil {
			t.Errorf("drop isolated test schema %s: %v", schema, err)
		}
	})

	isolatedURL, err := withSearchPath(dsn, schema)
	if err != nil {
		t.Fatalf("configure isolated PostgreSQL schema: %v", err)
	}
	db, err := sql.Open("postgres", isolatedURL)
	if err != nil {
		t.Fatalf("open isolated PostgreSQL schema: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	if err := applyMigrations(t, db); err != nil {
		t.Fatalf("apply catalog-comparison migrations: %v", err)
	}
	return db
}

func TestIntegrationDatabaseSetup(t *testing.T) {
	db := openTestDB(t)
	var schema string
	if err := db.QueryRow(`SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatalf("read isolated test schema: %v", err)
	}
	if !strings.HasPrefix(schema, "catalog_test_") {
		t.Fatalf("integration test is not isolated in a private schema: %q", schema)
	}
	var outboxExists bool
	if err := db.QueryRow(`SELECT to_regclass('message_outbox') IS NOT NULL`).Scan(&outboxExists); err != nil {
		t.Fatalf("check applied catalog migration: %v", err)
	}
	if !outboxExists {
		t.Fatal("catalog message outbox migration was not applied")
	}
}

func withSearchPath(dsn, schema string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		return "", fmt.Errorf("%s must be a PostgreSQL URL", databaseEnv)
	}
	query := parsed.Query()
	options := strings.TrimSpace(query.Get("options"))
	query.Set("options", strings.TrimSpace(options+" -c search_path="+schema))
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func applyMigrations(t *testing.T, db *sql.DB) error {
	t.Helper()

	migrations, err := filepath.Glob(filepath.Join("..", "..", "migrations", "*.up.sql"))
	if err != nil {
		return err
	}
	sort.Strings(migrations)
	for _, migration := range migrations {
		contents, err := os.ReadFile(migration)
		if err != nil {
			return fmt.Errorf("read %s: %w", migration, err)
		}
		if _, err := db.Exec(string(contents)); err != nil {
			return fmt.Errorf("execute %s: %w", migration, err)
		}
	}
	return nil
}
