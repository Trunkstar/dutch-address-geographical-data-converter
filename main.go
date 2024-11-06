package main

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type Feature struct {
	FeatureId           string         `db:"FeatureId" json:"featureId"`
	StreetName          sql.NullString `db:"StreetName" json:"streetName"`
	HouseNumber         sql.NullString `db:"HouseNumber" json:"houseNumber"`
	HouseNumberAddition sql.NullString `db:"HouseNumberAddition" json:"houseNumberAddition"`
	PostalCode          sql.NullString `db:"PostalCode" json:"postalCode"`
	CityName            sql.NullString `db:"CityName" json:"cityName"`
	Latitude            float64        `db:"Latitude" json:"latitude"`
	Longitude           float64        `db:"Longitude" json:"longitude"`
}

func main() {
	db1 := setupDB("bag-light.gpkg") // Set up the source database connection
	defer db1.Close()

	// Initialize the destination database and create table
	db2 := initializeDestDB("converted.db")
	defer db2.Close()

	// Get total records count from source database
	var totalRecords int
	err := db1.Get(&totalRecords, "SELECT COUNT(feature_id) FROM verblijfsobject")
	if err != nil {
		log.Fatal("Error getting total records count:", err)
	}

	batchSize := 500 // Number of records to fetch in each batch
	numWorkers := 10 // Number of concurrent workers
	start := 0

	var wg sync.WaitGroup
	dataChan := make(chan []Feature, numWorkers) // Buffered channel

	// Add progress tracking
	processedRecords := 0
	var progressMutex sync.Mutex

	go func() {
		for features := range dataChan {
			progressMutex.Lock()
			processedRecords += len(features)
			progress := float64(processedRecords) / float64(totalRecords) * 100
			fmt.Printf("\rProgress: %.2f%% (%d/%d records)", progress, processedRecords, totalRecords)
			progressMutex.Unlock()

			insertFeatures(db2, features)
		}
		fmt.Println() // Print newline when done
	}()

	// Dispatch workers
	for ; start < totalRecords; start += batchSize * numWorkers {
		for i := 0; i < numWorkers && (start+i*batchSize) < totalRecords; i++ {
			wg.Add(1)
			go fetchData(db1, &wg, start+i*batchSize, batchSize, dataChan)
		}
		wg.Wait() // Wait for all goroutines in this batch to finish
	}

	close(dataChan) // Close channel after all fetch routines are done
}

func setupDB(dbFile string) *sqlx.DB {
	db, err := sqlx.Connect("sqlite3", dbFile)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func initializeDestDB(dbFile string) *sqlx.DB {
	db, err := sqlx.Connect("sqlite3", dbFile)
	if err != nil {
		log.Fatal(err)
	}

	// Add SQLite optimizations
	_, err = db.Exec(`
		PRAGMA journal_mode = WAL;
		PRAGMA synchronous = NORMAL;
		PRAGMA temp_store = MEMORY;
		PRAGMA mmap_size = 30000000000;
		PRAGMA page_size = 4096;
		PRAGMA cache_size = -2000000;
	`)
	if err != nil {
		log.Fatal("Error setting PRAGMA:", err)
	}

	// Create the Features table with optimized indexes
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS Features (
			FeatureId TEXT PRIMARY KEY,
			StreetName TEXT,
			HouseNumber TEXT,
			HouseNumberAddition TEXT,
			PostalCode TEXT,
			CityName TEXT,
			Latitude REAL,
			Longitude REAL
		);
		
		-- Composite index for address lookups
		CREATE INDEX IF NOT EXISTS idx_address_lookup ON Features(
			PostalCode, 
			HouseNumber, 
			HouseNumberAddition
		);
		
		-- Individual indexes for flexible querying
		CREATE INDEX IF NOT EXISTS idx_postalcode ON Features(PostalCode);
		CREATE INDEX IF NOT EXISTS idx_housenumber ON Features(HouseNumber);
		CREATE INDEX IF NOT EXISTS idx_cityname ON Features(CityName);
	`)
	if err != nil {
		log.Fatal("Error creating table:", err)
	}

	return db
}

func fetchData(db *sqlx.DB, wg *sync.WaitGroup, start, count int, dataChan chan<- []Feature) {
	defer wg.Done()
	features := []Feature{}

	// Combine both queries into a single query with JOIN
	query := `
		SELECT 
			v.feature_id AS FeatureId,
			v.openbare_ruimte_naam AS StreetName, 
			v.huisnummer AS HouseNumber, 
			v.toevoeging AS HouseNumberAddition, 
			v.postcode AS PostalCode, 
			v.woonplaats_naam AS CityName,
			r.maxx AS Latitude,
			r.maxy AS Longitude
		FROM verblijfsobject v
		LEFT JOIN rtree_verblijfsobject_geom r ON v.feature_id = r.id
		LIMIT ? OFFSET ?`

	err := db.Select(&features, query, count, start)
	if err != nil {
		log.Printf("Error fetching data at offset %d: %v", start, err)
		return
	}

	// Convert coordinates in batch
	for i := range features {
		lat, lon := rdToWGS84(features[i].Latitude, features[i].Longitude)
		features[i].Latitude = lat
		features[i].Longitude = lon
	}

	if len(features) > 0 {
		dataChan <- features
	}
}

func insertFeatures(db *sqlx.DB, features []Feature) {
	// Build batch insert query
	valueStrings := make([]string, 0, len(features))
	valueArgs := make([]interface{}, 0, len(features)*8)

	for _, f := range features {
		valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?)")
		valueArgs = append(valueArgs,
			f.FeatureId,
			f.StreetName,
			f.HouseNumber,
			f.HouseNumberAddition,
			f.PostalCode,
			f.CityName,
			f.Latitude,
			f.Longitude)
	}

	stmt := fmt.Sprintf(`INSERT INTO Features (
		FeatureId, StreetName, HouseNumber, HouseNumberAddition, 
		PostalCode, CityName, Latitude, Longitude
	) VALUES %s`, strings.Join(valueStrings, ","))

	// Execute batch insert within transaction
	tx, err := db.Begin()
	if err != nil {
		log.Printf("Error starting transaction: %v", err)
		return
	}

	_, err = tx.Exec(stmt, valueArgs...)
	if err != nil {
		tx.Rollback()
		log.Printf("Error inserting batch: %v", err)
		return
	}

	if err := tx.Commit(); err != nil {
		log.Printf("Error committing transaction: %v", err)
	}
}

// Conversion constants
const (
	X0      = 155000.0
	Y0      = 463000.0
	Phi0    = 52.15517440
	Lambda0 = 5.38720621
)

// rdToWGS84 converts RD coordinates (x, y) to WGS84 (latitude, longitude).
func rdToWGS84(x, y float64) (float64, float64) {
	// Constants for the transformation equations
	dX := (x - X0) * 1e-5
	dY := (y - Y0) * 1e-5

	// Calculate latitude and longitude in radians
	phi := (3235.65389*dY + -32.58297*math.Pow(dX, 2) + -0.24750*math.Pow(dY, 2) +
		-0.84978*math.Pow(dX, 2)*dY + -0.06550*math.Pow(dY, 3) +
		-0.01709*math.Pow(dX, 2)*math.Pow(dY, 2) + -0.00738*dX +
		0.00530*math.Pow(dX, 4) + -0.00039*math.Pow(dX, 2)*math.Pow(dY, 3) +
		0.00033*math.Pow(dX, 4)*dY + -0.00012*dX*dY) / 3600

	lambda := (5260.52916*dX + 105.94684*dX*dY + 2.45656*math.Pow(dX, 3) +
		-0.81885*math.Pow(dX, 3)*dY + 0.05594*math.Pow(dX, 3)*math.Pow(dY, 2) +
		-0.05607*math.Pow(dX, 5) + 0.01199*dY + -0.00256*math.Pow(dX, 5)*dY +
		0.00128*dX*math.Pow(dY, 3) + 0.00022*math.Pow(dY, 5) + -0.00022*math.Pow(dX, 3)*math.Pow(dY, 4)) / 3600

	// Convert radians to degrees and adjust by base latitude and longitude
	phi = Phi0 + phi
	lambda = Lambda0 + lambda

	return phi, lambda
}
