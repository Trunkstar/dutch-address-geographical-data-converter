package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math"
	"runtime"
	"strings"
	"sync"
	"time"

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

// Add this struct for timing statistics
type TimingStats struct {
	fetchTime   time.Duration
	convertTime time.Duration
	insertTime  time.Duration
	batchCount  int
	recordCount int
	mutex       sync.Mutex
}

func newTimingStats() *TimingStats {
	return &TimingStats{}
}

func (ts *TimingStats) addTiming(fetch, convert, insert time.Duration, records int) {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()
	ts.fetchTime += fetch
	ts.convertTime += convert
	ts.insertTime += insert
	ts.batchCount++
	ts.recordCount += records
}

func (ts *TimingStats) logAverages() {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()
	if ts.batchCount == 0 {
		return
	}

	avgFetch := ts.fetchTime.Seconds() / float64(ts.batchCount)
	avgConvert := ts.convertTime.Seconds() / float64(ts.batchCount)
	avgInsert := ts.insertTime.Seconds() / float64(ts.batchCount)

	log.Printf("\nTiming Statistics (per batch):")
	log.Printf("  Avg Fetch:    %.3fs", avgFetch)
	log.Printf("  Avg Convert:  %.3fs", avgConvert)
	log.Printf("  Avg Insert:   %.3fs", avgInsert)
	log.Printf("  Total Batches: %d", ts.batchCount)
	log.Printf("  Records/Batch: %.0f", float64(ts.recordCount)/float64(ts.batchCount))
}

func main() {
	// Add command line flag
	var optimize bool
	flag.BoolVar(&optimize, "optimize", false, "Run optimization and indexing on existing database")
	flag.Parse()

	if optimize {
		// Run optimization on existing database
		db := connectToExistingDB("converted.db")
		defer db.Close()

		log.Println("Running optimization and indexing...")
		createIndexes(db)
		log.Println("Optimization completed successfully!")
		return
	}

	db1 := connectSourceDB("bag-light.gpkg")
	defer db1.Close()

	// Optimize source database
	_, err := db1.Exec(`
		PRAGMA journal_mode = WAL;
		PRAGMA synchronous = NORMAL;
		PRAGMA cache_size = -2000000;
		PRAGMA mmap_size = 30000000000;
	`)
	if err != nil {
		log.Fatal("Error setting source DB PRAGMA:", err)
	}

	db2 := initializeDestDB("converted.db")
	defer db2.Close()

	var totalRecords int
	err = db1.Get(&totalRecords, "SELECT COUNT(feature_id) FROM verblijfsobject")
	if err != nil {
		log.Fatal("Error getting total records count:", err)
	}

	// Get system info for tuning
	numCPU := runtime.NumCPU()
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	availableMemoryGB := float64(memStats.Sys) / 1024 / 1024 / 1024

	// Dynamic parameters calculation
	numWorkers := calculateOptimalWorkers(numCPU)
	batchSize := calculateOptimalBatchSize(availableMemoryGB)

	// Initialize performance monitoring
	perfMonitor := newPerformanceMonitor(totalRecords)

	var wg sync.WaitGroup
	dataChan := make(chan []Feature, numWorkers*2) // Double buffer size

	timingStats := newTimingStats()

	// Process data from channel
	go func() {
		for features := range dataChan {
			start := time.Now()
			insertFeatures(db2, features)
			insertTime := time.Since(start)

			perfMonitor.updateMetrics(len(features))
			timingStats.addTiming(0, 0, insertTime, len(features)) // Only tracking insert time here
		}
		perfMonitor.printFinalStats()
		timingStats.logAverages()
	}()

	// Process records in batches
	for start := 0; start < totalRecords; start += batchSize * numWorkers {
		for i := 0; i < numWorkers && (start+i*batchSize) < totalRecords; i++ {
			wg.Add(1)
			go fetchData(db1, &wg, start+i*batchSize, batchSize, dataChan)
		}
		wg.Wait() // Wait for all goroutines in this batch to finish
	}

	close(dataChan) // Close channel after all fetch routines are done

	log.Println("\nConversion completed successfully!")
	log.Println("Run with -optimize flag to create indexes and optimize the database")
}

func connectSourceDB(dbFile string) *sqlx.DB {
	db, err := sqlx.Connect("sqlite3", dbFile)
	if err != nil {
		log.Fatal(err)
	}

	// Enhanced source database optimizations
	_, err = db.Exec(`
			PRAGMA journal_mode = WAL;
			PRAGMA synchronous = NORMAL;
			PRAGMA cache_size = -2000000;
			PRAGMA mmap_size = 30000000000;
			PRAGMA temp_store = MEMORY;
			PRAGMA page_size = 4096;
			
			-- Create an index to help with the JOIN
			CREATE INDEX IF NOT EXISTS idx_verblijfsobject_feature_id 
			ON verblijfsobject(feature_id);
		`)
	if err != nil {
		log.Fatal("Error setting source DB PRAGMA:", err)
	}

	return db
}

func initializeDestDB(dbFile string) *sqlx.DB {
	db, err := sqlx.Connect("sqlite3", dbFile)
	if err != nil {
		log.Fatal(err)
	}

	// Enhanced SQLite optimizations for write-heavy workload
	_, err = db.Exec(`
		PRAGMA journal_mode = OFF;           -- Disable journaling since we can recreate the db
		PRAGMA synchronous = OFF;            -- Disable fsync
		PRAGMA locking_mode = EXCLUSIVE;     -- Keep db locked
		PRAGMA temp_store = MEMORY;          -- Use memory for temp storage
		PRAGMA page_size = 4096;             -- Optimal page size for most filesystems
		PRAGMA cache_size = -2000000;        -- Use 2GB of memory for DB cache
		PRAGMA mmap_size = 30000000000;      -- Memory map up to 30GB
		PRAGMA threads = 4;                  -- Use multiple background threads
	`)
	if err != nil {
		log.Fatal("Error setting PRAGMA:", err)
	}

	// Create the Features table but delay index creation
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
		)
	`)
	if err != nil {
		log.Fatal("Error creating table:", err)
	}

	return db
}

func fetchData(db *sqlx.DB, wg *sync.WaitGroup, start, count int, dataChan chan<- []Feature) {
	defer wg.Done()
	features := []Feature{}

	// Modified query to be more efficient with large offsets
	query := `
		WITH page AS (
			SELECT feature_id
			FROM verblijfsobject
			LIMIT ? OFFSET ?
		)
		SELECT 
			v.feature_id AS FeatureId,
			v.openbare_ruimte_naam AS StreetName, 
			v.huisnummer AS HouseNumber, 
			v.toevoeging AS HouseNumberAddition, 
			v.postcode AS PostalCode, 
			v.woonplaats_naam AS CityName,
			r.maxx AS Latitude,
			r.maxy AS Longitude
		FROM page 
		JOIN verblijfsobject v ON v.feature_id = page.feature_id
		LEFT JOIN rtree_verblijfsobject_geom r ON v.feature_id = r.id`

	err := db.Select(&features, query, count, start)
	if err != nil {
		// Fallback to simpler query if CTE isn't supported
		fallbackQuery := `
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
			ORDER BY v.feature_id
			LIMIT ? OFFSET ?`

		err = db.Select(&features, fallbackQuery, count, start)
		if err != nil {
			log.Printf("Error fetching data at offset %d: %v", start, err)
			return
		}
	}

	// Convert coordinates in parallel for large batches
	if len(features) > 1000 {
		var wg sync.WaitGroup
		chunks := len(features)/4 + 1
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func(start, end int) {
				defer wg.Done()
				for j := start; j < end && j < len(features); j++ {
					lat, lon := rdToWGS84(features[j].Latitude, features[j].Longitude)
					features[j].Latitude = lat
					features[j].Longitude = lon
				}
			}(i*chunks, (i+1)*chunks)
		}
		wg.Wait()
	} else {
		for i := range features {
			lat, lon := rdToWGS84(features[i].Latitude, features[i].Longitude)
			features[i].Latitude = lat
			features[i].Longitude = lon
		}
	}

	if len(features) > 0 {
		dataChan <- features
	}
}

func insertFeatures(db *sqlx.DB, features []Feature) {
	if len(features) == 0 {
		return
	}

	// Use prepared statement for better performance
	stmt := `INSERT INTO Features (
		FeatureId, StreetName, HouseNumber, HouseNumberAddition, 
		PostalCode, CityName, Latitude, Longitude
	) VALUES `

	// Pre-allocate with exact sizes
	placeholders := make([]string, len(features))
	vals := make([]interface{}, 0, len(features)*8)

	for i, f := range features {
		placeholders[i] = "(?, ?, ?, ?, ?, ?, ?, ?)"
		vals = append(vals,
			f.FeatureId,
			f.StreetName,
			f.HouseNumber,
			f.HouseNumberAddition,
			f.PostalCode,
			f.CityName,
			f.Latitude,
			f.Longitude)
	}

	stmt += strings.Join(placeholders, ",")

	// Simplified retry logic with exponential backoff
	var lastErr error
	maxRetries := 5
	for retries := 0; retries < maxRetries; retries++ {
		backoff := time.Duration(math.Pow(2, float64(retries))) * time.Second

		tx, err := db.Begin()
		if err != nil {
			lastErr = err
			time.Sleep(backoff)
			continue
		}

		_, err = tx.Exec(stmt, vals...)
		if err != nil {
			tx.Rollback()
			lastErr = err
			if strings.Contains(err.Error(), "database is locked") {
				time.Sleep(backoff)
				continue
			}
			break
		}

		if err := tx.Commit(); err != nil {
			lastErr = err
			time.Sleep(backoff)
			continue
		}

		return
	}

	if lastErr != nil {
		log.Printf("Failed to insert batch: %v", lastErr)
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

	// Round to 8 decimal places
	phi = math.Round(phi*100000000) / 100000000
	lambda = math.Round(lambda*100000000) / 100000000

	return phi, lambda
}

type PerformanceMonitor struct {
	startTime        time.Time
	totalRecords     int
	processedRecords int
	rateHistory      []float64
	lastUpdate       time.Time
	mutex            sync.Mutex
}

func newPerformanceMonitor(totalRecords int) *PerformanceMonitor {
	return &PerformanceMonitor{
		startTime:    time.Now(),
		totalRecords: totalRecords,
		rateHistory:  make([]float64, 0, 100),
		lastUpdate:   time.Now(),
	}
}

func (pm *PerformanceMonitor) updateMetrics(recordsProcessed int) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	currentTime := time.Now()
	pm.processedRecords += recordsProcessed

	// Update metrics every 5 seconds
	if currentTime.Sub(pm.lastUpdate).Seconds() > 5 {
		// Calculate rate based on total progress
		totalElapsed := currentTime.Sub(pm.startTime).Seconds()
		currentRate := float64(pm.processedRecords) / totalElapsed

		// Update rate history
		pm.rateHistory = append(pm.rateHistory, currentRate)
		if len(pm.rateHistory) > 10 {
			pm.rateHistory = pm.rateHistory[1:]
		}

		// Calculate moving average rate for smoother estimates
		avgRate := pm.getAverageRate()
		remainingRecords := pm.totalRecords - pm.processedRecords
		eta := time.Duration(float64(remainingRecords)/avgRate) * time.Second
		progress := float64(pm.processedRecords) / float64(pm.totalRecords) * 100

		// Clear line and print progress
		fmt.Printf("\r\033[K") // Clear the current line
		fmt.Printf("Progress: %.2f%% (%d/%d) | Rate: %.0f rec/sec | Elapsed: %v | ETA: %v",
			progress,
			pm.processedRecords,
			pm.totalRecords,
			avgRate,
			time.Since(pm.startTime).Round(time.Second),
			eta.Round(time.Second))

		pm.lastUpdate = currentTime
	}
}

func (pm *PerformanceMonitor) getAverageRate() float64 {
	if len(pm.rateHistory) == 0 {
		return 0
	}

	// Calculate weighted moving average, giving more weight to recent rates
	var weightedSum float64
	var weights float64
	for i, rate := range pm.rateHistory {
		weight := float64(i + 1) // More recent rates get higher weights
		weightedSum += rate * weight
		weights += weight
	}

	return weightedSum / weights
}

func (pm *PerformanceMonitor) getOptimalParameters() (int, int) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	if len(pm.rateHistory) < 3 {
		return 0, 0
	}

	currentRate := pm.rateHistory[len(pm.rateHistory)-1]
	avgRate := pm.getAverageRate()

	// If current rate is significantly lower than average, adjust parameters
	if currentRate < avgRate*0.7 {
		// Reduce workers if we might be having contention
		return pm.calculateNewParameters()
	}

	return 0, 0
}

func (pm *PerformanceMonitor) calculateNewParameters() (int, int) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// If memory usage is high, reduce batch size
	if memStats.Sys > memStats.TotalAlloc*80/100 {
		return runtime.NumCPU(), 500 // Reduce to baseline
	}

	return runtime.NumCPU() * 2, 1000 // Increase if memory is fine
}

func calculateOptimalWorkers(numCPU int) int {
	// Start with number of CPUs and adjust based on SQLite's concurrent write capabilities
	workers := numCPU
	if workers > 8 {
		workers = 8 // SQLite might not benefit from too many concurrent writers
	}
	if workers < 2 {
		workers = 2
	}
	return workers
}

func calculateOptimalBatchSize(availableMemoryGB float64) int {
	// Base batch size on available memory, with some constraints
	batchSize := int(availableMemoryGB * 500) // 500 records per GB as baseline
	if batchSize < 500 {
		batchSize = 500 // Minimum batch size
	}
	if batchSize > 5000 {
		batchSize = 5000 // Maximum batch size
	}
	return batchSize
}

func (pm *PerformanceMonitor) printFinalStats() {
	elapsed := time.Since(pm.startTime)
	avgRate := float64(pm.processedRecords) / elapsed.Seconds()
	fmt.Printf("\n\nFinal Statistics:\n")
	fmt.Printf("Total records processed: %d\n", pm.processedRecords)
	fmt.Printf("Total time: %v\n", elapsed.Round(time.Second))
	fmt.Printf("Average processing rate: %.0f records/second\n", avgRate)
}

func createIndexes(db *sqlx.DB) {
	log.Println("Creating indexes and optimizing for read performance...")

	// First create the composite index for the most common query pattern
	log.Println("Creating primary lookup index...")
	_, err := db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_address_lookup ON Features(
			PostalCode, 
			HouseNumber, 
				HouseNumberAddition
		) WHERE PostalCode IS NOT NULL;  -- Partial index for non-null postal codes
	`)
	if err != nil {
		log.Fatal("Error creating primary index:", err)
	}

	// Create other useful indexes
	log.Println("Creating secondary indexes...")
	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_postalcode ON Features(PostalCode) 
			WHERE PostalCode IS NOT NULL;
		CREATE INDEX IF NOT EXISTS idx_cityname ON Features(CityName) 
			WHERE CityName IS NOT NULL;
		CREATE INDEX IF NOT EXISTS idx_streetname ON Features(StreetName) 
			WHERE StreetName IS NOT NULL;
	`)
	if err != nil {
		log.Fatal("Error creating secondary indexes:", err)
	}

	// Optimize the database for reading
	log.Println("Optimizing database for read performance...")
	_, err = db.Exec(`
		-- Switch to normal journaling mode for reliability
		PRAGMA journal_mode = WAL;
		
		-- Optimize for reading
		PRAGMA synchronous = NORMAL;
		PRAGMA cache_size = -2000000;  -- Use 2GB memory for cache
		PRAGMA mmap_size = 30000000000;
		PRAGMA page_size = 4096;
		
		-- Analyze the database for query optimization
		ANALYZE;
		
		-- Compact the database
		VACUUM;
	`)
	if err != nil {
		log.Fatal("Error optimizing database:", err)
	}

	// Verify indexes
	log.Println("Verifying database structure...")
	var indexCount int
	err = db.Get(&indexCount, "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND tbl_name='Features'")
	if err != nil {
		log.Fatal("Error verifying indexes:", err)
	}
	log.Printf("Database has %d indexes\n", indexCount)

	// Add statistics
	var recordCount int
	err = db.Get(&recordCount, "SELECT COUNT(*) FROM Features")
	if err != nil {
		log.Fatal("Error counting records:", err)
	}
	log.Printf("Final record count: %d\n", recordCount)

	log.Println("Database optimization completed successfully")
}

func connectToExistingDB(dbFile string) *sqlx.DB {
	db, err := sqlx.Connect("sqlite3", dbFile)
	if err != nil {
		log.Fatal(err)
	}

	// Basic optimizations for the connection
	_, err = db.Exec(`
		PRAGMA journal_mode = WAL;
		PRAGMA synchronous = NORMAL;
		PRAGMA cache_size = -2000000;
		PRAGMA mmap_size = 30000000000;
	`)
	if err != nil {
		log.Fatal("Error setting PRAGMA:", err)
	}

	return db
}
