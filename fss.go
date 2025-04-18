package main

import (
	"archive/tar"
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	_ "github.com/mattn/go-sqlite3"
)

// Config holds all application configuration
type Config struct {
	LogFilePath      string
	ConcurrencyLimit int
	Timeout          time.Duration
	UsePrepare       bool
}

type FileInfo struct {
	Filename    string
	ContentType string
	Size        int64
	Reader      io.ReadCloser
}

type DownloadResult struct {
	URL         string
	Success     bool
	Error       string
	Filename    string
	ContentType string
	Size        int64
}

type FSS struct {
	config Config
	db     *sql.DB
}

var (
	ErrNoURLs         = errors.New("no URLs provided")
	ErrInvalidURL     = errors.New("invalid URL format")
	ErrDownloadFailed = errors.New("download failed")
	ErrDatabaseQuery  = errors.New("database query failed")
)

const (
	DefaultContentType = "application/octet-stream"
	TarContentType     = "application/x-tar"
)

var (
	logFilePath      = flag.String("l", "", "Path to the log file (leave empty to log to stdout)")
	concurrencyLimit = flag.Int("c", 4, "Maximum number of concurrent requests")
	timeout          = flag.Duration("t", 30*time.Second, "Timeout duration (0 to disable timeout)")
	usePrepare       = flag.Bool("p", false, "Allow prepare requests")
)

type tempFileReadCloser struct {
	io.ReadCloser
	OnClose func()
}

func (t *tempFileReadCloser) Close() error {
	err := t.ReadCloser.Close()
	if t.OnClose != nil {
		t.OnClose()
	}
	return err
}

func NewFSS() *FSS {
	config := Config{
		LogFilePath:      *logFilePath,
		ConcurrencyLimit: *concurrencyLimit,
		Timeout:          *timeout,
		UsePrepare:       *usePrepare,
	}

	return &FSS{
		config: config,
	}
}

func (fss *FSS) InitDB() error {
	var err error
	fss.db, err = sql.Open("sqlite3", "./fss.db")
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	// Test the connection
	if err = fss.db.Ping(); err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	sqlStmt := `
		CREATE TABLE IF NOT EXISTS prepare (
			id BLOB PRIMARY KEY,
			tar BOOLEAN DEFAULT 1,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		
		CREATE TABLE IF NOT EXISTS prepare_urls (
			prepare_id BLOB,
			url TEXT NOT NULL,
			PRIMARY KEY (prepare_id, url),
			FOREIGN KEY (prepare_id) REFERENCES prepare(id) ON DELETE CASCADE
    );`

	_, err = fss.db.Exec(sqlStmt)
	if err != nil {
		return fmt.Errorf("error creating tables: %w", err)
	}

	return nil
}

func (fss *FSS) extractFilenameFromResponse(resp *http.Response) string {
	cd := resp.Header.Get("Content-Disposition")
	if cd != "" {
		// Parse header for filename
		_, params, err := mime.ParseMediaType(cd)
		if err == nil {
			if filename, ok := params["filename"]; ok {
				return filename
			}
		}
	}

	return path.Base(resp.Request.URL.String())
}

func (fss *FSS) determineContentType(resp *http.Response) string {
	ct := resp.Header.Get("Content-Type")
	if ct != "" {
		mediatype, _, err := mime.ParseMediaType(ct)
		if err == nil {
			return mediatype
		}
	}

	name := fss.extractFilenameFromResponse(resp)
	ext := path.Ext(name)
	mimeExt := mime.TypeByExtension(ext)
	if mimeExt != "" {
		return mimeExt
	}

	return DefaultContentType
}

func (fss *FSS) createTempFile(r io.ReadCloser) (*os.File, error) {
	tempFile, err := os.CreateTemp("", "fss_*.tmp")
	if err != nil {
		return nil, err
	}

	if _, err = io.Copy(tempFile, r); err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return nil, err
	}

	_, err = tempFile.Seek(0, io.SeekStart)
	if err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return nil, err
	}

	return tempFile, nil
}

func (fss *FSS) fetchURL(ctx context.Context, client *http.Client, targetURL *url.URL) (*http.Response, error) {
	targetURL.RawQuery = targetURL.Query().Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", targetURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrInvalidURL, targetURL.String(), err)
	}

	req.Header.Set("User-Agent", "FileStreamService/1.0")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrDownloadFailed, targetURL.String(), err)
	}

	if resp.StatusCode != http.StatusOK {
		statusCode := resp.StatusCode
		statusText := http.StatusText(statusCode)
		if statusText == "" {
			statusText = "Unknown Status"
		}
		resp.Body.Close()
		return nil, fmt.Errorf("%w: HTTP %d %s - %s", ErrDownloadFailed, statusCode, statusText, targetURL.String())
	}

	return resp, nil
}

func (fss *FSS) copyDataToWriter(writer io.Writer, file io.Reader) error {
	if _, err := io.Copy(writer, file); err != nil {
		return fmt.Errorf("failed to write file to writer: %v", err)
	}

	return nil
}

func (fss *FSS) addFileToTarArchive(tarWriter *tar.Writer, fileName string, file io.Reader, size int64) error {
	header := &tar.Header{
		Name: fileName,
		Size: size,
		Mode: 0600,
	}

	if err := tarWriter.WriteHeader(header); err != nil {
		return fmt.Errorf("failed to write tar header for %s: %v", fileName, err)
	}

	if err := fss.copyDataToWriter(tarWriter, file); err != nil {
		return fmt.Errorf("failed to write file %s to tar: %v", fileName, err)
	}

	return nil
}

func (fss *FSS) processFilesSequentially(writer io.Writer, fiCh chan FileInfo, wg *sync.WaitGroup) {
	defer wg.Done()

	for fi := range fiCh {
		var err error
		if tw, ok := writer.(*tar.Writer); ok {
			err = fss.addFileToTarArchive(tw, fi.Filename, fi.Reader, fi.Size)
		} else {
			err = fss.copyDataToWriter(writer, fi.Reader)
		}
		fi.Reader.Close()

		if err != nil {
			log.Printf("Failed to stream %s: %v", fi.Filename, err)
		}
	}
}

// generateReport creates a report file listing all failed URLs and reasons
func (fss *FSS) generateReport(results []DownloadResult) (io.ReadCloser, int64) {
	var reportBuilder strings.Builder

	// Get current time
	currentTime := time.Now().Format("2006-01-02 15:04:05")

	// Build report header
	reportBuilder.WriteString(fmt.Sprintf("Download Report - %s\n", currentTime))
	reportBuilder.WriteString("===============================\n\n")

	// Count statistics
	successCount := 0
	failureCount := 0

	// Error types for summary
	errorTypes := make(map[string]int)

	// Process results
	for _, result := range results {
		if result.Success {
			successCount++
		} else {
			failureCount++

			// Extract error type
			errorMsg := result.Error
			errorType := "Unknown error"

			// HTTP status error pattern: "HTTP 404 Not Found"
			if strings.Contains(errorMsg, "HTTP ") {
				parts := strings.Split(errorMsg, " - ")
				if len(parts) > 0 {
					statusParts := strings.Split(parts[0], ": ")
					if len(statusParts) > 1 {
						errorType = statusParts[1] // "HTTP 404 Not Found"
					}
				}
			} else if strings.Contains(errorMsg, ":") {
				// Generic error pattern
				parts := strings.SplitN(errorMsg, ":", 2)
				errorType = strings.TrimSpace(parts[0])
			}

			errorTypes[errorType]++
		}
	}

	// Add summary section
	reportBuilder.WriteString("SUMMARY\n")
	reportBuilder.WriteString("-------\n")
	reportBuilder.WriteString(fmt.Sprintf("Total files: %d\n", len(results)))
	reportBuilder.WriteString(fmt.Sprintf("Successful: %d\n", successCount))
	reportBuilder.WriteString(fmt.Sprintf("Failed: %d\n\n", failureCount))

	// Add error type summary if failures exist
	if failureCount > 0 {
		reportBuilder.WriteString("ERROR TYPES\n")
		reportBuilder.WriteString("-----------\n")
		for errorType, count := range errorTypes {
			reportBuilder.WriteString(fmt.Sprintf("- %s: %d\n", errorType, count))
		}
		reportBuilder.WriteString("\n")
	}

	// Add detailed failures section
	if failureCount > 0 {
		reportBuilder.WriteString("FAILED DOWNLOADS\n")
		reportBuilder.WriteString("----------------\n")

		for i, result := range results {
			if !result.Success {
				// Get base filename only
				urlPath := result.URL
				if strings.Contains(urlPath, "?") {
					urlPath = strings.Split(urlPath, "?")[0]
				}
				filename := path.Base(urlPath)

				// Format error message to be more readable
				errorMsg := result.Error
				if strings.Contains(errorMsg, "download failed: HTTP") {
					// Extract the status code and text from HTTP errors
					statusStart := strings.Index(errorMsg, "HTTP")
					urlStart := strings.LastIndex(errorMsg, " - ")
					if statusStart >= 0 && urlStart > statusStart {
						statusInfo := errorMsg[statusStart:urlStart]
						errorMsg = statusInfo
					}
				}

				// Add numbered entry with concise details
				reportBuilder.WriteString(fmt.Sprintf("%d. %s\n", i+1, filename))
				reportBuilder.WriteString(fmt.Sprintf("   URL: %s\n", result.URL))
				reportBuilder.WriteString(fmt.Sprintf("   Error: %s\n\n", errorMsg))
			}
		}
	}

	reportContent := reportBuilder.String()
	size := int64(len(reportContent))

	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		io.WriteString(pw, reportContent)
	}()

	return pr, size
}

// downloadAndQueueFile fetches a URL and sends the file info to the channel
func (fss *FSS) downloadAndQueueFile(ctx context.Context, client *http.Client, rawUrl string, reqSize bool,
	wg *sync.WaitGroup, fiCh chan FileInfo, resultCh chan<- DownloadResult, sem chan struct{}) {
	sem <- struct{}{}
	defer func() {
		<-sem
		wg.Done()
	}()

	result := DownloadResult{
		URL:     rawUrl,
		Success: false,
	}

	url, err := url.Parse(rawUrl)
	if err != nil {
		log.Printf("error parsing URL: %s, %v", rawUrl, err)
		result.Error = fmt.Sprintf("Invalid URL format: %v", err)
		resultCh <- result
		return
	}

	resp, err := fss.fetchURL(ctx, client, url)
	if err != nil {
		log.Printf("%v", err)
		result.Error = err.Error()
		resultCh <- result
		return
	}

	fileName := fss.extractFilenameFromResponse(resp)
	contentType := fss.determineContentType(resp)
	size := resp.ContentLength

	result.Filename = fileName
	result.ContentType = contentType
	result.Size = size

	// If size is unknown (-1) or large (> 10MB), save to temp file first
	var reader io.ReadCloser
	if (reqSize && size == -1) || size > 10*1024*1024 {
		file, err := fss.createTempFile(resp.Body)
		if err != nil {
			resp.Body.Close()
			log.Printf("error creating temp file %v", err)
			result.Error = fmt.Sprintf("Failed to create temporary file: %v", err)
			resultCh <- result
			return
		}
		resp.Body.Close() // Close the original response body

		fileInfo, err := file.Stat()
		if err != nil {
			file.Close()
			os.Remove(file.Name())
			log.Printf("failed to get file info for %s: %v", url, err)
			result.Error = fmt.Sprintf("Failed to get file info: %v", err)
			resultCh <- result
			return
		}

		// Store the temp file path for cleanup
		tempPath := file.Name()
		reader = &tempFileReadCloser{
			ReadCloser: file,
			OnClose: func() {
				os.Remove(tempPath)
			},
		}
		size = fileInfo.Size()
		result.Size = size
	} else {
		// For streaming, we'll handle the body directly
		reader = resp.Body
	}

	select {
	case fiCh <- FileInfo{
		Filename:    fileName,
		ContentType: contentType,
		Size:        size,
		Reader:      reader,
	}:
		result.Success = true
		resultCh <- result
	case <-ctx.Done():
		log.Printf("context cancelled while queuing file %s", fileName)
		reader.Close()
		result.Error = "Request timed out or was cancelled"
		resultCh <- result
	}
}

func (fss *FSS) closeAllWriters(writers ...io.Writer) {
	for _, writer := range writers {
		if closer, ok := writer.(io.Closer); ok {
			err := closer.Close()
			if err != nil {
				log.Printf("Error closing writer %v", err)
			}
		}
	}
}

func (fss *FSS) parseDirectRequest(c echo.Context) ([]string, bool, error) {
	var urls []string
	if err := c.Bind(&urls); err != nil {
		return nil, false, echo.NewHTTPError(http.StatusBadRequest, "Invalid URL list")
	}

	if len(urls) == 0 {
		return nil, false, echo.NewHTTPError(http.StatusBadRequest, ErrNoURLs.Error())
	}

	// Set TAR mode if multiple URLs or explicitly requested
	tar := len(urls) > 1 || c.QueryParam("tar") == "true"
	return urls, tar, nil
}

func (fss *FSS) loadPreparedRequest(id uuid.UUID) ([]string, bool, error) {
	query := `
		SELECT 
			p.tar,
			GROUP_CONCAT(u.url) AS urls
		FROM 
			prepare p
		JOIN 
			prepare_urls u ON p.id = u.prepare_id
		WHERE 
			p.id = ? 
		GROUP BY 
			p.id;`

	var tar bool
	var urlsConcat string // comma-separated list of URLs
	err := fss.db.QueryRow(query, id[:]).Scan(&tar, &urlsConcat)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, echo.NewHTTPError(http.StatusNotFound, "Prepare ID not found")
		}
		return nil, false, echo.NewHTTPError(http.StatusInternalServerError, fmt.Sprintf("Database error: %v", err))
	}

	urls := strings.Split(urlsConcat, ",")
	return urls, tar, nil
}

func (fss *FSS) parseRequestParameters(c echo.Context) (urls []string, tar bool, err error) {
	// Check if this is a prepared request
	rawID := c.Param("id")
	if c.Request().Method == "GET" && rawID != "" {
		id, err := uuid.Parse(rawID)
		if err != nil {
			return nil, false, echo.NewHTTPError(http.StatusBadRequest, "Invalid UUID format: "+rawID)
		}
		return fss.loadPreparedRequest(id)
	}

	// Otherwise parse the direct request body
	return fss.parseDirectRequest(c)
}

func (fss *FSS) HandleFetchRequest(c echo.Context) error {
	ctx := c.Request().Context()

	urls, writeTar, err := fss.parseRequestParameters(c)
	if err != nil {
		return err
	}

	// Create a pipe to stream data back to the requester
	pr, pw := io.Pipe()

	// Create a writer for the tar file if requested
	var writer io.Writer = pw
	if writeTar {
		writer = tar.NewWriter(pw)
	}

	readWg := &sync.WaitGroup{}
	writeWg := &sync.WaitGroup{}
	resultWg := &sync.WaitGroup{}

	sem := make(chan struct{}, fss.config.ConcurrencyLimit) // Semaphore to limit concurrency
	fiCh := make(chan FileInfo, min(len(urls), fss.config.ConcurrencyLimit))
	resultCh := make(chan DownloadResult, len(urls))

	// Create a context that will be cancelled if the client disconnects
	fetchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Add timeout if configured
	if fss.config.Timeout > 0 {
		var timeoutCancel context.CancelFunc
		fetchCtx, timeoutCancel = context.WithTimeout(fetchCtx, fss.config.Timeout)
		defer timeoutCancel()
	}

	// Listen for client disconnection
	go func() {
		<-c.Request().Context().Done()
		cancel()
	}()

	client := &http.Client{
		Timeout: fss.config.Timeout,
	}

	// Start download tasks
	readWg.Add(len(urls))
	for _, url := range urls {
		go fss.downloadAndQueueFile(fetchCtx, client, url, writeTar, readWg, fiCh, resultCh, sem)
	}

	resultWg.Add(len(urls))
	var downloadResults []DownloadResult
	var resultsMutex sync.Mutex
	go func() {
		for result := range resultCh {
			resultsMutex.Lock()
			downloadResults = append(downloadResults, result)
			resultsMutex.Unlock()
			resultWg.Done() // Mark this result as processed
		}
	}()

	writeWg.Add(1)
	var contentType string
	var filename string

	// If we are not writing to TAR we are only dealing with one file.
	if !writeTar {
		select {
		case fi, ok := <-fiCh:
			if !ok {
				return echo.NewHTTPError(http.StatusBadGateway, "Failed to download any files")
			}
			filename = fi.Filename
			contentType = fi.ContentType
			go fss.writeFileToOutput(writer, fi, writeWg)
		case <-fetchCtx.Done():
			return echo.NewHTTPError(http.StatusGatewayTimeout, "Request timed out")
		}
	} else {
		contentType = TarContentType
		filename = fmt.Sprintf("fss-files-%s.tar", time.Now().Format("2006-01-02T15-04-05"))
		go fss.processFilesSequentially(writer, fiCh, writeWg)
	}

	go func() {
		// All content has been pushed to the channel
		readWg.Wait()
		close(fiCh)

		// Wait for all files to be processed before we start handling the report
		writeWg.Wait()

		close(resultCh)
		resultWg.Wait()

		if writeTar {
			resultsMutex.Lock()

			// Ensure report includes all URLs by checking for missing results
			if len(downloadResults) < len(urls) {
				// Add entries for URLs that didn't get a result
				urlMap := make(map[string]bool)
				for _, result := range downloadResults {
					urlMap[result.URL] = true
				}

				for _, url := range urls {
					if !urlMap[url] {
						// Add a synthetic result for this URL
						downloadResults = append(downloadResults, DownloadResult{
							URL:     url,
							Success: false,
							Error:   "No response received - request may have timed out",
						})
					}
				}
			}

			reportReader, reportSize := fss.generateReport(downloadResults)
			if tw, ok := writer.(*tar.Writer); ok {
				if err := fss.addFileToTarArchive(tw, "_download_report.txt", reportReader, reportSize); err != nil {
					log.Printf("Failed to add download report to tar: %v", err)
				}
			}
			reportReader.Close()
			resultsMutex.Unlock()
		}

		fss.closeAllWriters(writer, pw)
		pr.Close()
	}()

	c.Response().Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
	return c.Stream(http.StatusOK, contentType, pr)
}

func (fss *FSS) HandlePrepareRequest(c echo.Context) error {
	urls, tar, err := fss.parseRequestParameters(c)
	if err != nil {
		return err
	}

	tx, err := fss.db.Begin()
	if err != nil {
		log.Printf("failed to start transaction: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError, "Database error")
	}
	defer tx.Rollback() // Will be a no-op if tx.Commit() is called

	id := uuid.New()
	if _, err := tx.Exec(`INSERT INTO prepare(id, tar) VALUES(?, ?)`, id[:], tar); err != nil {
		log.Printf("insert failed: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError, "Database error")
	}

	for _, url := range urls {
		if _, err := tx.Exec(`INSERT INTO prepare_urls(prepare_id, url) VALUES(?, ?)`, id[:], url); err != nil {
			log.Printf("insert failed: %v", err)
			return echo.NewHTTPError(http.StatusInternalServerError, "Database error")
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("commit failed: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError, "Database error")
	}

	return c.JSON(http.StatusOK, echo.Map{
		"id": id.String(),
	})
}

func (fss *FSS) writeFileToOutput(writer io.Writer, fi FileInfo, wg *sync.WaitGroup) {
	defer wg.Done()
	defer fi.Reader.Close()

	var err error
	if tw, ok := writer.(*tar.Writer); ok {
		err = fss.addFileToTarArchive(tw, fi.Filename, fi.Reader, fi.Size)
	} else {
		err = fss.copyDataToWriter(writer, fi.Reader)
	}

	if err != nil {
		log.Printf("Failed to stream %s: %v", fi.Filename, err)
	}
}

func setupLogging() {
	var logOutput *os.File
	var err error
	if *logFilePath != "" {
		logOutput, err = os.OpenFile(*logFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("Error opening log file: %v", err)
		}
	} else {
		// Default to writing logs to stdout
		logOutput = os.Stdout
	}

	log.SetOutput(logOutput)
}

func main() {
	flag.Parse()
	setupLogging()

	fss := NewFSS()

	e := echo.New()
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{echo.GET, echo.POST, echo.PUT, echo.DELETE, echo.OPTIONS},
	}))

	if fss.config.UsePrepare {
		if err := fss.InitDB(); err != nil {
			log.Fatalf("Failed to initialize database: %v", err)
		}
		defer fss.db.Close()

		e.POST("/prepare", fss.HandlePrepareRequest)
		e.GET("/fetch/prepared/:id", fss.HandleFetchRequest)
	}

	e.POST("/fetch", fss.HandleFetchRequest)

	// Start the server
	log.Printf("Starting File Stream Service on port 5001")
	if err := e.Start(":5001"); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("Server error: %v", err)
	}
}
