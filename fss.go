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
		resp.Body.Close()
		return nil, fmt.Errorf("%w: %s: %s", ErrDownloadFailed, targetURL.String(), resp.Status)
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
		log.Printf("failed to stream %s: %v", fi.Filename, err)
	}
}

func (fss *FSS) processFilesSequentially(writer io.Writer, fiCh chan FileInfo, wg *sync.WaitGroup) {
	defer wg.Done()

	for fi := range fiCh {
		wg.Add(1)
		fss.writeFileToOutput(writer, fi, wg)
	}
}

func (fss *FSS) downloadAndQueueFile(ctx context.Context, client *http.Client, rawUrl string, reqSize bool, wg *sync.WaitGroup, fiCh chan FileInfo, sem chan struct{}) {
	sem <- struct{}{}
	defer func() {
		<-sem
		wg.Done()
	}()

	url, err := url.Parse(rawUrl)
	if err != nil {
		log.Printf("error parsing URL: %s, %v", rawUrl, err)
		return
	}

	resp, err := fss.fetchURL(ctx, client, url)
	if err != nil {
		log.Printf("%v", err)
		return
	}

	fileName := fss.extractFilenameFromResponse(resp)
	contentType := fss.determineContentType(resp)
	size := resp.ContentLength

	// If the caller requires the full size but the response is chunked, we first
	// have to read all the chunks before sending it along to the file channel
	var reader io.ReadCloser
	if reqSize && size == -1 {
		file, err := fss.createTempFile(resp.Body)
		if err != nil {
			resp.Body.Close()
			log.Printf("error creating temp file %v", err)
			return
		}
		resp.Body.Close() // Close the original response body

		fileInfo, err := file.Stat()
		if err != nil {
			file.Close()
			os.Remove(file.Name())
			log.Printf("failed to get file info for %s: %v", url, err)
			return
		}

		reader = file
		size = fileInfo.Size()
	} else {
		// For streaming, we'll handle the body in the goroutine
		pr, pw := io.Pipe()
		reader = pr

		// We need to keep the response body open for the pipe writing goroutine
		// Don't defer resp.Body.Close() here since we want the goroutine to control that
		go func() {
			defer pw.Close()
			defer resp.Body.Close() // Close the body after we're done copying

			if _, err := io.Copy(pw, resp.Body); err != nil {
				log.Printf("error copying data: %v", err)
			}
		}()
	}

	select {
	case fiCh <- FileInfo{
		Filename:    fileName,
		ContentType: contentType,
		Size:        size,
		Reader:      reader,
	}:
	case <-ctx.Done():
		log.Printf("context cancelled while queuing file %s", fileName)
		reader.Close()
		if reqSize && size == -1 {
			// Body is already closed in this case
		} else {
			// Otherwise ensure we close it
			resp.Body.Close()
		}
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

	rwg := &sync.WaitGroup{} // Wait for reads
	wwg := &sync.WaitGroup{} // wait for writes

	sem := make(chan struct{}, fss.config.ConcurrencyLimit) // Semaphore to limit concurrency
	fiCh := make(chan FileInfo, min(len(urls), fss.config.ConcurrencyLimit))

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

	rwg.Add(len(urls))
	for _, url := range urls {
		go fss.downloadAndQueueFile(fetchCtx, client, url, writeTar, rwg, fiCh, sem)
	}

	wwg.Add(1)
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
			go fss.writeFileToOutput(writer, fi, wwg)
		case <-fetchCtx.Done():
			return echo.NewHTTPError(http.StatusGatewayTimeout, "Request timed out")
		}
	} else {
		contentType = TarContentType
		filename = fmt.Sprintf("fss-files-%s.tar", time.Now().Format("2006-01-02T15-04-05"))
		go fss.processFilesSequentially(writer, fiCh, wwg)
	}

	go func() {
		// All content has been pushed to the channel
		rwg.Wait()
		close(fiCh)

		// Writers are done
		wwg.Wait()
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
