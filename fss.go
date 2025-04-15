package main

import (
	"archive/tar"
	"database/sql"
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

type FileInfo struct {
	Filename    string
	ContentType string
	Size        int64
	Reader      io.ReadCloser
}

var (
	logFilePath      = flag.String("l", "", "Path to the log file (leave empty to log to stdout)")
	concurrencyLimit = flag.Int("c", 50, "Maximum number of concurrent requests")
	timeout          = flag.Int("t", 30, "Timeout duration in seconds")
	usePrepare       = flag.Bool("p", false, "Allow prepare requests")
)

const DefaultContentType = "application/octet-stream"
const TarContentType = "application/x-tar"

// DB is a global variable for the SQLite database connection
var DB *sql.DB

func initDB() {
	var err error
	DB, err = sql.Open("sqlite3", "./fss.db")
	if err != nil {
		log.Fatal(err)
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

	_, err = DB.Exec(sqlStmt)
	if err != nil {
		log.Fatalf("Error creating table: %q: %s\n", err, sqlStmt)
	}
}

// Helper function to get the filename from the Content-Disposition header
func getFileName(resp *http.Response) string {
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

func getContentType(resp *http.Response) string {
	ct := resp.Header.Get("Content-Type")
	if ct != "" {
		mediatype, _, err := mime.ParseMediaType(ct)
		if err == nil {
			return mediatype
		}
	}

	name := getFileName(resp)
	ext := path.Ext(name)
	mimeExt := mime.TypeByExtension(ext)
	if mimeExt != "" {
		return mimeExt
	}

	return DefaultContentType
}

func createTemp(r io.ReadCloser) (*os.File, error) {
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

func doQuery(client *http.Client, url *url.URL) (*http.Response, error) {
	url.RawQuery = url.Query().Encode()

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to download %s: %v", url.String(), err)
	}

	req.Header.Set("User-Agent", "FileStreamService/1.0")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to download %s: %v", url.String(), err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to download %s: %v", url.String(), resp.Status)
	}

	return resp, nil
}

func writeFile(writer io.Writer, file io.Reader) error {

	if _, err := io.Copy(writer, file); err != nil {
		return fmt.Errorf("failed to write file to writer: %v", err)
	}

	return nil
}

func writeTarFile(tarWriter *tar.Writer, fileName string, file io.Reader, size int64) error {
	header := &tar.Header{
		Name: fileName,
		Size: size,
		Mode: 0600,
	}

	if err := tarWriter.WriteHeader(header); err != nil {
		return fmt.Errorf("failed to write tar header for %s: %v", fileName, err)
	}

	if err := writeFile(tarWriter, file); err != nil {
		return fmt.Errorf("failed to write file %s to tar: %v", fileName, err)
	}

	return nil
}

func processFile(writer io.Writer, fi FileInfo, wg *sync.WaitGroup) {
	var err error
	if tw, ok := writer.(*tar.Writer); ok {
		err = writeTarFile(tw, fi.Filename, fi.Reader, fi.Size)
	} else {
		err = writeFile(writer, fi.Reader)
	}

	if err != nil {
		log.Printf("failed to stream %s: %v", fi.Filename, err)
	}

	fi.Reader.Close()
	wg.Done()
}

func processFiles(writer io.Writer, fiCh chan FileInfo, wg *sync.WaitGroup) {

	for fi := range fiCh {
		wg.Add(1)
		processFile(writer, fi, wg)
	}

	wg.Done()
}

func processUrl(client *http.Client, rawUrl string, reqSize bool, wg *sync.WaitGroup, fiCh chan FileInfo, sem chan struct{}) {
	sem <- struct{}{}
	defer wg.Done()

	url, err := url.Parse(rawUrl)
	if err != nil {
		log.Printf("error parsing URL: %s, %v", rawUrl, err)
		return
	}

	resp, err := doQuery(client, url)
	if err != nil {
		log.Printf("%v", err)
		return
	}

	fileName := getFileName(resp)
	contentType := getContentType(resp)
	size := resp.ContentLength

	// If the caller requires the full size but the response is chunked, we first
	// have to read all the chunks before sending it along to the file channel
	var reader io.ReadCloser
	if reqSize && size == -1 {
		defer resp.Body.Close()
		file, err := createTemp(resp.Body)
		if err != nil {
			log.Printf("error creating temp file %v", err)
			return
		}

		fileInfo, err := file.Stat()
		if err != nil {
			log.Printf("failed to get file info for %s: %v", url, err)
			return
		}

		reader = file
		size = fileInfo.Size()

	} else {
		pr, pw := io.Pipe()
		reader = pr

		go func() {
			defer resp.Body.Close()
			defer pw.Close()

			if _, err := io.Copy(pw, resp.Body); err != nil {
				log.Printf("error copying data: %v", err)
			}

		}()
	}

	fiCh <- FileInfo{
		Filename:    fileName,
		ContentType: contentType,
		Size:        size,
		Reader:      reader,
	}

	<-sem
}

func closeWriters(writers ...io.Writer) {
	for _, writer := range writers {
		if closer, ok := writer.(io.Closer); ok {
			err := closer.Close()
			if err != nil {
				log.Printf("Error closing writer %v", err)
			}
		}
	}
}

func getParams(c echo.Context) (urls []string, tar bool, err error) {
	rawId := c.Param("id")
	if c.Request().Method == "GET" && rawId != "" {
		id, err := uuid.Parse(rawId)
		if err != nil {
			return nil, false, echo.NewHTTPError(http.StatusBadRequest, "Invalid UUID format: "+rawId)
		}

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

		var urlCs string // comma-separated list of URLs
		err = DB.QueryRow(query, id[:]).Scan(&tar, &urlCs)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, false, echo.NewHTTPError(http.StatusNotFound, "Prepare ID not found")
			}
			return nil, false, echo.NewHTTPError(http.StatusInternalServerError, "Database error")
		}

		urls = strings.Split(urlCs, ",")
	} else {
		if err := c.Bind(&urls); err != nil {
			return nil, false, echo.NewHTTPError(http.StatusBadRequest, "Invalid URL list")
		}

		if len(urls) == 0 {
			return nil, false, echo.NewHTTPError(http.StatusBadRequest, "No URLs provided")
		}

		tar = len(urls) > 1 || c.QueryParam("tar") == "true"
	}

	return urls, tar, nil
}

func fetch(c echo.Context) error {
	urls, writeTar, err := getParams(c)
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

	sem := make(chan struct{}, *concurrencyLimit) // Semaphore to limit concurrency
	fiCh := make(chan FileInfo, min(len(urls), *concurrencyLimit))

	client := &http.Client{
		Timeout: time.Duration(*timeout) * time.Second,
	}

	rwg.Add(len(urls))
	for _, url := range urls {
		go processUrl(client, url, writeTar, rwg, fiCh, sem)
	}

	wwg.Add(1)
	var contentType string
	var filename string

	// If we are not writing to TAR we are only dealing with one file.
	if !writeTar {
		fi := <-fiCh
		filename = fi.Filename
		contentType = fi.ContentType
		go processFile(writer, fi, wwg)
	} else {
		contentType = TarContentType
		filename = fmt.Sprintf("fss-files-%s.tar", time.Now().Format("2006-01-02T15-04-05"))
		go processFiles(writer, fiCh, wwg)
	}

	go func() {
		// All content has been pushed to the channel
		rwg.Wait()
		close(fiCh)

		// Writers are done
		wwg.Wait()
		closeWriters(writer, pw)
		pr.Close()
	}()

	c.Response().Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
	return c.Stream(http.StatusOK, contentType, pr)

}

func prepare(c echo.Context) error {
	urls, tar, err := getParams(c)
	if err != nil {
		return err
	}

	tx, err := DB.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %v", err)
	}

	id := uuid.New()
	if _, err := tx.Exec(`INSERT INTO prepare(id, tar) VALUES(?, ?)`, id[:], tar); err != nil {
		tx.Rollback()
		return fmt.Errorf("insert failed: %v", err)
	}

	for _, url := range urls {
		if _, err := tx.Exec(`INSERT INTO prepare_urls(prepare_id, url) VALUES(?, ?)`, id[:], url); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert failed: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit failed: %v", err)
	}

	return c.JSON(http.StatusOK, echo.Map{
		"id": id.String(),
	})
}

func setupLog() {
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
	setupLog()

	e := echo.New()
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{echo.GET, echo.POST, echo.PUT, echo.DELETE, echo.OPTIONS},
	}))

	if *usePrepare {
		initDB()
		e.POST("/prepare", prepare)
		e.GET("/fetch/prepared/:id", fetch)
	}

	e.POST("/fetch", fetch)
	e.Start(":5001")
}
