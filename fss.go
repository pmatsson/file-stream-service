package main

import (
	"archive/tar"
	"flag"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

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

func createTemp(resp *http.Response) (*os.File, error) {
	tempFile, err := os.CreateTemp("", "fss_*.tmp")
	if err != nil {
		return nil, err
	}

	if _, err = io.Copy(tempFile, resp.Body); err != nil {
		return nil, err
	}

	if _, err := tempFile.Seek(0, 0); err != nil {
		return nil, err
	}

	return tempFile, nil

}

func downloadFile(client *http.Client, url *url.URL) (*http.Response, error) {
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

func writeTar(tarWriter *tar.Writer, fileName string, file io.Reader, size int64) error {
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

func processFile(client *http.Client, url *url.URL, writer io.Writer, wg *sync.WaitGroup, mu *sync.Mutex) {
	defer wg.Done()
	resp, err := downloadFile(client, url)

	if err != nil {
		log.Printf("%v", err)
		return
	}

	defer resp.Body.Close()
	fileName := getFileName(resp)
	size := resp.ContentLength
	fileReader := resp.Body

	if resp.ContentLength == -1 {
		tempFile, err := createTemp(resp)

		if err != nil {
			log.Printf("failed to create temp file for %s: %v", url, err)
			return
		}

		fileInfo, err := tempFile.Stat()
		if err != nil {
			log.Printf("failed to get file info for %s: %v", url, err)
			return
		}

		fileReader = tempFile
		size = fileInfo.Size()
	}

	mu.Lock()
	defer mu.Unlock()
	if tw, ok := writer.(*tar.Writer); ok {
		err = writeTar(tw, fileName, fileReader, size)
		if err != nil {
			log.Printf("failed to stream %s to tar: %v", url, err)
			return
		}
	} else {
		err = writeFile(writer, fileReader)
		if err != nil {
			log.Printf("failed to stream %s to writer: %v", url, err)
			return
		}
	}
}

func fetch(c echo.Context) error {
	var urls []string
	if err := c.Bind(&urls); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid URL list")
	}

	tarParam := c.QueryParam("tar")
	writeTar, err := strconv.ParseBool(tarParam)
	if err != nil {
		writeTar = false
	}

	// If more than one URL is provided, write to a tar file
	if len(urls) > 1 {
		writeTar = true
	}

	// Create a pipe to stream data back to the requester
	pr, pw := io.Pipe()

	// Create a writer for the tar file if requested
	var writer io.Writer = pw
	if writeTar {
		writer = tar.NewWriter(pw)
	}

	var mu sync.Mutex
	wg := &sync.WaitGroup{}
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	for _, rawUrl := range urls {
		url, err := url.Parse(rawUrl)
		if err != nil {
			log.Printf("Error parsing URL: %s, %v", rawUrl, err)
			continue
		}

		wg.Add(1)
		go processFile(client, url, writer, wg, &mu)
	}

	// Close writers when all downloads are complete
	go func() {
		wg.Wait()
		// Close the writer if it is a tar writer
		if closer, ok := writer.(io.Closer); ok {
			err := closer.Close()
			if err != nil {
				log.Printf("Error closing writer %v", err)
			}
		}
		pw.Close()
	}()

	return c.Stream(http.StatusOK, "application/x-tar", pr)
}

func main() {
	logFilePath := flag.String("logFile", "", "Path to the log file (leave empty to log to stdout)")
	flag.Parse()

	var logOutput *os.File
	var err error
	if *logFilePath != "" {
		// Open the log file for writing (create if doesn't exist)
		logOutput, err = os.OpenFile(*logFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("Error opening log file: %v", err)
		}
	} else {
		// Default to writing logs to stdout
		logOutput = os.Stdout
	}

	log.SetOutput(logOutput)

	e := echo.New()
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{echo.GET, echo.POST, echo.PUT, echo.DELETE, echo.OPTIONS},
	}))
	e.POST("/fetch", fetch)
	e.Start(":5001")
}
