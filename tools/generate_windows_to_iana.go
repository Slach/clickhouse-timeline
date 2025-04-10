//go:generate go run generate_windows_to_iana.go
package main

import (
	"archive/zip"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	_ "time/tzdata"
)

// SupplementalData represents the root of the XML file.
type SupplementalData struct {
	WindowsZones WindowsZones `xml:"windowsZones"`
}

// WindowsZones wraps the mapTimezones element.
type WindowsZones struct {
	MapTimezones MapTimezones `xml:"mapTimezones"`
}

// MapTimezones contains the list of mapZone entries.
type MapTimezones struct {
	MapZone []MapZone `xml:"mapZone"`
}

// MapZone represents a single mapping entry.
type MapZone struct {
	Other string `xml:"other,attr"`
	Type  string `xml:"type,attr"`
}

func main() {
	const zipURL = "https://unicode.org/Public/cldr/latest/core.zip"
	const xmlPath = "common/supplemental/windowsZones.xml" // adjust if needed

	log.Println("Starting the process...")

	// Download the zip archive.
	log.Println("Downloading zip archive...")
	resp, createErr := http.Get(zipURL)
	if createErr != nil {
		log.Fatalf("Error downloading zip: %v\n", createErr)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Printf("can't close resp.Body, closeErr: %v\n", closeErr)
		}
	}()

	// Save the zip file to a temporary file.
	log.Println("Creating temporary file for zip...")
	tmpZip, createErr := os.CreateTemp("", "core-*.zip")
	if createErr != nil {
		log.Fatalf("Error creating temp file: %v\n", createErr)
	}
	defer func() {
		if err := os.Remove(tmpZip.Name()); err != nil {
			log.Printf("Error removing temp file: %v\n", err)
		}
	}()

	log.Println("Writing zip content to temporary file...")
	if _, err := io.Copy(tmpZip, resp.Body); err != nil {
		log.Fatalf("Error writing zip to temp file: %v\n", err)
	}
	if err := tmpZip.Close(); err != nil {
		log.Fatalf("Error closing temp file: %v\n", err)
	}

	// Open the zip archive.
	log.Println("Opening zip archive...")
	zipReader, createErr := zip.OpenReader(tmpZip.Name())
	if createErr != nil {
		log.Fatalf("Error opening zip file: %v\n", createErr)
	}
	defer func() {
		if err := zipReader.Close(); err != nil {
			log.Printf("Error closing zip reader: %v\n", err)
		}
	}()

	// Find the windowsZones.xml file.
	log.Println("Searching for windowsZones.xml in the zip archive...")
	var xmlFile *zip.File
	for _, f := range zipReader.File {
		if f.Name == xmlPath {
			xmlFile = f
			break
		}
	}
	if xmlFile == nil {
		log.Fatalf("Error: %q not found in zip archive\n", xmlPath)
	}

	// Open and parse the XML file.
	log.Println("Opening and parsing windowsZones.xml...")
	rc, createErr := xmlFile.Open()
	if createErr != nil {
		log.Fatalf("Error opening XML file in zip: %v\n", createErr)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			log.Printf("Error closing XML file: %v\n", err)
		}
	}()

	var data SupplementalData
	if err := xml.NewDecoder(rc).Decode(&data); err != nil {
		log.Fatalf("Error decoding XML: %v\n", err)
	}

	// Build the WindowsToIana mapping.
	log.Println("Building Windows to IANA mapping...")
	mapping := make(map[string]string)
	for _, zone := range data.WindowsZones.MapTimezones.MapZone {
		types := strings.Fields(zone.Type)
		if len(types) > 0 {
			mapping[zone.Other] = types[0] // using the first IANA zone listed
		}
	}

	// Prepare the output directory.
	log.Println("Preparing output directory...")
	outDir := filepath.Join("..", "pkg", "timezone")
	if mkdirErr := os.MkdirAll(outDir, 0755); mkdirErr != nil {
		log.Fatalf("Error creating output directory: %v\n", mkdirErr)
	}

	// Create the generated Go source file.
	log.Println("Creating output Go source file...")
	outPath := filepath.Join(outDir, "windows_to_iana.go")
	outFile, createErr := os.Create(outPath)
	if createErr != nil {
		log.Fatalf("Error creating output file: %v\n", createErr)
	}
	defer func() {
		if closeErr := outFile.Close(); closeErr != nil {
			log.Printf("Error closing output file: %v\n", closeErr)
		}
	}()

	// Write header and package declaration.
	log.Println("Writing header and package declaration...")
	if _, err := fmt.Fprintf(outFile, "// Code generated by go generate; DO NOT EDIT.\n"); err != nil {
		log.Fatalf("Error writing header: %v\n", err)
	}
	if _, err := fmt.Fprintf(outFile, "package timezone\n\n"); err != nil {
		log.Fatalf("Error writing package declaration: %v\n", err)
	}
	if _, err := fmt.Fprintf(outFile, "// WindowsToIana maps Windows time zone names to IANA time zone names.\n"); err != nil {
		log.Fatalf("Error writing comment: %v\n", err)
	}
	if _, err := fmt.Fprintf(outFile, "var WindowsToIana = map[string]string{\n"); err != nil {
		log.Fatalf("Error writing map declaration: %v\n", err)
	}

	// For consistent output, sort the keys.
	log.Println("Sorting keys for consistent output...")
	var keys []string
	for k := range mapping {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Write the mapping entries.
	log.Println("Writing mapping entries...")
	for _, k := range keys {
		if _, err := fmt.Fprintf(outFile, "\t%q: %q,\n", k, mapping[k]); err != nil {
			log.Fatalf("Error writing mapping entry: %v\n", err)
		}
	}
	if _, err := fmt.Fprintf(outFile, "}\n"); err != nil {
		log.Fatalf("Error writing closing brace: %v\n", err)
	}

	log.Println("Process completed successfully!")
}
