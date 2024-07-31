package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/cobra"
)

const sourcesURL = "https://api.worldbank.org/v2/sources?format=json&per_page=1000"

// PageInfo represents the pagination information in the JSON
type PageInfo struct {
	Page    string `json:"page"`
	Pages   string `json:"pages"`
	PerPage string `json:"per_page"`
	Total   string `json:"total"`
	// SourceId    int    `json:"sourceid"`
	// LastUpdated string `json:"lastupdated"`
}

// Dataset represents the dataset information in the JSON
type Dataset struct {
	ID                   string `json:"id"`
	LastUpdated          string `json:"lastupdated"`
	Name                 string `json:"name"`
	Code                 string `json:"code"`
	Description          string `json:"description"`
	URL                  string `json:"url"`
	DataAvailability     string `json:"dataavailability"`
	MetadataAvailability string `json:"metadataavailability"`
	Concepts             string `json:"concepts"`
}

// Response represents the entire JSON structure
type Response struct {
	PageInfo PageInfo  `json:"page_info"`
	Datasets []Dataset `json:"datasets"`
}

func init() {
	rootCmd.AddCommand(sourcesCmd)
}

func getSources() ([]Dataset, error) {
	resp, err := http.Get(sourcesURL)
	if err != nil {
		fmt.Println("Error fetching sources")
		return nil, err
	}
	defer resp.Body.Close()

	// parse sources
	body, _ := io.ReadAll(resp.Body)

	// Unmarshal the JSON data
	var result [2]interface{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}

	// Parse the first element (PageInfo)
	var pageInfo PageInfo
	pageInfoBytes, err := json.Marshal(result[0])
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(pageInfoBytes, &pageInfo)
	if err != nil {
		return nil, err
	}

	// Parse the second element (Datasets)
	var datasets []Dataset
	datasetsBytes, err := json.Marshal(result[1])
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(datasetsBytes, &datasets)
	if err != nil {
		return nil, err
	}

	return datasets, nil
}

var sourcesCmd = &cobra.Command{
	Use:   "sources",
	Short: "List the available sources",
	Run: func(cmd *cobra.Command, args []string) {
		sources, err := getSources()

		if err != nil {
			fmt.Println(err)
			return
		}

		t := table.NewWriter()
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(table.Row{"ID", "Name"})

		for _, source := range sources {
			t.AppendRow([]interface{}{source.ID, source.Name})
		}

		t.Render()
	},
}
