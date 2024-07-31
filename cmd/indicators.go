package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/cobra"
)

var (
	source_id int
)

func init() {
	indicatorsCmd.Flags().IntVarP(&source_id, "source", "s", -1, "Source ID")
	indicatorsCmd.MarkFlagRequired("source")

	rootCmd.AddCommand(indicatorsCmd)
}

const indicatorsURL = "https://api.worldbank.org/v2/sources/%d/indicators?format=json&per_page=10000"

type IndicatorInfoPageInfo struct {
	Page    int    `json:"page"`
	Pages   int    `json:"pages"`
	PerPage string `json:"per_page"`
	Total   int    `json:"total"`
}

type SourceInfo struct {
	ID    string `json:"id"`
	Value string `json:"value"`
}

type TopicInfo struct {
	ID    string `json:"id"`
	Value string `json:"value"`
}

type IndicatorInfo struct {
	ID                 string      `json:"id"`
	Name               string      `json:"name"`
	Unit               string      `json:"unit"`
	Source             SourceInfo  `json:"source"`
	SourceNote         string      `json:"sourceNote"`
	SourceOrganization string      `json:"sourceOrganization"`
	Topics             []TopicInfo `json:"topics"`
}

// Response represents the entire JSON structure
type IndicatorsResponse struct {
	PageInfo  IndicatorInfoPageInfo `json:"page_info"`
	Indicator []IndicatorInfo       `json:"indicators"`
}

func init() {
	rootCmd.AddCommand(sourcesCmd)
}

func getIndicators() ([]IndicatorInfo, error) {
	resp, err := http.Get(fmt.Sprintf(indicatorsURL, source_id))
	if err != nil {
		fmt.Println("Error fetching indicator")
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
	var pageInfo IndicatorInfoPageInfo
	pageInfoBytes, err := json.Marshal(result[0])
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(pageInfoBytes, &pageInfo)
	if err != nil {
		return nil, err
	}

	var indicatorsInfo []IndicatorInfo
	datasetsBytes, err := json.Marshal(result[1])
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(datasetsBytes, &indicatorsInfo)
	if err != nil {
		return nil, err
	}

	return indicatorsInfo, nil
}

var indicatorsCmd = &cobra.Command{
	Use:   "indicators",
	Short: "List the available indicators",
	Run: func(cmd *cobra.Command, args []string) {
		indicators, err := getIndicators()
		if err != nil {
			log.Fatal(err)
		}

		t := table.NewWriter()
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(table.Row{"Code", "Name"})

		for _, indicator := range indicators {
			t.AppendRow([]interface{}{indicator.ID, indicator.Name})
		}

		t.Render()
	},
}
