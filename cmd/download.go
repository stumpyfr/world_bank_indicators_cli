package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"database/sql"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/spf13/cobra"
)

type IndicatorPageInfo struct {
	Page        int    `json:"page"`
	Pages       int    `json:"pages"`
	PerPage     int    `json:"per_page"`
	Total       int    `json:"total"`
	SourceId    string `json:"sourceid"`
	LastUpdated string `json:"lastupdated"`
}

type IndicatorId struct {
	ID    string `json:"id"`
	Value string `json:"value"`
}

type Country struct {
	ID    string `json:"id"`
	Value string `json:"value"`
}

type Indicator struct {
	Indicator       IndicatorId `json:"indicator"`
	Country         Country     `json:"country"`
	Countryiso3code string      `json:"countryiso3code"`
	Date            string      `json:"date"`
	Value           float32     `json:"value"`
	Decimal         int         `json:"decimal"`
}

type IndicatorPageResponse struct {
	PageInfo  IndicatorPageInfo `json:"page_info"`
	Indicator []Indicator       `json:"indicators"`
}

var duckdb_db string
var csv_output string
var parquet_output string
var indicator string
var date string
var refresh bool
var nb_per_page int
var table_name string

func init() {
	downloadCmd.Flags().StringVarP(&duckdb_db, "database", "d", "", "DuckDB database, if not provided will use an in-memory database")
	downloadCmd.Flags().StringVarP(&csv_output, "csv", "", "", "CSV output file")
	downloadCmd.Flags().StringVarP(&parquet_output, "parquet", "", "", "Parquet output file")

	downloadCmd.Flags().StringVarP(&indicator, "indicator", "i", "", "Indicator code to download")
	downloadCmd.MarkFlagRequired("indicator")

	downloadCmd.Flags().StringVarP(&date, "timeframe", "t", "", "Timeframe to download (ex: 2023:2010)")
	downloadCmd.MarkFlagRequired("date")

	downloadCmd.Flags().StringVarP(&table_name, "table", "n", "", "Name of the table to store (if database is provided), by default will be the indicator code")
	downloadCmd.Flags().IntVarP(&nb_per_page, "nb_per_page", "", 1000, "Number of items per page, default is 1000")
	downloadCmd.Flags().BoolVarP(&refresh, "refresh", "r", false, "Force refresh of the data if the table already exists in the database")

	rootCmd.AddCommand(downloadCmd)
}

func downloadPage(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil, err
	}

	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return body, nil
}

func parsePage(body []byte) (*IndicatorPageInfo, []Indicator, error) {
	// Unmarshal the JSON data
	var result [2]interface{}
	err := json.Unmarshal(body, &result)
	if err != nil {
		return nil, nil, err
	}

	// Parse the first element (PageInfo)
	var pageInfo IndicatorPageInfo
	pageInfoBytes, err := json.Marshal(result[0])
	if err != nil {
		return nil, nil, err
	}
	err = json.Unmarshal(pageInfoBytes, &pageInfo)
	if err != nil {
		return nil, nil, err
	}

	// Parse the second element (Datasets)
	var indicators []Indicator
	datasetsBytes, err := json.Marshal(result[1])
	if err != nil {
		return nil, nil, err
	}
	err = json.Unmarshal(datasetsBytes, &indicators)
	if err != nil {
		return nil, nil, err
	}

	return &pageInfo, indicators, nil
}

func downloadIndicator(indicator_code, date string) ([]Indicator, error) {
	url := fmt.Sprintf("https://api.worldbank.org/v2/country/all/indicator/%s?format=json&date=%s&per_page=%d", indicator_code, date, nb_per_page)
	payload, err := downloadPage(url)
	if err != nil {
		return nil, err
	}

	indicators := make([]Indicator, 0)

	// download the first page
	pageInfo, indicatorPage, err := parsePage(payload)
	if err != nil {
		return nil, err
	}
	indicators = append(indicators, indicatorPage...)
	fmt.Println("Page: 1", "Number of page:", pageInfo.Pages, "Total datapoint:", pageInfo.Total)

	// download the other pages
	for i := 2; i <= pageInfo.Pages; i++ {
		url := fmt.Sprintf("https://api.worldbank.org/v2/country/all/indicator/%s?format=json&date=%s&per_page=%d&page=%d", indicator_code, date, nb_per_page, i)
		// fmt.Printf("Downloading page %d...\n", i)
		fmt.Println("Page:", i, "Number of page:", pageInfo.Pages, "Total datapoint:", pageInfo.Total)
		payload, err := downloadPage(url)
		if err != nil {
			return nil, err
		}

		_, indicatorPage, err := parsePage(payload)
		if err != nil {
			return nil, err
		}
		indicators = append(indicators, indicatorPage...)
	}

	return indicators, nil
}

var downloadCmd = &cobra.Command{
	Use:   "dl",
	Short: "Download an indicator in various format",
	Run: func(cmd *cobra.Command, args []string) {

		db, err := sql.Open("duckdb", duckdb_db)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		defer db.Close()

		if table_name == "" {
			table_name = strings.Replace(indicator, ".", "_", -1)
		}

		// check if in the indicator is already in the database
		if !refresh {
			rows, err := db.Query(fmt.Sprintf(`SELECT * FROM %s LIMIT 1`, table_name))
			if err == nil {
				rows.Close()
				fmt.Printf("Table '%s' already exists, use -r to force refresh\n", table_name)
			}
		}

		if refresh {
			indicators, err := downloadIndicator(indicator, date)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			_, err = db.Exec(`CREATE TEMPORARY TABLE tmp (name VARCHAR, iso3name VARCHAR, year INTEGER, value DOUBLE)`)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			for _, indicator := range indicators {
				_, err = db.Exec(`INSERT INTO tmp VALUES (?, ?, ?, ?)`,
					indicator.Country.Value,
					indicator.Countryiso3code,
					indicator.Date,
					indicator.Value)
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
			}

			fmt.Println("Creating table:", table_name)
			_, err = db.Exec(fmt.Sprintf(`CREATE OR REPLACE TABLE %s AS PIVOT tmp ON year USING SUM(value) GROUP BY name, iso3name`, table_name))
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		}

		if csv_output != "" {
			fmt.Println("export to csv")
			_, err = db.Exec(fmt.Sprintf(`COPY %s TO '%s' (HEADER, DELIMITER ',')`, table_name, csv_output))
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		}

		if parquet_output != "" {
			fmt.Println("export to parquet")
			_, err = db.Exec(fmt.Sprintf(`COPY %s TO '%s' (FORMAT 'parquet')`, table_name, parquet_output))
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		}
	},
}
