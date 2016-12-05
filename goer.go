package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	bigquery "github.com/aeud/go_google_bigquery/v2"
	storage "github.com/aeud/go_google_storage"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

var (
	argFrom          *string = flag.String("from", time.Now().Add(-24*time.Hour).Format("2006-01-02"), "From")
	argDelta         *int    = flag.Int("delta", 3, "Delta")
	argBases         *string = flag.String("bases", "SGD", "Base currencies")
	argGoogleKey     *string = flag.String("key", "", "Google key file path")
	argGoogleProject *string = flag.String("project", "", "Google project")
	argDataset       *string = flag.String("dataset", "", "Google BigQuery Dataset")
	argTable         *string = flag.String("table", "", "Google BigQuery Table")
	argBucket        *string = flag.String("bucket", "", "Google Storage Bucket")
	argAppId         *string = flag.String("app", "", "Open Exchange Rate App Id")
	from             time.Time
	delta            int
	bases            []string
	googleKey        string
	googleProject    string
	bqDataset        string
	bqTable          string
	bucketName       string
	appId            string
)

type OpenExchangeRateResponse struct {
	Base      string             `json:"base"`
	Timestamp int64              `json:"timestamp"`
	Rates     map[string]float64 `json:"rates"`
}

func (r *OpenExchangeRateResponse) ExchangeRates(d time.Time) []*ExchangeRate {
	rs := make([]*ExchangeRate, len(r.Rates))
	var i int
	for c, rate := range r.Rates {
		b := r.Base
		cpu := rate
		upc := 1 / cpu
		rs[i] = NewExchangeRate(d, b, c, upc, cpu)
		i++
	}
	return rs
}

type ExchangeRate struct {
	date              time.Time
	StringDate        string  `json:"date"`
	Base              string  `json:"base"`
	Currency          string  `json:"currency"`
	UnitsPerCurrency  float64 `json:"units_per_currency"`
	CurrenciesPerUnit float64 `json:"currencies_per_unit"`
}

func NewExchangeRate(d time.Time, b, c string, upc, cpu float64) (er *ExchangeRate) {
	er = new(ExchangeRate)
	er.date = d
	er.StringDate = d.Format("2006-01-02")
	er.Base = b
	er.Currency = c
	er.UnitsPerCurrency = upc
	er.CurrenciesPerUnit = cpu
	return
}

func (er *ExchangeRate) JSON() (bs []byte) {
	bs, err := json.Marshal(er)
	catchError(err)
	return
}

func catchError(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

func Fetch(d time.Time, base string) (content []byte) {
	multiContent := make([][]byte, 0)
	layout := "https://openexchangerates.org/api/historical/%v.json?app_id=%v&base=%v"
	resp, err := http.Get(fmt.Sprintf(layout, d.Format("2006-01-02"), appId, base))
	catchError(err)
	defer resp.Body.Close()
	bs, err := ioutil.ReadAll(resp.Body)
	catchError(err)
	oerr := new(OpenExchangeRateResponse)
	catchError(json.Unmarshal(bs, oerr))

	ers := oerr.ExchangeRates(d)

	for _, er := range ers {
		multiContent = append(multiContent, er.JSON())
	}

	content = bytes.Join(multiContent, []byte("\n"))
	return
}

func init() {
	var err error
	flag.Parse()
	from, err = time.Parse("2006-01-02", *argFrom)
	catchError(err)
	delta = *argDelta
	bases = strings.Split(*argBases, ",")
	googleKey = *argGoogleKey
	googleProject = *argGoogleProject
	bqDataset = *argDataset
	bqTable = *argTable
	bucketName = *argBucket
	appId = *argAppId
}

func main() {
	storageClient := storage.NewStorageClient(googleKey)
	for i := 0; i < delta; i++ {
		d := from.Add(-1 * time.Duration(i) * 24 * time.Hour)
		wg := new(sync.WaitGroup)
		for _, base := range bases {
			wg.Add(1)
			go func(d time.Time, base string, wg *sync.WaitGroup) {
				content := Fetch(d, base)
				filename := fmt.Sprintf("rates/%v/%v/export.json.gz", d.Format("2006/01/02"), base)
				log.Println(filename)
				storageClient.Store(bucketName, filename, content)
				wg.Done()
			}(d, base, wg)

		}
		wg.Wait()
	}
	bqClient := bigquery.NewBQService(googleProject, googleKey)
	schema := `
{
	"fields": [
		{ "name": "date", "type": "DATE", "mode": "NULLABLE", "description": "Date of the measurement" },
		{ "name": "base", "type": "STRING", "mode": "NULLABLE", "description": "Base currency (ISO Code)" },
		{ "name": "currency", "type": "STRING", "mode": "NULLABLE", "description": "Currency compared to (ISO Code)" },
		{ "name": "units_per_currency", "type": "FLOAT", "mode": "NULLABLE", "description": "Units per currency. Amout in currency = Amout in base / UPC" },
		{ "name": "currencies_per_unit", "type": "FLOAT", "mode": "NULLABLE", "description": "Currencies per unit. Amout in currency = Amout in base * CPU" }
	]
}
	`
	r := bqClient.NewJob(bqDataset, bqTable, fmt.Sprintf("gs://%v/rates/*", bucketName), schema).Do()
	catchError(r.Error)

}
