package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

/*
 * Complete the 'avgRotorSpeed' function below.
 *
 * URL for cut and paste
 * https://jsonmock.hackerrank.com/api/iot_devices/search?status={statusQuery}&page={number}
 *
 * The function is expected to return an INTEGER.
 * The function accepts following parameters:
 *  1. STRING statusQuery
 *  2. INTEGER parentId
 */

// OperatingParams defines device IOT devices operating parameters.
// TODO: better naming after more insight
type OperatingParams struct {
	RotorSpeed    int64   `json:"rotorSpeed"`
	Slack         float64 `json:"slack"`
	RootThreshold float64 `json:"rootThreshold"`
}

// Asset defines IOT devices asset.
// TODO: better naming after more insight
type Asset struct {
	ID    int32  `json:"id"`
	Alias string `json:"alias"`
}

// Parent defines IOT device parent device.
// TODO: better naming after more insight
type Parent struct {
	ID    int32  `json:"id"`
	Alias string `json:"alias"`
}

// Device holds IOT device information
type Device struct {
	ID              int32            `json:"id"`
	Status          string           `json:"status"`
	OperatingParams *OperatingParams `json:"operatingParams"`
	Asset           *Asset           `json:"asset"`
	Parent          *Parent          `json:"parent"`
}

// APIResponse defines the response of the API:
// https://jsonmock.hackerrank.com/api/iot_devices/search?status={statusQuery}&page={number}
type APIResponse struct {
	Page       int32     `json:"page"`
	PerPage    int32     `json:"per_page"`
	TotalPages int32     `json:"total_pages"`
	Total      int32     `json:"total"`
	Data       []*Device `json:"data"`
}

func getURL(statusQuery string, page int32) string {
	return fmt.Sprintf(
		"https://jsonmock.hackerrank.com/api/iot_devices/search?status=%s&page=%d",
		statusQuery, page,
	)
}

func callHTTP(ctx context.Context, url string) (*APIResponse, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		url,
		nil,
	)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var apiResp APIResponse

	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, err
	}

	return &apiResp, nil
}

func filterDeviceByParent(devices []*Device, parentID int32) []*Device {
	fd := make([]*Device, 0, len(devices))

	for _, v := range devices {
		if v.Parent != nil && v.Parent.ID == parentID {
			fd = append(fd, v)
		}
	}

	return fd
}

func getIOTDeviceFromAPI(
	ctx context.Context,
	statusQuery string,
	wg *sync.WaitGroup,
	reqChan chan int32,
	resChan chan []*Device,
	parentID int32,
) error {
	defer wg.Done()

	for {
		select {
		case page, more := <-reqChan:
			if !more {
				return nil
			}

			apiResp, err := callHTTP(ctx, getURL(statusQuery, page))
			if err != nil {
				return err
			}

			resChan <- filterDeviceByParent(apiResp.Data, parentID)
		case <-ctx.Done():
			return nil
		}
	}
}

func getALLIOTDeviceFromAPI(
	ctx context.Context, statusQuery string, parentId int32,
) ([]*Device, error) {
	apiResp, err := callHTTP(ctx, getURL(statusQuery, 0))
	if err != nil {
		return nil, err
	}

	totalDevice := (apiResp.TotalPages * apiResp.PerPage) + 5
	ans := make([]*Device, 0, totalDevice)

	ans = append(ans, filterDeviceByParent(apiResp.Data, parentId)...)

	fmt.Println(totalDevice)

	// number of concurrency for API fetch
	numConcurrency := 5
	var wg sync.WaitGroup
	reqChan := make(chan int32, numConcurrency)
	resChan := make(chan []*Device, numConcurrency)

	wg.Add(numConcurrency)

	// spawn worker
	for i := 0; i < numConcurrency; i++ {
		go getIOTDeviceFromAPI(ctx, statusQuery, &wg, reqChan, resChan, parentId)
	}

	go func() {
		for i := int32(2); i <= apiResp.TotalPages; i++ {
			fmt.Println("Pushed work: ", i)
			reqChan <- i
		}

		close(reqChan)
	}()

	go func() {
		wg.Wait()
		close(resChan)
	}()

	for devices := range resChan {
		ans = append(ans, devices...)
	}

	return ans, nil
}

func avgRotorSpeed(statusQuery string, parentId int32) int32 {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()

	devicses, err := getALLIOTDeviceFromAPI(ctx, statusQuery, parentId)
	if err != nil {
		log.Fatal("failed to get device from api: ", err)
	}

	lenDevices := int64(len(devicses))
	if lenDevices == 0 {
		return 0
	}

	totalRotorSpeed := int64(0)
	for _, v := range devicses {
		totalRotorSpeed += v.OperatingParams.RotorSpeed
	}

	return int32(totalRotorSpeed / lenDevices)
}

func main() {
	fmt.Println(avgRotorSpeed("RUNNING", 7))
}
