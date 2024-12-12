package main

import (
	"flag"
	"fmt"
	"log"
	"mapReduce/mapper"
	"mapReduce/master"
	"mapReduce/reducer"
	"mapReduce/utils"
	"math/rand"
	"sync"
	"time"
)

// Start-up functions for the various nodes
func startMaster(wg *sync.WaitGroup, config utils.Config) {
	defer wg.Done()
	fmt.Println(utils.ColoredText(utils.CYAN, "Starting Master..."))
	// Logic to start the master with the specific configuration
	// Initialises the random number generator with the current time-based seed
	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src) // Create a random number generator with the source

	// Creates an empty list of type int32
	var list []int

	// Add n random numbers to the list
	for i := 0; i < 9; i++ {
		list = append(list, r.Intn(1000)) // Add a random int32
	}
	for _, masterConfig := range config.Master.Nodes {
		err := master.Master(config, list)
		if err != nil {
			log.Fatalf("Error loading Master: %v", err)
			return
		}
		fmt.Println(utils.ColoredText(utils.GREEN, "Master "+masterConfig.Name+"is started...\n"))
	}
}

func startMapper(wg *sync.WaitGroup, config utils.Config) {
	defer wg.Done()
	fmt.Printf(utils.ColoredText(utils.CYAN, "Starting Mappers...\n"))
	// Logic to start the mapper with specific ID
	err := mapper.Mapper(config)
	if err != nil {
		log.Fatalf("Error loading Mappers: %v", err)
		return
	}
	fmt.Println(utils.ColoredText(utils.GREEN, "Mappers are started...\n"))
}

func startReducer(wg *sync.WaitGroup, config utils.Config) {
	defer wg.Done()
	fmt.Printf(utils.ColoredText(utils.CYAN, "Starting Reducers...\n"))

	red := config.Reducer
	var maxNum []int
	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)
	for i := 0; i < len(red.Nodes)-1; i++ {
		maxNum = append(maxNum, r.Intn(1000))
	}
	maxNum = append(maxNum, 1000)

	for i := range red.Nodes {
		minN := 0
		if i > 0 {
			minN = maxNum[i-1]
		}
		go func(idx, minN, maxN int) {
			err := reducer.Reducer(config, idx, minN, maxN)
			if err != nil {
				log.Fatalf("Error loading Reducer: %v", err)
			}
		}(i, minN, maxNum[i])
	}
	fmt.Println(utils.ColoredText(utils.GreenBold, "Reducers are started...\n"))
}

func main() {
	// Parsing flags once
	flag.Parse() // This is only called once in the programme

	// Upload configuration
	config, err := utils.LoadConfig("config.json")
	if err != nil {
		log.Fatalf("Error loading configuration: %v", err)
	}

	utils.PrintConfig(config)

	var wg sync.WaitGroup

	// Start the Master
	wg.Add(1)
	go startMaster(&wg, *config)

	// Start the mappers
	fmt.Println(utils.ColoredText(utils.MAGENTA, "Starting the mappers...\n"))
	wg.Add(1)
	go startMapper(&wg, *config)

	// Start the reducers
	fmt.Println(utils.ColoredText(utils.MAGENTA, "Starting the reducers...\n"))
	wg.Add(1)
	go startReducer(&wg, *config)

	// Wait for all goroutines to finish
	wg.Wait()
	fmt.Println("Application completed.")
}
