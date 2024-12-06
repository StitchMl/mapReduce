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
	"strconv"
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
		list = append(list, r.Int()) // Add a random int32
	}
	for _, masterConfig := range config.Master.Nodes {
		err := master.Master(config, masterConfig.Port, list)
		if err != nil {
			log.Fatalf("Error loading Master: %v", err)
			return
		}
		fmt.Println(utils.ColoredText(utils.GREEN, "Master "+masterConfig.Name+"is started...\n"))
	}
}

func startMapper(wg *sync.WaitGroup, config utils.Config, mapperID int) {
	defer wg.Done()
	fmt.Printf(utils.ColoredText(utils.CYAN, "Starting Mapper "+strconv.Itoa(mapperID)+"...\n"))
	// Logic to start the mapper with specific ID
	err := mapper.Mapper(config.Mapper.Nodes[mapperID], config.Master.Nodes[0])
	if err != nil {
		log.Fatalf("Error loading %s: %v", config.Mapper.Nodes[mapperID].Name, err)
		return
	}
	fmt.Println(utils.ColoredText(utils.GREEN, "Mapper "+config.Mapper.Nodes[mapperID].Name+"is started...\n"))
}

func startReducer(wg *sync.WaitGroup, config utils.Config, reducerID int) {
	defer wg.Done()
	fmt.Printf(utils.ColoredText(utils.CYAN, "Starting Reducer "+strconv.Itoa(reducerID)+"...\n"))
	// Logic to start the reducer with specific ID
	err := reducer.Reducer(config.Reducer.Nodes[reducerID], config.Master.Nodes[0])
	if err != nil {
		log.Fatalf("Error loading %s: %v", config.Reducer.Nodes[reducerID].Name, err)
		return
	}
	fmt.Println(utils.ColoredText(utils.GreenBold, "Reducer "+config.Reducer.Nodes[reducerID].Name+"is started...\n"))
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
	for i, configMapper := range config.Mapper.Nodes {
		fmt.Println(utils.ColoredText(utils.PURPLE, "Starting "+configMapper.Name+"...\n"))
		wg.Add(1)
		go startMapper(&wg, *config, i)
	}

	// Start the reducers
	fmt.Println(utils.ColoredText(utils.MAGENTA, "Starting the reducers...\n"))
	for i, configReducer := range config.Reducer.Nodes {
		fmt.Println(utils.ColoredText(utils.PURPLE, "Starting "+configReducer.Name+"...\n"))
		wg.Add(1)
		go startReducer(&wg, *config, i)
	}

	// Wait for all goroutines to finish
	wg.Wait()
	fmt.Println("Application completed.")
}
