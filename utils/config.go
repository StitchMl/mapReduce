package utils

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
)

// Node Define the structure to represent a node
type Node struct {
	Name string `json:"name"`
	IP   string `json:"ip"`
	Port string `json:"port"`
}

// Master Define configuration structure of master nodes
type Master struct {
	Nodes []Node `json:"master"`
}

// Mapper Define configuration structure of mapper nodes
type Mapper struct {
	Nodes []Node `json:"mapper"`
}

// Reducer Define configuration structure of reducer nodes
type Reducer struct {
	Nodes []Node `json:"reducer"`
}

// Config Define configuration structure
type Config struct {
	Master  Master
	Mapper  Mapper
	Reducer Reducer
}

// LoadConfig Function for loading configuration from JSON file
func LoadConfig(filename string) (*Config, error) {
	// Open the file
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Printf(ColoredText(RedBold, "LoadConfig: "+err.Error()))
		}
	}(file)

	// Read the contents of the file
	bytes, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	// Decode the JSON
	var master Master
	var mapper Mapper
	var reducer Reducer
	if err := json.Unmarshal(bytes, &master); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(bytes, &mapper); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(bytes, &reducer); err != nil {
		return nil, err
	}

	var config = Config{master, mapper, reducer}

	return &config, nil
}

// PrintConfig Function to print the configuration in a readable format
func PrintConfig(config *Config) {
	var name = "- Name: "
	var port = ", Port: "
	var ip = ", IP: "
	fmt.Println(ColoredText(YELLOW, "Master Nodes:"))
	for _, node := range config.Master.Nodes {
		fmt.Printf(ColoredText(BLUE, name+node.Name+ip+node.IP+port+node.Port+"\n"))
	}

	fmt.Println(ColoredText(YELLOW, "\nMapper Nodes:"))
	for _, node := range config.Mapper.Nodes {
		fmt.Printf(ColoredText(BLUE, name+node.Name+ip+node.IP+port+node.Port+"\n"))
	}

	fmt.Println(ColoredText(YELLOW, "\nReducer Nodes:"))
	for _, node := range config.Reducer.Nodes {
		fmt.Printf(ColoredText(BLUE, name+node.Name+ip+node.IP+port+node.Port+"\n"))
	}
}
