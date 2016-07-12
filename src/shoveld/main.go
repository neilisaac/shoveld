package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"runtime"
	"sync"
	"os"
)

func OpenFile(filename string) io.Reader {
	reader, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	return reader
}

func main() {
	threads := flag.Int("threads", runtime.NumCPU(), "set GOMAXPROCS")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s: filenames..\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	
	runtime.GOMAXPROCS(*threads)
	
	files := flag.Args()
	if len(files) == 0 {
		log.Fatal("no config files specified")
		flag.Usage()
		os.Exit(1)
	}
	
	shovels := make([]ShovelConfig, len(files))
	for i, f := range files {
		shovels[i] = ParseShovel(OpenFile(f))
	}
	
	var wg sync.WaitGroup
	wg.Add(len(shovels))
	
	for _, shovel := range shovels {
		log.Println("initializing", shovel.Name)
		worker := Worker{Config: shovel}
		worker.Init()
		
		go func() {
			defer wg.Done()
			worker.Work()
		}()
	}
	
	wg.Wait()
}
