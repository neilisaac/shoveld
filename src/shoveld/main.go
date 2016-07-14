package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
)

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
		reader, err := os.Open(f)
		if err != nil {
			log.Fatal(err)
		}
		shovels[i] = ParseShovel(reader)
	}

	var wg sync.WaitGroup
	wg.Add(len(shovels))

	for _, shovel := range shovels {
		log.Println("initializing", shovel.Name)

		wg.Add(shovel.Concurrency)

		for i := 0; i < shovel.Concurrency; i++ {
			worker := Worker{ShovelConfig: shovel}
			worker.Name = fmt.Sprintf("%s [%d]", worker.Name, i+1)
			worker.Init()
			go func() {
				defer wg.Done()
				worker.Work()
			}()
		}
	}

	log.Println("workers are up and running")

	wg.Wait()
}
