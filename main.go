package main

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func main() {
	defer timeTrack(time.Now(), "datamerge")

	// input folder
	inFolder := flag.String("input", "", "input folder")
	// output folder
	outFolder := flag.String("output", "", "output folder")
	// output step length
	stepSize := flag.Int("step", 500000, "step size")
	// concurrency
	concurrency := flag.Int("concurrency", 10, "concurrency")
	// delimiters
	delimiters := flag.String("delimiters", ":|;", "delimiters")

	flag.Parse()

	if _, err := os.Stat(*outFolder); os.IsNotExist(err) {
		// path/to/whatever does not exist
		log.Fatalf("Error: outfolder %s does not exist", *outFolder)
	}

	if _, err := os.Stat(*inFolder); os.IsNotExist(err) {
		// path/to/whatever does not exist
		log.Fatalf("Error: infolder %s does not exist", *inFolder)
	}

	re, err := regexp.Compile("^(.*?)["+ *delimiters +"](.*)$")
	if err != nil {
		log.Fatalf("Could not compile delimiter regex.")
	}

	out := make(chan string, 1000)
	done := make(chan bool, 1)
	go fileWriter(*outFolder, *stepSize, out, done)

	sem := make(chan bool, *concurrency)
	err = filepath.Walk(*inFolder,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				log.Fatalf("Error reading %s: %v", path, err)
				return nil
			}
			if info.IsDir() {
				return nil
			}
			log.Printf("reading %s, %v", path, info.Size())
			sem <- true
			go func(path string) {
				defer func() { <-sem }()
				process(path, &out, re)
			}(path)

			return nil
		})
	log.Print("Starting closing handler.")
	for i := 0; i < cap(sem); i++ {
		sem <- true
	}
	log.Print("Closing output channel.")
	close(out)
	if err != nil {
		log.Println(err)
	}

	<-done
}

func save(file string, outBuffer *[]string) {
	log.Printf("Saving %s", file)
	res := ""
	for _, line := range *outBuffer {
		res += line + "\n"
	}
	res = strings.TrimSuffix(res, "\n")
	err := ioutil.WriteFile(file, []byte(res), os.FileMode(0777))
	if err != nil {
		log.Fatalf("Error writing %s: %v", file, err)
	} else {
		log.Printf("Saved %s", file)
	}
}

func process(file string, out *chan string, delimiters *regexp.Regexp) {

	dat, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatalf("Cannot read file %s", file)
		return
	}
	raw := string(dat)
	dat = nil
	lines := strings.Split(raw, "\n")
	raw = ""

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			*out <- delimiters.ReplaceAllString(line, "${1}:$2")
		}
	}
	log.Printf("Done reading %s", file)

}


func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

func fileWriter(outFolder string, stepSize int, out chan string, done chan bool) {
	log.Print("Starting file writer.")

	currentFile := 0
	currLine := 0
	currFile, err := os.Create(outFolder + "output-" + strconv.Itoa(currentFile) + ".txt")
	defer currFile.Close()
	currentFile++;
	if err != nil {
		log.Fatalf("Fatal Error: %v", err)
	}

	for {
		line, more := <-out

		_, err = currFile.WriteString(line + "\n")
		if err != nil {
			log.Fatalf("Fatal Error: %v", err)
		}
		currLine++;

		if currLine % stepSize == 0 {
			err = currFile.Close()
			currFile, err = os.Create(outFolder + "output-" + strconv.Itoa(currentFile) + ".txt")
			currentFile++;
		}

		if !more {
			break;
		}
	}
	log.Print("Done writing.")

	done<-true

}