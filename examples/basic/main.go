package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/humanjuan/acacia/v2"
)

func main() {
	basicTest()
	// stressTest()
	// stressByteTest()
	//  stressJSONTest()
	// mixContentTest()
}

func basicTest() {
	lg, err := acacia.Start("acacia.log", "./examples", acacia.Level.DEBUG)
	if err != nil {
		panic(err)
	}
	defer lg.Close()

	lg.TimestampFormat(acacia.TS.Special)

	data1 := 2022
	data2 := []string{"dog", "cat", "fish", "bird"}

	lg.Critical("This is a Critical message %d", 2023)
	lg.Critical("Simple log message without variables")
	lg.Critical("Data: %v, Value: %v", data1, data2)
	lg.Critical(1)
	lg.Critical(1.2)
	lg.Critical(true)
	lg.Critical("% X", []byte{1, 2, 3})
	lg.Critical(map[string]interface{}{})

	lg.Info("This is a Critical message %d", 2023)
	lg.Info("Simple log message without variables")
	lg.Info("Data: %v, Value: %v", data1, data2)
	lg.Info(1)
	lg.Info(1.0)
	lg.Info(true)
	lg.Info("% X", []byte{1, 2, 3})
	lg.Info(map[string]interface{}{})

	lg.Warn("This is a Critical message %d", 2023)
	lg.Warn("Simple log message without variables")
	lg.Warn("Data: %v, Value: %v", data1, data2)
	lg.Warn(1)
	lg.Warn(1.0)
	lg.Warn(true)
	lg.Warn("% X", []byte{1, 2, 3})
	lg.Warn(map[string]interface{}{})

	lg.Error("This is a Critical message %d", 2023)
	lg.Error("Simple log message without variables")
	lg.Error("Data: %v, Value: %v", data1, data2)
	lg.Error(1)
	lg.Error(1.0)
	lg.Error(true)
	lg.Error("% X", []byte{1, 2, 3})
	lg.Error(map[string]interface{}{})

	lg.Debug("This is a Critical message %d", 2023)
	lg.Debug("Simple log message without variables")
	lg.Debug("Data: %v, Value: %v", data1, data2)
	lg.Debug(1)
	lg.Debug(1.0)
	lg.Debug(true)
	lg.Debug("% X", []byte{1, 2, 3})
	lg.Debug(map[string]interface{}{})

	// Optional rotation configuration (defaults are 40MB size, 4 backups)
	// lg.Rotation(80, 5)

	// lg.Close()
}

func stressTest() {
	lg, err := acacia.Start("acacia.log", "./examples", acacia.Level.INFO)
	if err != nil {
		panic(err)
	}

	defer lg.Close()

	lg.TimestampFormat(acacia.TS.Special)
	const workers = 500
	const messagesPerWorker = 10_000

	var wg sync.WaitGroup
	wg.Add(workers)

	start := time.Now()

	for id := 1; id <= workers; id++ {
		go func(workerID int) {
			defer wg.Done()

			for i := 1; i <= messagesPerWorker; i++ {
				lg.Info("Goroutine %02d → msg #%05d", workerID, i)
			}
		}(id)
	}

	wg.Wait()

	elapsed := time.Since(start)
	lg.Info("Test concurrente finalizado en %s", elapsed)

	fmt.Printf("DONE: %d goroutines × %d mensajes = %d líneas\n",
		workers, messagesPerWorker, workers*messagesPerWorker)
}

func stressByteTest() {
	lg, err := acacia.Start("acacia.log", "./examples", acacia.Level.INFO)
	if err != nil {
		panic(err)
	}

	defer lg.Close()

	lg.TimestampFormat(acacia.TS.Special)
	const workers = 500
	const messagesPerWorker = 10_000

	var wg sync.WaitGroup
	wg.Add(workers)

	start := time.Now()

	for id := 1; id <= workers; id++ {
		go func(workerID int) {
			defer wg.Done()

			for i := 1; i <= messagesPerWorker; i++ {
				lg.InfoBytes([]byte(fmt.Sprintf("Goroutine %02d → msg #%05d", workerID, i)))
			}
		}(id)
	}

	wg.Wait()

	elapsed := time.Since(start)
	lg.Info([]byte(fmt.Sprintf("Test concurrente finalizado en %s", elapsed)))

	fmt.Printf("DONE: %d goroutines × %d mensajes = %d líneas\n",
		workers, messagesPerWorker, workers*messagesPerWorker)
}

func stressJSONTest() {
	lg, err := acacia.Start("acacia.json", "./examples", acacia.Level.INFO)
	lg.StructuredJSON(true)
	lg.Rotation(5, 5)
	if err != nil {
		panic(err)
	}

	defer lg.Close()

	lg.TimestampFormat(acacia.TS.Special)
	const workers = 500
	const messagesPerWorker = 10_000

	var wg sync.WaitGroup
	wg.Add(workers)

	start := time.Now()

	for id := 1; id <= workers; id++ {
		go func(workerID int) {
			defer wg.Done()

			for i := 1; i <= messagesPerWorker; i++ {
				lg.Info(
					map[string]interface{}{
						"Goroutine": workerID,
						"msg":       i,
					})
			}
		}(id)
	}

	wg.Wait()

	elapsed := time.Since(start)
	lg.Info(
		map[string]interface{}{
			"msg": fmt.Sprintf("Test concurrente finalizado en %s", elapsed),
		})

	fmt.Printf("DONE: %d goroutines × %d mensajes = %d líneas\n",
		workers, messagesPerWorker, workers*messagesPerWorker)
}

func mixContentTest() {
	lg, err := acacia.Start("acacia.log", "./examples", acacia.Level.INFO)
	if err != nil {
		panic(err)
	}
	lg.Rotation(5, 3)
	defer lg.Close()

	data, err := os.ReadFile("./examples/basic/data.txt")
	if err != nil {
		lg.Error("Error al leer el archivo: %v", err)
	}
	msg := string(data)
	lg.TimestampFormat(acacia.TS.Special)
	const workers = 100
	const messagesPerWorker = 1000

	var wg sync.WaitGroup
	wg.Add(workers)

	start := time.Now()

	for id := 1; id <= workers; id++ {
		go func(workerID int) {
			defer wg.Done()

			for i := 1; i <= messagesPerWorker; i++ {
				lg.Info(msg)
			}
		}(id)
	}

	wg.Wait()

	elapsed := time.Since(start)
	lg.Info("Test concurrente finalizado en %s", elapsed)
	//lg.Info(msg)

}
