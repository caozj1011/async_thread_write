package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/apache/iotdb-client-go/client"
)

var (
	host     string
	port     string
	user     string
	password string
)

var wgMain sync.WaitGroup

func main() {
	wgMain.Add(1)
	testSession()
	// wg.Add(1)
	// testPool()
	wgMain.Wait()
	fmt.Printf("end of main\n")
}

func testSession() {
	flag.StringVar(&host, "host", "127.0.0.1", "--host=192.168.1.100")
	flag.StringVar(&port, "port", "6667", "--port=6667")
	flag.StringVar(&user, "user", "root", "--user=root")
	flag.StringVar(&password, "password", "root", "--password=root")
	flag.Parse()
	config := &client.Config{
		Host:     host,
		Port:     port,
		UserName: user,
		Password: password,
	}
	session := client.NewSession(config)
	if err := session.Open(false, 0); err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	defer wgMain.Done()

	var wgSingle sync.WaitGroup

	// wgSingle.Add(1)
	// go createTSInSingle(&session, &wgSingle)
	// wgSingle.Add(1)
	// go deleteTSInSingle(&session, &wgSingle)
	wgSingle.Add(1)
	go insertTSInSingle(&session, &wgSingle, "s01")
	time.Sleep(time.Duration(50) * time.Millisecond)
	
	// wgSingle.Add(1)
	// go insertTSInSingle(&session, &wgSingle, "s02")

	wgSingle.Wait()
}

func createTSInSingle(s *client.Session, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Printf("Create starting\n")
	var (
		dataType   = client.FLOAT
		encoding   = client.PLAIN
		compressor = client.SNAPPY
	)

	for i := 0; i <= 100; i++ {
		var pathString string
		pathString = "root.db.device.s" + strconv.FormatInt(int64(i), 10)
		fmt.Printf("Create %s starting... ", pathString)
		_, err := s.CreateTimeseries(pathString, dataType, encoding, compressor, nil, nil)
		if err == nil {
			fmt.Printf("Create %s done.\n", pathString)
		} else {
			fmt.Printf("Create %s ERROR[%v].\n", pathString, err)
		}
		time.Sleep(time.Duration(5) * time.Millisecond)
	}
}

func deleteTSInSingle(s *client.Session, wg *sync.WaitGroup) {
	defer wg.Done()
	time.Sleep(time.Duration(20) * time.Millisecond)
	fmt.Printf("Delete starting\n")
	for i := 0; i <= 100; i++ {
		var pathString string
		pathString = "root.db.device.s" + strconv.FormatInt(int64(i), 10)
		fmt.Printf("Delete %s starting... ", pathString)
		_, err := s.DeleteTimeseries([]string{pathString})
		if err == nil {
			fmt.Printf("Delete %s done.\n", pathString)
		} else {
			fmt.Printf("Delete %s ERROR[%v].\n", pathString, err)
		}
		time.Sleep(time.Duration(5) * time.Millisecond)
	}
}

func insertTSInSingle(s *client.Session, wg *sync.WaitGroup, node string) {
	defer wg.Done()
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("\n Catch an error: %v", err)
		}
	}()
	fmt.Printf("Insert starting\n")
	// pathString := "root.db.device." + node
	for i := 0; i <= 10000; i++ {
		fmt.Printf("Insert %s [%d] starting... ", node, i)
		_, err := s.InsertRecord(
			"root.db.device", []string{node}, []client.TSDataType{client.FLOAT},
			[]interface{}{float32(i)}, int64(i),
		)
		if err == nil {
			fmt.Printf("Insert %s [%d] done.\n", node, i)
		} else {
			fmt.Printf("Insert %s [%d] ERROR[%v].\n", node, i, err)
		}
		time.Sleep(time.Duration(5) * time.Millisecond)
	}
}

func testPool() {
	flag.StringVar(&host, "host", "127.0.0.1", "--host=192.168.1.100")
	flag.StringVar(&port, "port", "6667", "--port=6667")
	flag.StringVar(&user, "user", "root", "--user=root")
	flag.StringVar(&password, "password", "root", "--password=root")
	flag.Parse()
	config := &client.PoolConfig{
		Host:     host,
		Port:     port,
		UserName: user,
		Password: password,
	}
	sessionPool := client.NewSessionPool(config, 3, 60000, 60000, false)

	defer sessionPool.Close()
	defer wgMain.Done()

	var wgPool sync.WaitGroup
	wgPool.Add(1)

	go createTSInPool(&sessionPool, &wgPool)
	// go deleteTSInSingle()

	wgPool.Wait()
}

func createTSInPool(spool *client.SessionPool, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Printf("Create starting\n")
	var (
		dataType   = client.FLOAT
		encoding   = client.PLAIN
		compressor = client.SNAPPY
	)
	session, _ := spool.GetSession()
	for i := 0; i <= 100; i++ {
		var pathString string
		pathString = "root.db.device.s" + strconv.FormatInt(int64(i), 10)
		session.CreateTimeseries(pathString, dataType, encoding, compressor, nil, nil)
		fmt.Printf("Create %s\n", pathString)
		time.Sleep(time.Duration(2000) * time.Millisecond)
	}
	spool.PutBack(session)
}
