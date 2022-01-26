package main

/*
	incoming messages are persisted as k, v



*/

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/boltdb/bolt"
	"github.com/golang/glog"
)

func sigHandler() <-chan struct{} {
	termCh := make(chan struct{})
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c,
			syscall.SIGINT,  // Ctrl+C
			syscall.SIGTERM, // Termination Request
			syscall.SIGSEGV, // FullDerp
			syscall.SIGABRT, // Abnormal termination
			syscall.SIGILL,  // illegal instruction
			syscall.SIGFPE)  // floating point - this is why we can't have nice things
		sig := <-c
		glog.Warningf("Signal (%v) Detected, Shutting Down", sig)
		close(termCh)
	}()
	return termCh
}

func loadConfig() {
	flag.Parse()
}

var (
	termCh = sigHandler()
	db     *bolt.DB
)

func main() {
	var err error

	loadConfig()
	if db, err = bolt.Open("my.db", 0600, &bolt.Options{
		Timeout:         1 * time.Second,
		NoGrowSync:      false,
		ReadOnly:        false,
		MmapFlags:       0,
		InitialMmapSize: 0,
	}); err != nil {
		glog.Fatalf("open DB failed", err, "my.db")
	}
	defer db.Close()

	<-termCh
	fmt.Println("bye")
}