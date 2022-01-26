package bin

/*
	incoming messages are persisted as k, v



*/

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/golang/glog"
)

func SigHandler() <-chan struct{} {
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

// func loadConfig() {
// 	flag.Parse()
// }

// var (
// 	termCh = sigHandler()
// 	db     *bolt.DB
// )

// func main() {
// 	var err error

// 	loadConfig()
// 	if db, err = bolt.Open("local.my.db", 0600, &bolt.Options{
// 		Timeout:         1 * time.Second,
// 		NoGrowSync:      false,
// 		ReadOnly:        false,
// 		MmapFlags:       0,
// 		InitialMmapSize: 0,
// 	}); err != nil {
// 		glog.Fatalf("open DB %s failed: %s", "my.db", err)
// 	}
// 	defer db.Close()

// 	<-termCh
// 	fmt.Println("bye")
// }
