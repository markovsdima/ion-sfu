package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/gorilla/websocket"
	"github.com/pion/ion-sfu/pkg/middlewares/datachannel"
	"github.com/pion/ion-sfu/pkg/sfu"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sourcegraph/jsonrpc2"
	websocketjsonrpc2 "github.com/sourcegraph/jsonrpc2/websocket"

	"github.com/pion/ion-sfu/cmd/signal/json-rpc/server"

	log "github.com/pion/ion-sfu/pkg/logger"
)

var (
	conf        = sfu.Config{}
	file        string
	cert        string
	key         string
	addr        string
	metricsAddr string
	verbosity   int
	logger      = log.New()
)

func parseFlags() bool {
	flag.StringVar(&file, "c", "config.toml", "config file")
	flag.StringVar(&cert, "cert", "", "cert file")
	flag.StringVar(&key, "key", "", "key file")
	flag.StringVar(&addr, "a", ":7000", "address to listen")
	flag.StringVar(&metricsAddr, "m", ":8100", "metrics address")
	flag.IntVar(&verbosity, "v", -1, "verbosity level")
	help := flag.Bool("h", false, "help")
	flag.Parse()

	if *help {
		return false
	}
	return true
}

func startMetrics(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Handler: mux}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error(err, "cannot bind metrics")
		os.Exit(1)
	}
	logger.Info("Metrics listening", "addr", addr)
	_ = srv.Serve(lis)
}

func main() {
	if !parseFlags() {
		fmt.Println("Usage: ...")
		os.Exit(1)
	}

	log.SetGlobalOptions(log.GlobalConfig{V: verbosity})
	logger.Info("--- Starting SFU Node ---")

	s := sfu.NewSFU(conf)
	dc := s.NewDatachannel(sfu.APIChannelLabel)
	dc.Use(datachannel.SubscriberAPI)

	manager := server.NewConnectionManager(logger)

	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

	http.Handle("/ws", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			panic(err)
		}
		defer c.Close()

		p := server.NewJSONSignal(sfu.NewPeer(s), manager, logger)
		defer p.Close()

		jc := jsonrpc2.NewConn(r.Context(), websocketjsonrpc2.NewObjectStream(c), p)
		<-jc.DisconnectNotify()
	}))

	go startMetrics(metricsAddr)

	var err error
	if key != "" && cert != "" {
		logger.Info("Listening", "addr", "https://"+addr)
		err = http.ListenAndServeTLS(addr, cert, key, nil)
	} else {
		logger.Info("Listening", "addr", "http://"+addr)
		err = http.ListenAndServe(addr, nil)
	}
	if err != nil {
		panic(err)
	}
}
