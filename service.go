package elastic_journald

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os/exec"
	"strings"
	"time"
	"unsafe"

	"github.com/mattbaird/elastigo/lib"
)

// #include <stdio.h>
// #include <string.h>
// #include <systemd/sd-journal.h>
// #cgo LDFLAGS: -lsystemd
import "C"

type Config struct {
	Hosts       elasticHostsType
	IndexPrefix string
}

type Service struct {
	Config  *Config
	Journal *C.sd_journal
	Cursor  string
	Elastic *elastigo.Conn
}

func NewService() *Service {
	config := &Config{
		Hosts:       elasticHosts,
		IndexPrefix: *elasticPrefix,
	}

	service := &Service{
		Config:  config,
		Elastic: elastigo.NewConn(),
	}
	return service
}

func (s *Service) Run() {
	s.Elastic.SetHosts(s.Config.Hosts)

	s.InitJournal()
	s.ProcessStream(GetFQDN())
}

func (s *Service) ProcessStream(hostname *string) {
	for {
		r := C.sd_journal_next(s.Journal)
		if r < 0 {
			panic(fmt.Sprintf("failed to iterate to next entry: %s", C.strerror(-r)))
		}
		if r == 0 {
			r = C.sd_journal_wait(s.Journal, 1000000)
			if r < 0 {
				panic(fmt.Sprintf("failed to wait for changes: %s", C.strerror(-r)))
			}
			continue
		}
		s.ProcessEntry(hostname)
	}
}

func (s *Service) ProcessEntry(hostname *string) {
	var realtime C.uint64_t
	r := C.sd_journal_get_realtime_usec(s.Journal, &realtime)
	if r < 0 {
		panic(fmt.Sprintf("failed to get realtime timestamp: %s", C.strerror(-r)))
	}

	var cursor *C.char
	r = C.sd_journal_get_cursor(s.Journal, &cursor)
	if r < 0 {
		panic(fmt.Sprintf("failed to get cursor: %s", C.strerror(-r)))
	}

	row := make(map[string]interface{})

	timestamp := time.Unix(int64(realtime/1000000), int64(realtime%1000000)).UTC()

	row["ts"] = timestamp.Format("2006-01-02T15:04:05Z")
	row["host"] = hostname
	s.ProcessEntryFields(row)

	message, _ := json.Marshal(row)
	indexName := fmt.Sprintf("%v-%v", s.Config.IndexPrefix, timestamp.Format("2006-01-02"))
	cursorId := C.GoString(cursor)

	_, err := s.Elastic.Index(indexName, "journal", cursorId, nil, string(message))
	if err != nil {
		panic(err)
	} else {
		ioutil.WriteFile(".elastic_journal_cursor", []byte(C.GoString(cursor)), 0644)
	}
}

func (s *Service) ProcessEntryFields(row map[string]interface{}) {
	var length C.size_t
	var cData *C.char

	for C.sd_journal_restart_data(s.Journal); C.sd_journal_enumerate_data(s.Journal, (*unsafe.Pointer)(unsafe.Pointer(&cData)), &length) > 0; {
		data := C.GoString(cData)

		parts := strings.SplitN(data, "=", 2)

		key := strings.ToLower(parts[0])
		value := parts[1]

		switch key {
		// don't index bloat
		case "_cap_effective":
		case "_cmdline":
		case "_exe":
		case "_hostname":
		case "_systemd_cgroup":
		case "_systemd_slice":
		case "_transport":
		case "syslog_facility":
		case "syslog_identifier":
			continue
		default:
			row[strings.TrimPrefix(key, "_")] = value
		}
	}
}

func (s *Service) InitJournal() {
	r := C.sd_journal_open(&s.Journal, C.SD_JOURNAL_LOCAL_ONLY)
	if r < 0 {
		panic(fmt.Sprintf("failed to open journal: %s", C.strerror(-r)))
	}

	bytes, err := ioutil.ReadFile(".elastic_journal_cursor")
	if err == nil {
		s.Cursor = string(bytes)
	}

	if s.Cursor != "" {
		r = C.sd_journal_seek_cursor(s.Journal, C.CString(s.Cursor))
		if r < 0 {
			panic(fmt.Sprintf("failed to seek journal: %s", C.strerror(-r)))
		}
		r = C.sd_journal_next_skip(s.Journal, 1)
		if r < 0 {
			panic(fmt.Sprintf("failed to skip current journal entry: %s", C.strerror(-r)))
		}
	}
}

func GetFQDN() *string {
	cmd := exec.Command("hostname", "-f")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return nil
	}
	fqdn := string(bytes.TrimSpace(out.Bytes()))
	return &fqdn
}

type elasticHostsType []string

func (e *elasticHostsType) String() string {
	return strings.Join(*e, ",")
}

func (e *elasticHostsType) Set(value string) error {
	for _, host := range strings.Split(value, ",") {
		*e = append(*e, host)
	}
	return nil
}

var elasticHosts elasticHostsType
var elasticPrefix = flag.String("prefix", "journald", "The index prefix to use")

func init() {
	flag.Var(&elasticHosts, "hosts", "comma-separated list of elastic (target) hosts")
}
