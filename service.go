package elastic_journald

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os/exec"
	"regexp"
	"strings"
	"time"
	"unsafe"

	"github.com/liquidm/elastigo/lib"
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
	Indexer *elastigo.BulkIndexer

	messageFieldsRe *regexp.Regexp
}

func NewService() *Service {
	config := &Config{
		Hosts:       elasticHosts,
		IndexPrefix: *elasticPrefix,
	}

	elastic := elastigo.NewConn()
	indexer := elastic.NewBulkIndexerErrors(2, 30)
	indexer.BufferDelayMax = time.Duration(30) * time.Second
	indexer.BulkMaxDocs = 1000
	indexer.BulkMaxBuffer = 65536
	indexer.Sender = func(buf *bytes.Buffer) error {
		respJson, err := elastic.DoCommand("POST", "/_bulk", nil, buf)
		if err != nil {
			// TODO
			panic(fmt.Sprintf("Bulk error: \n%v", err))
		} else {
			response := struct {
				Took   int64 `json:"took"`
				Errors bool  `json:"errors"`
				Items  []struct {
					Index struct {
						Id string `json:"_id"`
					} `json:"index"`
				} `json:"items"`
			}{}

			jsonErr := json.Unmarshal(respJson, &response)
			if jsonErr != nil {
				// TODO
				panic(jsonErr)
			}
			if response.Errors {
				// TODO
				fmt.Println(string(respJson))
				panic("elasticsearch reported errors on intake")
			}

			messagesStored := len(response.Items)
			lastStoredCursor := response.Items[messagesStored-1].Index.Id
			ioutil.WriteFile(*elasticCursorFile, []byte(lastStoredCursor), 0644)

			// this will cause a busy loop, don't do it
			// fmt.Printf("Sent %v entries\n", messagesStored)
		}
		return err
	}

	service := &Service{
		Config:  config,
		Elastic: elastic,
		Indexer: indexer,

		messageFieldsRe: regexp.MustCompile("indexed:([[:word:]]+)=([[:graph:]]+)"),
	}
	return service
}

func (s *Service) Run() {
	s.Elastic.SetHosts(s.Config.Hosts)

	s.InitJournal()
	s.ProcessStream(GetFQDN())
}

func (s *Service) ProcessStream(hostname *string) {
	s.Indexer.Start()
	defer s.Indexer.Stop()

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

	s.Indexer.Index(
		indexName,       // index
		"journal",       // type
		cursorId,        // id
		"",              // parent
		"",              // ttl
		nil,             // date
		string(message), // content
	)
}

func (s *Service) ProcessEntryFields(row map[string]interface{}) {
	var length C.ulong
	var cData *C.char

	for C.sd_journal_restart_data(s.Journal); C.sd_journal_enumerate_data(s.Journal, (*unsafe.Pointer)(unsafe.Pointer(&cData)), &length) > 0; {
		data := C.GoStringN(cData, C.int(length))

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
		case "message":
			row[key] = value
			match := s.messageFieldsRe.FindAllStringSubmatch(value, -1)
			for _, m := range match {
				row[m[1]] = m[2]
			}
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

	bytes, err := ioutil.ReadFile(*elasticCursorFile)
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

var elasticCursorFile = flag.String("cursor", ".elastic_journal_cursor", "The file to keep cursor state between runs")
var elasticHosts elasticHostsType
var elasticPrefix = flag.String("prefix", "journald", "The index prefix to use")

func init() {
	flag.Var(&elasticHosts, "hosts", "comma-separated list of elastic (target) hosts")
}
