package elastic_journald

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"time"
	"unsafe"
)

// #include <stdio.h>
// #include <string.h>
// #include <systemd/sd-journal.h>
// #cgo LDFLAGS: -lsystemd
import "C"

type Config struct {
}

type Service struct {
	Config  *Config
	Journal *C.sd_journal
	Cursor  string
}

func NewService() *Service {
	config := &Config{}

	service := &Service{Config: config}

	return service
}

func (s *Service) Run() {
	s.InitJournal()
	s.ProcessStream()
}

func (s *Service) ProcessStream() {
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
		s.ProcessEntry()
	}
}

func (s *Service) ProcessEntry() {
	var realtime C.uint64_t
	r := C.sd_journal_get_realtime_usec(s.Journal, &realtime)
	if r < 0 {
		panic(fmt.Sprintf("failed to get realtime timestamp: %s", C.strerror(-r)))
	}

	var monotonic C.uint64_t
	var boot_id C.sd_id128_t
	r = C.sd_journal_get_monotonic_usec(s.Journal, &monotonic, &boot_id)
	if r < 0 {
		panic(fmt.Sprintf("failed to get monotonic timestamp: %s", C.strerror(-r)))
	}

	var cursor *C.char
	r = C.sd_journal_get_cursor(s.Journal, &cursor)
	if r < 0 {
		panic(fmt.Sprintf("failed to get cursor: %s", C.strerror(-r)))
	}

	var sid = "1234567890abcdefghijklmnopqrstuvwxyz" // 33+ chars
	message := fmt.Sprintf(`{"ts":"%s","__CURSOR":"%s","__MONOTONIC_TIMESTAMP":"%d","_BOOT_ID":"%s"%s}`,
		time.Unix(int64(realtime/1000000), int64(realtime%1000000)).UTC().Format("2006-01-02T15:04:05Z"),
		C.GoString(cursor), monotonic,
		C.GoString(C.sd_id128_to_string(boot_id, C.CString(sid))),
		s.ProcessEntryFields())

	fmt.Println(message)
	ioutil.WriteFile(".elastic_journal_cursor", []byte(C.GoString(cursor)), 0644)
}

func (s *Service) ProcessEntryFields() string {
	var counts map[string]int = make(map[string]int)
	var length C.size_t
	var cData *C.char

	for C.sd_journal_restart_data(s.Journal); C.sd_journal_enumerate_data(s.Journal, (*unsafe.Pointer)(unsafe.Pointer(&cData)), &length) > 0; {
		data := C.GoString(cData)
		parts := strings.Split(data, "=")
		counts[parts[0]] += 1
	}

	separator := true
	var buf bytes.Buffer

	for C.sd_journal_restart_data(s.Journal); C.sd_journal_enumerate_data(s.Journal, (*unsafe.Pointer)(unsafe.Pointer(&cData)), &length) > 0; {
		data := C.GoString(cData)
		parts := strings.SplitN(data, "=", 2)

		if parts[0] == "_BOOT_ID" {
			continue
		}

		if separator {
			buf.WriteString(", ")
		}

		count := counts[parts[0]]

		if count == 0 {
			separator = false
			continue
		} else if count == 1 {
			key, _ := json.Marshal(parts[0])
			value, _ := json.Marshal(parts[1])
			buf.WriteString(fmt.Sprintf("%s:%s", key, value))
			separator = true
			continue
		} else {
			panic(fmt.Sprintf("unsupported multiple field: %s", parts[0]))
		}
	}

	return buf.String()
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
