package sink

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// SyslogFormat selects the message encoding.
type SyslogFormat string

const (
	// SyslogRFC5424 writes standard POSIX syslog messages, suitable for
	// rsyslog, syslog-ng, or a UDP/TCP collector.
	SyslogRFC5424 SyslogFormat = "rfc5424"
	// SyslogJournald writes the systemd-journald native protocol: one
	// newline-separated key=value block per record, terminated by a blank
	// line. It is only meaningful on a socket owned by journald.
	SyslogJournald SyslogFormat = "journald"
)

// SyslogFacility is a POSIX syslog facility number.
type SyslogFacility int

// The facilities prism can emit under. local0 is the default because it is
// reserved for locally defined use.
const (
	FacilityKern     SyslogFacility = 0
	FacilityUser     SyslogFacility = 1
	FacilityMail     SyslogFacility = 2
	FacilityDaemon   SyslogFacility = 3
	FacilityAuth     SyslogFacility = 4
	FacilitySyslog   SyslogFacility = 5
	FacilityLPR      SyslogFacility = 6
	FacilityNews     SyslogFacility = 7
	FacilityUUCP     SyslogFacility = 8
	FacilityCron     SyslogFacility = 9
	FacilityAuthPriv SyslogFacility = 10
	FacilityFTP      SyslogFacility = 11
	FacilityLocal0   SyslogFacility = 16
	FacilityLocal1   SyslogFacility = 17
	FacilityLocal2   SyslogFacility = 18
	FacilityLocal3   SyslogFacility = 19
	FacilityLocal4   SyslogFacility = 20
	FacilityLocal5   SyslogFacility = 21
	FacilityLocal6   SyslogFacility = 22
	FacilityLocal7   SyslogFacility = 23
)

// Syslog severities used by the sink. Records are informational; failed
// transactions and deployments are warnings, and diagnostic events are debug
// so a production collector can drop them without dropping activity.
const (
	severityDebug   = 7
	severityInfo    = 6
	severityWarning = 4
)

const (
	syslogStructuredDataID = "prism@32473"
	defaultSyslogApp       = "prism"
	defaultSyslogFacility  = "local0"
	defaultSyslogMessageID = "stream"
)

var facilityNames = map[string]SyslogFacility{
	"kern":     FacilityKern,
	"user":     FacilityUser,
	"mail":     FacilityMail,
	"daemon":   FacilityDaemon,
	"auth":     FacilityAuth,
	"syslog":   FacilitySyslog,
	"lpr":      FacilityLPR,
	"news":     FacilityNews,
	"uucp":     FacilityUUCP,
	"cron":     FacilityCron,
	"authpriv": FacilityAuthPriv,
	"ftp":      FacilityFTP,
	"local0":   FacilityLocal0,
	"local1":   FacilityLocal1,
	"local2":   FacilityLocal2,
	"local3":   FacilityLocal3,
	"local4":   FacilityLocal4,
	"local5":   FacilityLocal5,
	"local6":   FacilityLocal6,
	"local7":   FacilityLocal7,
}

// ParseSyslogFacility converts a facility name such as "local0" to its
// number. Numeric values are accepted too.
func ParseSyslogFacility(name string) (SyslogFacility, error) {
	value := strings.ToLower(strings.TrimSpace(name))
	if value == "" {
		value = defaultSyslogFacility
	}
	if facility, ok := facilityNames[value]; ok {
		return facility, nil
	}
	if number, err := strconv.Atoi(value); err == nil {
		if number >= 0 && number <= 23 {
			return SyslogFacility(number), nil
		}
	}
	return 0, fmt.Errorf("syslog: unknown facility %q", name)
}

// SyslogOptions configures a Syslog sink.
type SyslogOptions struct {
	// Writer receives each message. Required. A net.Conn from DialSyslog works
	// for a real syslog or journald socket; a buffer works in tests.
	Writer io.Writer
	// Format selects RFC 5424 or journald framing. Empty means RFC 5424.
	Format SyslogFormat
	// Facility is a POSIX facility name such as "local0". Empty uses local0.
	Facility string
	// AppName is the SYSLOG_IDENTIFIER / APP-NAME. Empty uses "prism".
	AppName string
	// Hostname is the RFC 5424 hostname. Empty asks the OS, falling back to
	// the RFC's NILVALUE "-".
	Hostname string
	// MessageID is the RFC 5424 MSGID used when a record's kind is not a
	// valid token. Empty uses "stream".
	MessageID string
	// Decode embeds decoded XDR in the JSON message, matching a stream run
	// without --raw.
	Decode bool
	// Now is the clock used for the syslog timestamp. Tests set it; nil means
	// time.Now.
	Now func() time.Time
}

// Syslog emits one message per record, formatted as RFC 5424 or as a
// systemd-journald native field block.
//
// A record is never split across messages: a collector sees either the whole
// JSON object or nothing. The JSON payload is the documented NDJSON shape, so
// a syslog pipeline can parse it exactly like a file or webhook stream.
//
// See docs/sinks/file_rotation_and_syslog.md for syslog, journald, and
// systemd setup examples.
type Syslog struct {
	opts     SyslogOptions
	format   SyslogFormat
	facility SyslogFacility
	appName  string
	hostname string
	msgID    string
	now      func() time.Time

	mu     sync.Mutex
	closed bool
}

// NewSyslog validates opts and builds the sink.
func NewSyslog(opts SyslogOptions) (*Syslog, error) {
	if opts.Writer == nil {
		return nil, fmt.Errorf("syslog: Writer is required")
	}

	format := opts.Format
	if format == "" {
		format = SyslogRFC5424
	}
	if format != SyslogRFC5424 && format != SyslogJournald {
		return nil, fmt.Errorf("syslog: unknown format %q", opts.Format)
	}

	facility, err := ParseSyslogFacility(opts.Facility)
	if err != nil {
		return nil, err
	}

	appName := opts.AppName
	if appName == "" {
		appName = defaultSyslogApp
	}
	if !validSyslogToken(appName) {
		return nil, fmt.Errorf("syslog: AppName %q must be printable ASCII without spaces", appName)
	}

	msgID := opts.MessageID
	if msgID == "" {
		msgID = defaultSyslogMessageID
	}
	if !validSyslogToken(msgID) {
		return nil, fmt.Errorf("syslog: MessageID %q must be printable ASCII without spaces", msgID)
	}

	hostname := opts.Hostname
	if hostname == "" {
		if name, hostErr := os.Hostname(); hostErr == nil {
			hostname = name
		} else {
			hostname = "-"
		}
	}

	now := opts.Now
	if now == nil {
		now = time.Now
	}

	return &Syslog{
		opts:     opts,
		format:   format,
		facility: facility,
		appName:  appName,
		hostname: hostname,
		msgID:    msgID,
		now:      now,
	}, nil
}

// Emit implements stream.Sink.
func (s *Syslog) Emit(r stream.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("syslog: emit after close")
	}

	message, err := s.formatRecord(r)
	if err != nil {
		return err
	}
	if _, err := s.opts.Writer.Write(message); err != nil {
		return fmt.Errorf("syslog: writing record %s: %w", r.ID, err)
	}
	return nil
}

// EmitBatch implements stream.BatchSink.
func (s *Syslog) EmitBatch(records []stream.Record) error {
	for _, r := range records {
		if err := s.Emit(r); err != nil {
			return err
		}
	}
	return nil
}

// Flush implements stream.Sink, syncing writers that support it.
func (s *Syslog) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if syncer, ok := s.opts.Writer.(interface{ Sync() error }); ok {
		if err := syncer.Sync(); err != nil {
			return fmt.Errorf("syslog: flushing: %w", err)
		}
	}
	return nil
}

// Close flushes and closes a writer that owns a resource, such as a socket
// from DialSyslog. It is safe to call more than once.
func (s *Syslog) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	var errs []error
	if syncer, ok := s.opts.Writer.(interface{ Sync() error }); ok {
		if err := syncer.Sync(); err != nil {
			errs = append(errs, fmt.Errorf("syslog: flushing: %w", err))
		}
	}
	if closer, ok := s.opts.Writer.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("syslog: closing writer: %w", err))
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("syslog: close: %w", errors.Join(errs...))
}

func (s *Syslog) formatRecord(r stream.Record) ([]byte, error) {
	payload, err := MarshalRecord(r, s.opts.Decode)
	if err != nil {
		return nil, fmt.Errorf("syslog: encoding record %s: %w", r.ID, err)
	}

	severity := severityFor(r)
	priority := int(s.facility)*8 + severity

	if s.format == SyslogJournald {
		return s.journaldMessage(r, payload, severity), nil
	}
	return s.rfc5424Message(r, payload, priority), nil
}

func (s *Syslog) rfc5424Message(r stream.Record, payload []byte, priority int) []byte {
	var b strings.Builder

	fmt.Fprintf(&b, "<%d>1 %s %s %s - %s [%s kind=%s ledger=%s id=%s] ",
		priority,
		s.now().UTC().Format("2006-01-02T15:04:05.000Z"),
		sanitizeSyslogValue(s.hostname),
		s.appName,
		s.messageID(r),
		syslogStructuredDataID,
		syslogQuote(string(r.Kind)),
		syslogQuote(strconv.FormatUint(uint64(r.Sequence), 10)),
		syslogQuote(r.ID),
	)
	b.Write(payload)
	// Newline framing keeps a TCP syslog stream parsable; datagram transports
	// simply carry the delimiter as the last byte of the message.
	b.WriteByte('\n')

	return []byte(b.String())
}

func (s *Syslog) journaldMessage(r stream.Record, payload []byte, severity int) []byte {
	var b strings.Builder

	fmt.Fprintf(&b, "PRIORITY=%d\n", severity)
	fmt.Fprintf(&b, "SYSLOG_IDENTIFIER=%s\n", s.appName)
	fmt.Fprintf(&b, "SYSLOG_FACILITY=%d\n", s.facility)
	fmt.Fprintf(&b, "PRISM_KIND=%s\n", string(r.Kind))
	fmt.Fprintf(&b, "PRISM_LEDGER=%d\n", r.Sequence)
	fmt.Fprintf(&b, "PRISM_RECORD_ID=%s\n", r.ID)
	if !r.Time.IsZero() {
		fmt.Fprintf(&b, "PRISM_LEDGER_TIME=%s\n", r.Time.UTC().Format(time.RFC3339))
	}
	writeJournaldField(&b, "MESSAGE", string(payload))
	// An empty line terminates one journald entry.
	b.WriteString("\n")

	return []byte(b.String())
}

// messageID maps a record kind to a stable RFC 5424 MSGID. A kind that is not
// a valid token (for example from a future source) falls back to the
// configured default rather than producing an unparseable header.
func (s *Syslog) messageID(r stream.Record) string {
	kind := string(r.Kind)
	if kind != "" && validSyslogToken(kind) && len(kind) <= 32 {
		return kind
	}
	return s.msgID
}

// severityFor picks the syslog severity for a record.
func severityFor(r stream.Record) int {
	switch r.Kind {
	case stream.KindTransaction:
		if r.Transaction != nil && !r.Transaction.Successful {
			return severityWarning
		}
	case stream.KindWASMDeployment:
		if r.WASMDeployment != nil && !r.WASMDeployment.Successful {
			return severityWarning
		}
	case stream.KindEvent:
		if r.Event != nil && r.Event.Type == "diagnostic" {
			return severityDebug
		}
	}
	return severityInfo
}

// writeJournaldField writes one native-protocol field. A value containing a
// newline cannot use NAME=value framing, so the protocol's length-prefixed
// form is used instead.
func writeJournaldField(b *strings.Builder, name, value string) {
	if strings.ContainsRune(value, '\n') {
		fmt.Fprintf(b, "%s\n%d\n%s\n", name, len(value), value)
		return
	}
	fmt.Fprintf(b, "%s=%s\n", name, value)
}

// syslogQuote escapes a structured-data parameter value as RFC 5424 requires:
// backslash, double quote and closing bracket.
func syslogQuote(value string) string {
	var b strings.Builder
	b.WriteByte('"')
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '\\', '"', ']':
			b.WriteByte('\\')
		}
		b.WriteByte(value[i])
	}
	b.WriteByte('"')
	return b.String()
}

// sanitizeSyslogValue replaces whitespace in a header field, which RFC 5424
// does not allow.
func sanitizeSyslogValue(value string) string {
	if value == "" {
		return "-"
	}
	var b strings.Builder
	for _, r := range value {
		switch r {
		case ' ', '\t', '\n', '\r':
			b.WriteByte('-')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// validSyslogToken reports whether value can appear as an RFC 5424
// APP-NAME/MSGID: printable US-ASCII, no spaces, at most 48 characters.
func validSyslogToken(value string) bool {
	if value == "" || len(value) > 48 {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < 33 || value[i] > 126 {
			return false
		}
	}
	return true
}

// DialSyslog connects to a syslog or journald socket.
//
// Accepted forms:
//
//	unixgram:///run/systemd/journal/socket   journald native or /dev/log
//	unix:///dev/log                          SOCK_STREAM syslog
//	udp://host:514                           classic UDP syslog
//	tcp://host:601                           framed TCP syslog
//	/path/to/socket                          shorthand for unixgram
//
// The returned connection can be passed to SyslogOptions.Writer and closed by
// the sink's Close.
func DialSyslog(address string) (net.Conn, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return nil, fmt.Errorf("syslog: address is required")
	}

	network := "unixgram"
	target := address

	if before, after, ok := strings.Cut(address, "://"); ok {
		switch strings.ToLower(before) {
		case "unixgram", "unix":
			network = strings.ToLower(before)
		case "udp", "tcp":
			network = strings.ToLower(before)
		default:
			return nil, fmt.Errorf("syslog: unsupported scheme %q", before)
		}
		target = after
	} else if !strings.HasPrefix(address, "/") {
		return nil, fmt.Errorf("syslog: %q needs a scheme such as udp:// or unixgram://", address)
	}

	if target == "" {
		return nil, fmt.Errorf("syslog: address %q has no path or host", address)
	}

	conn, err := net.Dial(network, target)
	if err != nil {
		return nil, fmt.Errorf("syslog: connecting to %s: %w", address, err)
	}
	return conn, nil
}
