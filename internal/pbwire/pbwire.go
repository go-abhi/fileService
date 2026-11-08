// Package pbwire implements the Protocol Buffers binary wire format for the
// service's hand-written message types. It follows proto3 semantics:
// scalar fields with zero values are omitted, repeated scalars are packed,
// unknown fields and fields with an unexpected wire type are skipped, and
// strings must be valid UTF-8.
package pbwire

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
	"unicode/utf8"
)

// WireType is a protobuf wire type.
type WireType uint8

// Wire types.
const (
	Varint     WireType = 0
	Fixed64    WireType = 1
	Bytes      WireType = 2
	StartGroup WireType = 3
	EndGroup   WireType = 4
	Fixed32    WireType = 5
)

// Message is implemented by every message type.
type Message interface {
	// AppendProto appends the encoded message to b.
	AppendProto(b []byte) []byte
	// MergeProto decodes b and merges it into the message. The message may
	// keep references to b (bytes fields are not copied).
	MergeProto(b []byte) error
	// Reset clears the message.
	Reset()
}

// Marshal encodes m.
func Marshal(m Message) []byte { return m.AppendProto(nil) }

// Unmarshal resets m and decodes b into it.
func Unmarshal(b []byte, m Message) error {
	m.Reset()
	return m.MergeProto(b)
}

// ErrMalformed reports invalid wire data.
var ErrMalformed = errors.New("pbwire: malformed message")

// --- encoding ---------------------------------------------------------------------

// AppendVarint appends v as a base-128 varint.
func AppendVarint(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

// AppendTag appends a field tag.
func AppendTag(b []byte, num int, t WireType) []byte {
	return AppendVarint(b, uint64(num)<<3|uint64(t))
}

// String appends a string field, omitted when empty.
func String(b []byte, num int, s string) []byte {
	if s == "" {
		return b
	}
	b = AppendTag(b, num, Bytes)
	b = AppendVarint(b, uint64(len(s)))
	return append(b, s...)
}

// BytesField appends a bytes field, omitted when empty.
func BytesField(b []byte, num int, v []byte) []byte {
	if len(v) == 0 {
		return b
	}
	return BytesAlways(b, num, v)
}

// BytesAlways appends a bytes field even when empty (oneof members).
func BytesAlways(b []byte, num int, v []byte) []byte {
	b = AppendTag(b, num, Bytes)
	b = AppendVarint(b, uint64(len(v)))
	return append(b, v...)
}

// Int64 appends an int64 field, omitted when zero.
func Int64(b []byte, num int, v int64) []byte {
	if v == 0 {
		return b
	}
	return Int64Always(b, num, v)
}

// Int64Always appends an int64 field even when zero (optional fields).
func Int64Always(b []byte, num int, v int64) []byte {
	b = AppendTag(b, num, Varint)
	return AppendVarint(b, uint64(v))
}

// Int32 appends an int32 (or enum) field, omitted when zero. Negative
// values are sign-extended to 64 bits as the spec requires.
func Int32(b []byte, num int, v int32) []byte {
	if v == 0 {
		return b
	}
	b = AppendTag(b, num, Varint)
	return AppendVarint(b, uint64(int64(v)))
}

// Bool appends a bool field, omitted when false.
func Bool(b []byte, num int, v bool) []byte {
	if !v {
		return b
	}
	b = AppendTag(b, num, Varint)
	return append(b, 1)
}

// Embedded appends a length-delimited field holding body.
func Embedded(b []byte, num int, body []byte) []byte {
	b = AppendTag(b, num, Bytes)
	b = AppendVarint(b, uint64(len(body)))
	return append(b, body...)
}

// MessageField appends a message field. Nil messages are omitted.
func MessageField[T any, P interface {
	*T
	Message
}](b []byte, num int, m P) []byte {
	if m == nil {
		return b
	}
	return Embedded(b, num, m.AppendProto(nil))
}

// RepeatedMessage appends one field per element.
func RepeatedMessage[T any, P interface {
	*T
	Message
}](b []byte, num int, ms []P) []byte {
	for _, m := range ms {
		if m == nil {
			m = new(T)
		}
		b = Embedded(b, num, m.AppendProto(nil))
	}
	return b
}

// PackedInt32 appends a packed repeated int32 field.
func PackedInt32(b []byte, num int, vs []int32) []byte {
	if len(vs) == 0 {
		return b
	}
	var body []byte
	for _, v := range vs {
		body = AppendVarint(body, uint64(int64(v)))
	}
	return Embedded(b, num, body)
}

// StringMap appends a map<string,string> field with keys in sorted order.
func StringMap(b []byte, num int, m map[string]string) []byte {
	if len(m) == 0 {
		return b
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		var entry []byte
		entry = AppendTag(entry, 1, Bytes)
		entry = AppendVarint(entry, uint64(len(k)))
		entry = append(entry, k...)
		v := m[k]
		entry = AppendTag(entry, 2, Bytes)
		entry = AppendVarint(entry, uint64(len(v)))
		entry = append(entry, v...)
		b = Embedded(b, num, entry)
	}
	return b
}

// Timestamp appends a google.protobuf.Timestamp field; zero times are omitted.
func Timestamp(b []byte, num int, t time.Time) []byte {
	if t.IsZero() {
		return b
	}
	var body []byte
	body = Int64(body, 1, t.Unix())
	body = Int32(body, 2, int32(t.Nanosecond()))
	return Embedded(b, num, body)
}

// Duration appends a google.protobuf.Duration field; zero is omitted.
func Duration(b []byte, num int, d time.Duration) []byte {
	if d == 0 {
		return b
	}
	var body []byte
	body = Int64(body, 1, int64(d/time.Second))
	body = Int32(body, 2, int32(d%time.Second))
	return Embedded(b, num, body)
}

// --- decoding ---------------------------------------------------------------------

// Field is one decoded field.
type Field struct {
	Num  int
	Type WireType
	// Value holds varint and fixed values.
	Value uint64
	// Data holds the payload of length-delimited fields.
	Data []byte
}

// ConsumeVarint decodes a varint from b, returning it and its length.
func ConsumeVarint(b []byte) (uint64, int) {
	var v uint64
	for i := 0; i < len(b) && i < 10; i++ {
		c := b[i]
		if i == 9 && c > 1 {
			return 0, -1
		}
		v |= uint64(c&0x7f) << (7 * i)
		if c < 0x80 {
			return v, i + 1
		}
	}
	return 0, -1
}

// Parse calls fn for every field in b.
func Parse(b []byte, fn func(Field) error) error {
	for len(b) > 0 {
		tag, n := ConsumeVarint(b)
		if n < 0 {
			return ErrMalformed
		}
		b = b[n:]
		num := tag >> 3
		if num == 0 || num > math.MaxInt32 {
			return fmt.Errorf("%w: invalid field number %d", ErrMalformed, num)
		}
		f := Field{Num: int(num), Type: WireType(tag & 7)}
		switch f.Type {
		case Varint:
			v, n := ConsumeVarint(b)
			if n < 0 {
				return ErrMalformed
			}
			f.Value, b = v, b[n:]
		case Fixed64:
			if len(b) < 8 {
				return ErrMalformed
			}
			f.Value = uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
				uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
			b = b[8:]
		case Fixed32:
			if len(b) < 4 {
				return ErrMalformed
			}
			f.Value = uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24
			b = b[4:]
		case Bytes:
			l, n := ConsumeVarint(b)
			if n < 0 || l > uint64(len(b)-n) {
				return ErrMalformed
			}
			f.Data, b = b[n:n+int(l)], b[n+int(l):]
		case StartGroup:
			rest, err := skipGroup(b, f.Num)
			if err != nil {
				return err
			}
			b = rest
			continue // groups are never part of our schema
		default:
			return fmt.Errorf("%w: invalid wire type %d", ErrMalformed, f.Type)
		}
		if err := fn(f); err != nil {
			return err
		}
	}
	return nil
}

func skipGroup(b []byte, num int) ([]byte, error) {
	for {
		tag, n := ConsumeVarint(b)
		if n < 0 {
			return nil, ErrMalformed
		}
		b = b[n:]
		t := WireType(tag & 7)
		switch t {
		case EndGroup:
			if int(tag>>3) != num {
				return nil, ErrMalformed
			}
			return b, nil
		case Varint:
			if _, n = ConsumeVarint(b); n < 0 {
				return nil, ErrMalformed
			}
			b = b[n:]
		case Fixed64:
			if len(b) < 8 {
				return nil, ErrMalformed
			}
			b = b[8:]
		case Fixed32:
			if len(b) < 4 {
				return nil, ErrMalformed
			}
			b = b[4:]
		case Bytes:
			l, n := ConsumeVarint(b)
			if n < 0 || l > uint64(len(b)-n) {
				return nil, ErrMalformed
			}
			b = b[n+int(l):]
		case StartGroup:
			var err error
			if b, err = skipGroup(b, int(tag>>3)); err != nil {
				return nil, err
			}
		default:
			return nil, ErrMalformed
		}
	}
}

// StringTo stores a string field into dst. Fields with another wire type
// are ignored (treated as unknown).
func (f Field) StringTo(dst *string) error {
	if f.Type != Bytes {
		return nil
	}
	if !utf8.Valid(f.Data) {
		return fmt.Errorf("%w: field %d: invalid UTF-8", ErrMalformed, f.Num)
	}
	*dst = string(f.Data)
	return nil
}

// BytesTo stores a bytes field into dst (no copy). A present but empty
// field yields a non-nil empty slice.
func (f Field) BytesTo(dst *[]byte) error {
	if f.Type != Bytes {
		return nil
	}
	if f.Data == nil {
		*dst = []byte{}
	} else {
		*dst = f.Data
	}
	return nil
}

// Int64To stores an int64 field into dst.
func (f Field) Int64To(dst *int64) error {
	if f.Type == Varint {
		*dst = int64(f.Value)
	}
	return nil
}

// Int32To stores an int32 field into dst.
func (f Field) Int32To(dst *int32) error {
	if f.Type == Varint {
		*dst = int32(f.Value)
	}
	return nil
}

// BoolTo stores a bool field into dst.
func (f Field) BoolTo(dst *bool) error {
	if f.Type == Varint {
		*dst = f.Value != 0
	}
	return nil
}

// EnumTo stores an enum field into dst.
func EnumTo[E ~int32](f Field, dst *E) error {
	if f.Type == Varint {
		*dst = E(int32(f.Value))
	}
	return nil
}

// OptionalInt64To stores an optional int64 field into dst.
func (f Field) OptionalInt64To(dst **int64) error {
	if f.Type == Varint {
		v := int64(f.Value)
		*dst = &v
	}
	return nil
}

// MessageTo merges a message field into *dst, allocating it if needed.
func MessageTo[T any, P interface {
	*T
	Message
}](f Field, dst *P) error {
	if f.Type != Bytes {
		return nil
	}
	if *dst == nil {
		*dst = new(T)
	}
	return (*dst).MergeProto(f.Data)
}

// AppendMessageTo decodes a repeated message element and appends it.
func AppendMessageTo[T any, P interface {
	*T
	Message
}](f Field, dst *[]P) error {
	if f.Type != Bytes {
		return nil
	}
	m := P(new(T))
	if err := m.MergeProto(f.Data); err != nil {
		return err
	}
	*dst = append(*dst, m)
	return nil
}

// Int32sTo appends a repeated int32 field, accepting packed and unpacked forms.
func (f Field) Int32sTo(dst *[]int32) error {
	switch f.Type {
	case Varint:
		*dst = append(*dst, int32(f.Value))
	case Bytes:
		b := f.Data
		for len(b) > 0 {
			v, n := ConsumeVarint(b)
			if n < 0 {
				return ErrMalformed
			}
			*dst = append(*dst, int32(v))
			b = b[n:]
		}
	}
	return nil
}

// MapEntryTo decodes a map<string,string> entry into *dst.
func (f Field) MapEntryTo(dst *map[string]string) error {
	if f.Type != Bytes {
		return nil
	}
	var k, v string
	err := Parse(f.Data, func(e Field) error {
		switch e.Num {
		case 1:
			return e.StringTo(&k)
		case 2:
			return e.StringTo(&v)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if *dst == nil {
		*dst = map[string]string{}
	}
	(*dst)[k] = v
	return nil
}

// TimeTo decodes a google.protobuf.Timestamp field.
func (f Field) TimeTo(dst *time.Time) error {
	if f.Type != Bytes {
		return nil
	}
	var sec int64
	var nsec int32
	err := Parse(f.Data, func(e Field) error {
		switch e.Num {
		case 1:
			return e.Int64To(&sec)
		case 2:
			return e.Int32To(&nsec)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if nsec < 0 || nsec > 999_999_999 {
		return fmt.Errorf("%w: timestamp nanos out of range", ErrMalformed)
	}
	*dst = time.Unix(sec, int64(nsec)).UTC()
	return nil
}

// DurationTo decodes a google.protobuf.Duration field.
func (f Field) DurationTo(dst *time.Duration) error {
	if f.Type != Bytes {
		return nil
	}
	var sec int64
	var nsec int32
	err := Parse(f.Data, func(e Field) error {
		switch e.Num {
		case 1:
			return e.Int64To(&sec)
		case 2:
			return e.Int32To(&nsec)
		}
		return nil
	})
	if err != nil {
		return err
	}
	const maxSec = int64(math.MaxInt64 / int64(time.Second))
	if sec > maxSec || sec < -maxSec {
		return fmt.Errorf("%w: duration out of range", ErrMalformed)
	}
	*dst = time.Duration(sec)*time.Second + time.Duration(nsec)
	return nil
}
