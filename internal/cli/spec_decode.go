package cli

import (
	"context"
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

type specDecodeSource struct {
	next       stream.Source
	spec       *contractSpec
	contractID string
}

func newSpecDecodeSource(next stream.Source, spec *contractSpec, contractID string) stream.Source {
	return &specDecodeSource{next: next, spec: spec, contractID: contractID}
}

func (s *specDecodeSource) Name() string { return s.next.Name() }

func (s *specDecodeSource) CursorLedger(cursor string) (uint32, bool) {
	return s.next.CursorLedger(cursor)
}

func (s *specDecodeSource) Fetch(ctx context.Context, cursor string) (stream.Batch, error) {
	batch, err := s.next.Fetch(ctx, cursor)
	if err != nil {
		return batch, err
	}
	for i := range batch.Records {
		s.decode(&batch.Records[i])
	}
	return batch, nil
}

func (s *specDecodeSource) decode(record *stream.Record) {
	if record.Kind != stream.KindEvent || record.Event == nil {
		return
	}
	if s.contractID != "" && record.Event.ContractID != s.contractID {
		return
	}
	for i := range s.spec.Events {
		params, ok := matchSpecEvent(record.Event, s.spec.Events[i])
		if !ok {
			continue
		}
		record.Event.SpecName = string(s.spec.Events[i].Name)
		record.Event.SpecParams = params
		return
	}
}

func matchSpecEvent(event *stream.Event, spec xdr.ScSpecEventV0) ([]stream.EventSpecParam, bool) {
	topics := make([]xdr.ScVal, 0, len(event.TopicXDR))
	for _, raw := range event.TopicXDR {
		var value xdr.ScVal
		if err := xdr.SafeUnmarshalBase64(raw, &value); err != nil {
			return nil, false
		}
		topics = append(topics, value)
	}

	prefix := make([]string, 0, len(spec.PrefixTopics))
	for _, symbol := range spec.PrefixTopics {
		prefix = append(prefix, string(symbol))
	}
	skip := len(prefix)
	if skip == 0 {
		if len(topics) == 0 || (spec.Name != "" && scValName(topics[0]) != string(spec.Name)) {
			return nil, false
		}
		skip = 1
	}
	if len(topics) < skip {
		return nil, false
	}
	for i, want := range prefix {
		if scValName(topics[i]) != want {
			return nil, false
		}
	}

	var data []xdr.ScVal
	if event.ValueXDR != "" {
		var value xdr.ScVal
		if err := xdr.SafeUnmarshalBase64(event.ValueXDR, &value); err == nil {
			data = []xdr.ScVal{value}
		}
	}

	params := make([]stream.EventSpecParam, 0, len(spec.Params))
	topicIndex := 0
	dataIndex := 0
	for _, param := range spec.Params {
		entry := stream.EventSpecParam{
			Name:     param.Name,
			Type:     formatSpecType(param.Type),
			Location: "data",
		}
		if param.Location == xdr.ScSpecEventParamLocationV0ScSpecEventParamLocationTopicList {
			entry.Location = "topic"
			if skip+topicIndex >= len(topics) {
				return nil, false
			}
			entry.XDR = marshalScVal(topics[skip+topicIndex])
			topicIndex++
		} else {
			value, ok := specDataValue(data, spec.DataFormat, dataIndex, entry.Name)
			if !ok {
				return nil, false
			}
			entry.XDR = marshalScVal(value)
			dataIndex++
		}
		params = append(params, entry)
	}
	return params, len(params) > 0
}

func specDataValue(data []xdr.ScVal, format xdr.ScSpecEventDataFormat, index int, name string) (xdr.ScVal, bool) {
	if len(data) == 0 || data[0].Type == xdr.ScValTypeScvVoid {
		return xdr.ScVal{}, false
	}
	switch format {
	case xdr.ScSpecEventDataFormatScSpecEventDataFormatSingleValue:
		if index >= len(data) || index > 0 {
			return xdr.ScVal{}, false
		}
		return data[0], true
	case xdr.ScSpecEventDataFormatScSpecEventDataFormatVec:
		value := data[0]
		if value.Type != xdr.ScValTypeScvVec || value.Vec == nil || *value.Vec == nil || index >= len(**value.Vec) {
			return xdr.ScVal{}, false
		}
		return (**value.Vec)[index], true
	case xdr.ScSpecEventDataFormatScSpecEventDataFormatMap:
		value := data[0]
		if value.Type != xdr.ScValTypeScvMap || value.Map == nil || *value.Map == nil {
			return xdr.ScVal{}, false
		}
		for _, entry := range **value.Map {
			if scValName(entry.Key) == name {
				return entry.Val, true
			}
		}
		return xdr.ScVal{}, false
	default:
		return xdr.ScVal{}, false
	}
}

func scValName(value xdr.ScVal) string {
	switch value.Type {
	case xdr.ScValTypeScvSymbol:
		if value.Sym != nil {
			return string(*value.Sym)
		}
	case xdr.ScValTypeScvString:
		if value.Str != nil {
			return string(*value.Str)
		}
	}
	return ""
}

func marshalScVal(value xdr.ScVal) string {
	encoded, err := xdr.MarshalBase64(value)
	if err != nil {
		return ""
	}
	return encoded
}

func localSpecDecoder(next stream.Source, spec *contractSpec, contractID string) (stream.Source, error) {
	if spec == nil {
		return nil, fmt.Errorf("local contract spec is not loaded")
	}
	return newSpecDecodeSource(next, spec, contractID), nil
}
