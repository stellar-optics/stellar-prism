package prismpb

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The public issue API names this service EventStreamService and its payloads
// FilterRequest/EventRecord. The generated PrismStream service is equivalent;
// these aliases let both spellings share one implementation.
type FilterRequest = EventRequest
type EventRecord = EventResponse

const EventStreamService_Subscribe_FullMethodName = "/prism.v1.EventStreamService/Subscribe"

type EventStreamServiceClient interface {
	Subscribe(ctx context.Context, in *FilterRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[EventRecord], error)
}

type eventStreamServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewEventStreamServiceClient(cc grpc.ClientConnInterface) EventStreamServiceClient {
	return &eventStreamServiceClient{cc}
}

func (c *eventStreamServiceClient) Subscribe(ctx context.Context, in *FilterRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[EventRecord], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &EventStreamService_ServiceDesc.Streams[0], EventStreamService_Subscribe_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[FilterRequest, EventRecord]{ClientStream: stream}
	if err := x.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type EventStreamService_SubscribeClient = grpc.ServerStreamingClient[EventRecord]

type EventStreamService_SubscribeServer = grpc.ServerStreamingServer[EventRecord]

type EventStreamServiceServer interface {
	Subscribe(*FilterRequest, EventStreamService_SubscribeServer) error
	mustEmbedUnimplementedEventStreamServiceServer()
}

type UnimplementedEventStreamServiceServer struct{}

func (UnimplementedEventStreamServiceServer) Subscribe(*FilterRequest, grpc.ServerStreamingServer[EventRecord]) error {
	return status.Errorf(codes.Unimplemented, "method Subscribe not implemented")
}
func (UnimplementedEventStreamServiceServer) mustEmbedUnimplementedEventStreamServiceServer() {}
func (UnimplementedEventStreamServiceServer) testEmbeddedByValue()                            {}

type UnsafeEventStreamServiceServer interface {
	mustEmbedUnimplementedEventStreamServiceServer()
}

func RegisterEventStreamServiceServer(s grpc.ServiceRegistrar, srv EventStreamServiceServer) {
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&EventStreamService_ServiceDesc, srv)
}

func _EventStreamService_Subscribe_Handler(srv any, stream grpc.ServerStream) error {
	in := new(FilterRequest)
	if err := stream.RecvMsg(in); err != nil {
		return err
	}
	impl, ok := srv.(EventStreamServiceServer)
	if !ok {
		return status.Errorf(codes.Internal, "prism.v1.EventStreamService: unexpected server type %T", srv)
	}
	if err := impl.Subscribe(in, &eventStreamServiceSubscribeServer{stream}); err != nil {
		return err
	}
	return nil
}

type eventStreamServiceSubscribeServer struct {
	grpc.ServerStream
}

func (s *eventStreamServiceSubscribeServer) Send(m *EventRecord) error {
	return s.SendMsg(m)
}

var EventStreamService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "prism.v1.EventStreamService",
	HandlerType: (*EventStreamServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Subscribe",
			Handler:       _EventStreamService_Subscribe_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "proto/prism.proto",
}
