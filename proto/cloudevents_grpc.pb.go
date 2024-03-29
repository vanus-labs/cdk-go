// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.21.12
// source: cloudevents.proto

package cloudevents

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// CloudEventsClient is the client API for CloudEvents service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type CloudEventsClient interface {
	Send(ctx context.Context, in *BatchEvent, opts ...grpc.CallOption) (*emptypb.Empty, error)
}

type cloudEventsClient struct {
	cc grpc.ClientConnInterface
}

func NewCloudEventsClient(cc grpc.ClientConnInterface) CloudEventsClient {
	return &cloudEventsClient{cc}
}

func (c *cloudEventsClient) Send(ctx context.Context, in *BatchEvent, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, "/vanus.core.cloudevents.CloudEvents/Send", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CloudEventsServer is the server API for CloudEvents service.
// All implementations should embed UnimplementedCloudEventsServer
// for forward compatibility
type CloudEventsServer interface {
	Send(context.Context, *BatchEvent) (*emptypb.Empty, error)
}

// UnimplementedCloudEventsServer should be embedded to have forward compatible implementations.
type UnimplementedCloudEventsServer struct {
}

func (UnimplementedCloudEventsServer) Send(context.Context, *BatchEvent) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Send not implemented")
}

// UnsafeCloudEventsServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to CloudEventsServer will
// result in compilation errors.
type UnsafeCloudEventsServer interface {
	mustEmbedUnimplementedCloudEventsServer()
}

func RegisterCloudEventsServer(s grpc.ServiceRegistrar, srv CloudEventsServer) {
	s.RegisterService(&CloudEvents_ServiceDesc, srv)
}

func _CloudEvents_Send_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BatchEvent)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CloudEventsServer).Send(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/vanus.core.cloudevents.CloudEvents/Send",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CloudEventsServer).Send(ctx, req.(*BatchEvent))
	}
	return interceptor(ctx, in, info, handler)
}

// CloudEvents_ServiceDesc is the grpc.ServiceDesc for CloudEvents service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var CloudEvents_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "vanus.core.cloudevents.CloudEvents",
	HandlerType: (*CloudEventsServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Send",
			Handler:    _CloudEvents_Send_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "cloudevents.proto",
}
