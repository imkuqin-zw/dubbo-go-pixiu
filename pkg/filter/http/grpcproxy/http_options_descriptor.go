package grpcproxy

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	stdHttp "net/http"
	"reflect"
	"regexp"
	"strings"
	"sync"

	"github.com/apache/dubbo-go-pixiu/pkg/context/http"
	"github.com/apache/dubbo-go-pixiu/pkg/logger"
	"github.com/apache/dubbo-go-pixiu/pkg/server"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"github.com/julienschmidt/httprouter"
	perrors "github.com/pkg/errors"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

type httpContextKey struct{}

type Api struct {
	ServiceName  string
	MethodName   *desc.MethodDescriptor
	RequestProto *desc.MessageDescriptor
	// HTTPMethod is the HTTP method which this method is mapped to.
	HTTPMethod string
	// Path is the  HTTP request path.
	Path string
}

func extractAPIOptions(meth *descriptorpb.MethodDescriptorProto) (*annotations.HttpRule, error) {
	if meth.Options == nil {
		return nil, nil
	}
	if !proto.HasExtension(meth.Options, annotations.E_Http) {
		return nil, nil
	}
	ext := proto.GetExtension(meth.Options, annotations.E_Http)
	opts, ok := ext.(*annotations.HttpRule)
	if !ok {
		return nil, fmt.Errorf("extension is %T; want an HttpRule", ext)
	}
	return opts, nil
}

func newApi(opts *annotations.HttpRule, meth *desc.MethodDescriptor) (*Api, error) {
	api := &Api{
		ServiceName:  meth.GetService().GetName(),
		MethodName:   meth,
		RequestProto: meth.GetInputType(),
	}
	switch {
	case opts.GetGet() != "":
		api.HTTPMethod = "GET"
		api.Path = opts.GetGet()
		if opts.Body != "" {
			return nil, fmt.Errorf("must not set request body when http method is GET: %s", meth.GetName())
		}
	case opts.GetPut() != "":
		api.HTTPMethod = "PUT"
		api.Path = opts.GetPut()

	case opts.GetPost() != "":
		api.HTTPMethod = "POST"
		api.Path = opts.GetPost()

	case opts.GetDelete() != "":
		api.HTTPMethod = "DELETE"
		api.Path = opts.GetDelete()
	case opts.GetPatch() != "":
		api.HTTPMethod = "PATCH"
		api.Path = opts.GetPatch()

	default:
		return nil, errors.New(fmt.Sprintf("No pattern specified in google.api.HttpRule: %s"))
	}
	reg, _ := regexp.Compile(`{([a-z]|[A-Z])([1-9]|[a-z]|[A-Z]|_|\.)*}`)
	api.Path = reg.ReplaceAllStringFunc(api.Path, func(s string) string {
		return ":" + strings.TrimRight(strings.TrimLeft(s, "{"), "}")
	})
	return api, nil
}

func extractApi(opts *annotations.HttpRule, meth *desc.MethodDescriptor) ([]*Api, error) {
	list := make([]*Api, 0, 1)
	api, err := newApi(opts, meth)
	if err != nil {
		return nil, err
	}
	list = append(list, api)
	for _, additional := range opts.GetAdditionalBindings() {
		if len(additional.AdditionalBindings) > 0 {
			return nil, fmt.Errorf("additional_binding in additional_binding not allowed: %s.%s", meth.GetService().GetName(), meth.GetName())
		}
		api, err := newApi(additional, meth)
		if err != nil {
			return nil, err
		}
		list = append(list, api)
	}

	return list, nil
}

func handleMaker(api *Api, f *Filter) httprouter.Handle {
	return func(writer stdHttp.ResponseWriter, request *stdHttp.Request, params httprouter.Params) {
		c := request.Context().Value(httpContextKey{}).(*http.HttpContext)
		msgFac := dynamic.NewMessageFactoryWithExtensionRegistry(&f.extReg)
		grpcReq := msgFac.NewDynamicMessage(api.RequestProto) //msgFac.NewMessage(api.RequestProto)
		err := jsonToProtoMsg(c.Request.Body, grpcReq)
		if err != nil && !errors.Is(err, io.EOF) {
			logger.Errorf("%s err {failed to convert json to proto msg, %s}", loggerHeader, err.Error())
			c.Err = err
			c.Next()
			return
		}
		if len(params) > 0 {
			for _, item := range params {
				if err := setPathParam(msgFac, grpcReq, item.Key, item.Value); err != nil {
					logger.Errorf("%s err {failed to convert path param to proto msg, %s}", loggerHeader, err.Error())
					c.Err = err
					c.Next()
					return
				}
			}
		}
		var clientConn *grpc.ClientConn
		re := c.GetRouteEntry()
		logger.Debugf("%s client choose endpoint from cluster :%v", loggerHeader, re.Cluster)

		e := server.GetClusterManager().PickEndpoint(re.Cluster)
		if e == nil {
			logger.Errorf("%s err {cluster not exists}", loggerHeader)
			c.Err = perrors.New("cluster not exists")
			c.Next()
			return
		}

		ep := e.Address.GetAddress()

		p, ok := f.pools[strings.Join([]string{re.Cluster, ep}, ".")]
		if !ok {
			p = &sync.Pool{}
		}

		clientConn, ok = p.Get().(*grpc.ClientConn)
		if !ok || clientConn == nil {
			// TODO(Kenway): Support Credential and TLS
			clientConn, err = grpc.DialContext(c.Ctx, ep, grpc.WithInsecure())
			if err != nil || clientConn == nil {
				logger.Errorf("%s err {failed to connect to grpc service provider}", loggerHeader)
				c.Err = err
				c.Next()
				return
			}
		}

		stub := grpcdynamic.NewStubWithMessageFactory(clientConn, msgFac)

		// metadata in grpc has the same feature in http
		md := mapHeaderToMetadata(c.AllHeaders())
		ctx := metadata.NewOutgoingContext(c.Ctx, md)

		md = metadata.MD{}
		t := metadata.MD{}

		resp, err := Invoke(ctx, stub, api.MethodName, grpcReq, grpc.Header(&md), grpc.Trailer(&t))
		// judge err is server side error or not
		if st, ok := status.FromError(err); !ok || isServerError(st) {
			logger.Error("%s err {failed to invoke grpc service provider, %s}", loggerHeader, err.Error())
			c.Err = err
			c.Next()
			return
		}

		res, err := protoMsgToJson(resp)
		if err != nil {
			logger.Error("%s err {failed to convert proto msg to json, %s}", loggerHeader, err.Error())
			c.Err = err
			c.Next()
			return
		}

		h := mapMetadataToHeader(md)
		th := mapMetadataToHeader(t)
		h.Set("content-type", "application/json")
		// let response filter handle resp
		c.SourceResp = &stdHttp.Response{
			StatusCode: stdHttp.StatusOK,
			Header:     h,
			Body:       ioutil.NopCloser(strings.NewReader(res)),
			Trailer:    th,
			Request:    c.Request,
		}
		p.Put(clientConn)
		c.Next()
	}
}

func initServiceRouter(f *Filter, meth *desc.MethodDescriptor) error {
	httpRule, err := extractAPIOptions(meth.AsMethodDescriptorProto())
	if err != nil {
		return fmt.Errorf("fault to extract http rule: %v", err)
	}
	if httpRule == nil {
		return nil
	}
	apis, err := extractApi(httpRule, meth)
	if err != nil {
		return fmt.Errorf("fault to extract api: %v", err)
	}
	for _, item := range apis {
		f.router.Handle(item.HTTPMethod, item.Path, handleMaker(item, f))
	}
	return nil
}

func isNil(i interface{}) bool {
	vi := reflect.ValueOf(i)
	if vi.Kind() == reflect.Ptr {
		return vi.IsNil()
	}
	return false
}

func setPathParam(msgFac *dynamic.MessageFactory, req *dynamic.Message, key string, val string) error {
	var msg = req
	paths := strings.Split(key, ".")
	if len(paths) > 0 {
		for i := 0; i < len(paths)-1; i++ {
			tmp := msg.GetFieldByName(paths[i])
			if isNil(tmp) {
				desc := msg.FindFieldDescriptorByName(paths[i])
				if desc == nil {
					return errors.New(fmt.Sprintf("not found field, %s", key))
				}
				subMsg := msgFac.NewDynamicMessage(msg.FindFieldDescriptorByName(paths[i]).GetMessageType())
				msg.SetFieldByName(paths[i], subMsg)
				msg = subMsg
			} else {
				msg = tmp.(*dynamic.Message)
			}
		}
	}
	fd := msg.FindFieldDescriptorByName(paths[len(paths)-1])
	if fd == nil {
		return errors.New(fmt.Sprintf("not found field, %s", key))
	}
	v, err := convertVal(fd, val)
	if err != nil {
		return err
	}
	msg.SetField(fd, v)
	return nil
}

func convertVal(fd *desc.FieldDescriptor, val string) (interface{}, error) {
	t := fd.GetType()
	switch t {
	case descriptor.FieldDescriptorProto_TYPE_SFIXED32,
		descriptor.FieldDescriptorProto_TYPE_INT32,
		descriptor.FieldDescriptorProto_TYPE_SINT32,
		descriptor.FieldDescriptorProto_TYPE_ENUM:
		return Int32(val)
	case descriptor.FieldDescriptorProto_TYPE_SFIXED64,
		descriptor.FieldDescriptorProto_TYPE_INT64,
		descriptor.FieldDescriptorProto_TYPE_SINT64:
		return Int64(val)
	case descriptor.FieldDescriptorProto_TYPE_FIXED32,
		descriptor.FieldDescriptorProto_TYPE_UINT32:
		return Uint32(val)
	case descriptor.FieldDescriptorProto_TYPE_FIXED64,
		descriptor.FieldDescriptorProto_TYPE_UINT64:
		return Uint64(val)
	case descriptor.FieldDescriptorProto_TYPE_FLOAT:
		return Float32(val)
	case descriptor.FieldDescriptorProto_TYPE_DOUBLE:
		return Float64(val)
	case descriptor.FieldDescriptorProto_TYPE_STRING:
		return val, nil
	default:
		return nil, fmt.Errorf("unable to set from path to field type: %v", fd.GetType())
	}
}
