package main

import (
	"encoding/json"
	"errors"
	"go/ast"
	"hrpc/codec"
	"io"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
)

type Option struct {
	MagicNumber int        // MagicNumber 标记为 rpc 请求
	CodecType   codec.Type // 客户端可以选择不同的编解码器来编码正文
}

const MagicNumber = 0x3bef5c

var DefaultOption = &Option{
	MagicNumber: MagicNumber,
	CodecType:   codec.GobType,
}

// Server 封装了服务端相关的方法
type Server struct {
	serviceMap sync.Map
}

// NewServer Server 构造器
func NewServer() *Server {
	return &Server{}
}

var DefaultServer = NewServer()

func StartListen(lis net.Listener) {
	DefaultServer.Accept(lis)
}

// Accept 轮询监听器获取连接，并创建一个 goroutine 为每一个连接进行服务
func (server *Server) Accept(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return
		}
		go server.ServeConn(conn)
	}
}

// Accept accepts connections on the listener and serves requests
// for each incoming connection.
func Accept(lis net.Listener) { DefaultServer.Accept(lis) }

// ServeConn 对连接进行解码
func (server *Server) ServeConn(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error: ", err)
		return
	}
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}
	// 返回用于创建编、解码器的函数
	codecFunc := codec.NewCodecFuncMap[opt.CodecType]
	if codecFunc == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}
	server.serveCodec(codecFunc(conn))
}

// invalidRequest 是有错误发生时用于占位的空结构体
var invalidRequest = struct{}{}

// serveCodec 轮询读取请求，同步处理多个请求
func (server *Server) serveCodec(cc codec.Codec) {
	sending := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	for {
		// 读取请求
		req, err := server.readRequest(cc)
		if err != nil {
			if req == nil {
				break
			}
			req.h.Err = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		// 处理请求
		go server.handleRequest(cc, req, sending, wg)
	}
	wg.Wait()
	_ = cc.Close()
}

func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	header, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{h: header}
	req.svc, req.mtype, err = server.findService(header.ServiceMethod)
	if err != nil {
		return nil, err
	}

	req.argv = req.mtype.newArgv()
	req.replyv = req.mtype.newReplyv()

	// 确保 argvi 是一个指针，ReadBody需要一个指针作为参数
	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}

	if err = cc.ReadBody(argvi); err != nil {
		log.Println("rpc server: read argv err:", err)
		return req, err
	}
	return req, nil
}

// handleRequest 处理请求，返回响应
func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()

	err := req.svc.call(req.mtype, req.argv, req.replyv)
	if err != nil {
		req.h.Err = err.Error()
		server.sendResponse(cc, req.h, invalidRequest, sending)
		return
	}

	server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
}

// sendResponse 同步返回响应
func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

// Register 服务端暴露服务
func (server *Server) Register(rcvr any) error {
	s := newService(rcvr)
	_, loaded := server.serviceMap.LoadOrStore(s.name, s)
	if loaded {
		return errors.New("rpc: service already defined: " + s.name)
	}
	return nil
}

func Register(rcvr interface{}) error { return DefaultServer.Register(rcvr) }

// findService 根据服务名和方法名查找对应的服务
func (server *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	dotIndex := strings.LastIndex(serviceMethod, ".")
	if dotIndex < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dotIndex], serviceMethod[dotIndex+1:]
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}
	svc = svci.(*service)
	mtype = svc.method[methodName]
	if mtype == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}

// request 存储一次 RPC 调用的请求信息
type request struct {
	h            *codec.Header
	argv, replyv reflect.Value
	mtype        *methodType
	svc          *service
}

// methodType 表示服务端暴露的一个方法的完整信息
type methodType struct {
	method             reflect.Method // 方法本身
	ArgType, ReplyType reflect.Type   // 第一个和第二个参数的类型
	numCalls           uint64         // 统计方法调用次数
}

func (methodType *methodType) NumCalls() uint64 {
	return atomic.LoadUint64(&methodType.numCalls)
}

func (methodType *methodType) newArgv() reflect.Value {
	var argv reflect.Value
	// 指针类型和值类型创建实例的方式有细微区别
	if methodType.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(methodType.ArgType.Elem())
	} else {
		argv = reflect.New(methodType.ArgType).Elem()
	}
	return argv
}

func (methodType *methodType) newReplyv() reflect.Value {
	// reply 必须是指针类型（指针类型才能通过反射修改值）
	replyv := reflect.New(methodType.ReplyType.Elem())

	switch methodType.ReplyType.Elem().Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(methodType.ReplyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(methodType.ReplyType.Elem(), 0, 0))
	default:
	}

	return replyv
}

// service 封装了服务端用于暴露的服务信息
//
// RPC 框架的一个基础能力是：像调用本地程序一样调用远程服务。
// 那如何将程序映射为服务呢？那么对 Go 来说，这个问题就变成了如何将结构体的方法映射为服务。
type service struct {
	name   string                 // 映射的结构体的名称
	typ    reflect.Type           // 映射的结构体类型
	rcvr   reflect.Value          // 映射的结构体实例
	method map[string]*methodType // 存储映射的结构体的所有符合条件的方法
}

// registerMethods 过滤出了符合条件的方法
func (service *service) registerMethods() {
	service.method = make(map[string]*methodType)
	for i := 0; i < service.typ.NumMethod(); i++ {
		method := service.typ.Method(i)
		mType := method.Type
		if mType.NumIn() != 3 || mType.NumOut() != 1 {
			continue
		}
		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		argType, replyType := mType.In(1), mType.In(2)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}

		service.method[method.Name] = &methodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
		log.Printf("rpc server: register %s.%s\n", service.name, method.Name)
	}
}

// call 能够通过反射值调用方法
func (service *service) call(m *methodType, argv, replyv reflect.Value) error {
	atomic.AddUint64(&m.numCalls, 1)
	f := m.method.Func
	retruns := f.Call([]reflect.Value{service.rcvr, argv, replyv})
	errInterface := retruns[0].Interface()
	if errInterface != nil {
		return errInterface.(error)
	}
	return nil
}

// newService 构建 service，rcvr 是任意需要映射为服务的结构体实例
func newService(rcvr interface{}) *service {
	s := new(service)

	s.rcvr = reflect.ValueOf(rcvr)
	s.name = reflect.Indirect(s.rcvr).Type().Name()
	s.typ = reflect.TypeOf(rcvr)

	if !ast.IsExported(s.name) {
		log.Fatalf("rpc server: %s is not a valid service name", s.name)
	}
	s.registerMethods()
	return s
}

func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}
