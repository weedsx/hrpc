package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"hrpc/codec"
	"io"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"
)

type Option struct {
	MagicNumber    int           // MagicNumber 标记为 rpc 请求
	CodecType      codec.Type    // 客户端可以选择不同的编解码器来编码正文
	ConnectTimeout time.Duration // 连接的超时时间，0 表示无限制
	HandleTimeout  time.Duration // 处理请求的超时时间
}

const MagicNumber = 0x3bef5c

var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      codec.GobType,
	ConnectTimeout: 10 * time.Second,
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
	server.serveCodec(codecFunc(conn), &opt)
}

// invalidRequest 是有错误发生时用于占位的空结构体
var invalidRequest = struct{}{}

// serveCodec 轮询读取请求，同步处理多个请求
func (server *Server) serveCodec(cc codec.Codec, opt *Option) {
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
		go server.handleRequest(cc, req, sending, wg, opt.HandleTimeout)
	}
	wg.Wait()
	_ = cc.Close()
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

// handleRequest 处理请求，返回响应
func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()

	sent := make(chan struct{})
	var once sync.Once // 确保 sendResponse 仅调用一次

	go func() {
		err := req.svc.call(req.mtype, req.argv, req.replyv)
		if err != nil {
			req.h.Err = err.Error()
			once.Do(func() {
				server.sendResponse(cc, req.h, invalidRequest, sending)
				sent <- struct{}{}
			})
			return
		}
		once.Do(func() {
			server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
			sent <- struct{}{}
		})
	}()

	if timeout == 0 {
		<-sent
		return
	}

	select {
	case <-time.After(timeout):
		once.Do(func() {
			req.h.Err = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
			server.sendResponse(cc, req.h, invalidRequest, sending)
		})
	case <-sent:
	}
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
