package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"hrpc/codec"
	"io"
	"log"
	"net"
	"sync"
)

// Call 代表一个 RPC 调用
type Call struct {
	Seq           uint64
	ServiceMethod string      // 格式 "<service>.<method>"
	Args          interface{} // 函数的参数
	Reply         interface{} // 函数的回复
	Error         error       // if error occurs, it will be set
	Done          chan *Call  // 当调用结束时，会调用 call.done() 通知调用方
}

func (c *Call) done() {
	c.Done <- c
}

type Client struct {
	cc       codec.Codec
	opt      *Option
	sending  sync.Mutex       // sending 是一个互斥锁，和服务端类似，为了保证请求的有序发送，即防止出现多个请求报文混淆
	header   codec.Header     // header 是每个请求的消息头，header 只有在请求发送时才需要，而请求发送是互斥的，因此每个客户端只需要一个，声明在 Client 结构体中可以复用
	mu       sync.Mutex       // protect following
	seq      uint64           // seq 用于给发送的请求进行编号，每个请求拥有唯一编号，默认进行加一
	pending  map[uint64]*Call // pending 存储未处理完的请求，键是编号，值是 Call 实例
	closing  bool             // 用户主动关闭，调用 Close
	shutdown bool             // 有错误发生
}

var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connection is shutdown")

// Close 关闭连接
func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	return client.cc.Close()
}

// IsAvailable 如果客户端正常工作，返回 true (closing 和 shutdown 任意一个值置为 true，则表示 Client 处于不可用的状态)
func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

// registerCall 将参数 call 添加到 client.pending 中，并更新 client.seq
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = client.seq
	client.pending[call.Seq] = call
	client.seq++
	return call.Seq, nil
}

// removeCall 根据 seq，从 client.pending 中移除对应的 call，并返回
func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// terminateCalls 服务端或客户端发生错误时调用，将 shutdown 设置为 true，且将错误信息通知给所有 pending 状态的 call
func (client *Client) terminateCalls(e error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.shutdown = true
	for _, call := range client.pending {
		call.Error = e
		call.done()
	}
}

// receive 客户端接收响应，接收到的响应有三种情况：
//
// call 不存在，可能是请求没有发送完整，或者因为其他原因被取消，但是服务端仍旧处理了。
// call 存在，但服务端处理出错，即 h.Error 不为空。
// call 存在，服务端处理正常，那么需要从 body 中读取 Reply 的值。
func (client *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err := client.cc.ReadHeader(&h); err != nil {
			break
		}
		call := client.removeCall(h.Seq)
		switch {
		case call == nil:
			// 通常意味着写入部分失败，调用已被删除
			err = client.cc.ReadBody(nil)
		case h.Err != "":
			call.Error = fmt.Errorf(h.Err)
			err = client.cc.ReadBody(nil)
			call.done()
		default:
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = fmt.Errorf("reading body %s", err.Error())
			}
			call.done()
		}
	}
	// 发生错误，调用 terminateCalls
	client.terminateCalls(err)
}

// send 发送请求
func (client *Client) send(call *Call) {
	// 确保客户端将发送完整的请求
	client.sending.Lock()
	defer client.sending.Unlock()

	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	// 准备请求的 header
	client.header = codec.Header{
		ServiceMethod: call.ServiceMethod,
		Seq:           seq,
		Err:           "",
	}

	// 编码发送请求
	if err := client.cc.Write(&client.header, call.Args); err != nil {
		removedCall := client.removeCall(seq)
		if removedCall != nil {
			call.Error = err
			call.done()
		}
	}
}

// Go 构造 Call 并发送请求， 返回表示调用的 Call 结构。
func (client *Client) Go(serviceMethod string, args, reply any, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}

	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}

	client.send(call)
	return call
}

// Call 异步通过 Go 调用命名函数，等待它完成，并返回其错误状态。
func (client *Client) Call(serviceMethod string, args, reply any) error {
	call := client.Go(serviceMethod, args, reply, make(chan *Call, 1))
	done := <-call.Done
	return done.Error
}

// NewClient 创建并返回一个 Client 实例
//
// 首先需要完成一开始的协议交换，即发送 Option 信息给服务端。
// 协商好消息的编解码方式之后，再创建一个子协程调用 receive() 接收响应。
func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	codecFunc := codec.NewCodecFuncMap[opt.CodecType]
	if codecFunc == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}

	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options error:", err)
		_ = conn.Close()
		return nil, err
	}

	client := &Client{
		seq:     1, // 默认从 1 开始
		cc:      codecFunc(conn),
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	go client.receive()

	return client, nil
}

// Dial 连接到指定网络地址的 RPC 服务器，返回客户端 Client
func Dial(network, address string, opts ...*Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}

	dial, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}

	defer func() {
		// 如果 client 为 nil，则关闭对应连接
		if client == nil {
			_ = dial.Close()
		}
	}()

	return NewClient(dial, opt)
}

func parseOptions(opts ...*Option) (*Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}

	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}

	option := opts[0]
	option.MagicNumber = DefaultOption.MagicNumber
	if option.CodecType == "" {
		option.CodecType = DefaultOption.CodecType
	}
	return option, nil
}
