package codec

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

type GobCodec struct {
	conn io.ReadWriteCloser
	buf  *bufio.Writer
	dec  *gob.Decoder
	enc  *gob.Encoder
}

var _ Codec = (*GobCodec)(nil)

func NewGobCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &GobCodec{
		conn: conn,
		buf:  buf,
		// 基于连接 conn 构造编解码器
		dec: gob.NewDecoder(conn),
		enc: gob.NewEncoder(buf),
	}
}

func (c *GobCodec) Close() error {
	return c.conn.Close()
}

// gob.Decoder 在解码时会智能地处理数据流，即使数据是以追加方式进行写入的。
// gob 编解码器的设计允许它在流式数据中准确地解析出结构体。

func (c *GobCodec) ReadHeader(header *Header) error {
	return c.dec.Decode(header)
}

func (c *GobCodec) ReadBody(body interface{}) error {
	return c.dec.Decode(body)
}

func (c *GobCodec) Write(header *Header, body interface{}) error {
	defer func() {
		// 将缓冲区中的数据实际写入到底层连接（conn）中，并发送请求
		err := c.buf.Flush()
		if err != nil {
			_ = c.Close()
		}
	}()
	// 编码成字节流，并写入 buf 缓冲区
	if err := c.enc.Encode(header); err != nil {
		log.Println("rpc codec: gob error encoding header:", err)
		return err
	}
	if err := c.enc.Encode(body); err != nil {
		log.Println("rpc codec: gob error encoding body:", err)
		return err
	}
	return nil
}
