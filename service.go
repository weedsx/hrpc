package main

import (
	"go/ast"
	"log"
	"reflect"
	"sync/atomic"
)

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
