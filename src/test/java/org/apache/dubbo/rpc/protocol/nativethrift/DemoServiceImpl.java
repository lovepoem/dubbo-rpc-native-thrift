package org.apache.dubbo.rpc.protocol.nativethrift;

import com.alibaba.dubbo.rpc.RpcContext;

import org.apache.thrift.TException;

public class DemoServiceImpl implements DemoService.Iface {
    private boolean called;

    public String sayHello(String name) {
        called = true;
        return "Hello, " + name;
    }

    @Override
    public boolean hasName(boolean hasName) throws TException {
        return hasName;
    }

    @Override
    public String sayHelloTimes(String name, int times) throws TException {
        return null;
    }

    public String sayHello(String name, int times) {
        called = true;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < times; i++) {
            sb.append("Hello, " + name + ". ");
        }
        return sb.toString();
    }

    public boolean isCalled() {
        return called;
    }

    public void timeOut(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String customException() {
        throw new MyException("custom exception");
    }

    public String context(String name) {
        return "Hello, " + name + " context, " + RpcContext.getContext().getAttachment("myContext");
    }

    static class MyException extends RuntimeException {

        private static final long serialVersionUID = -3051041116483629056L;

        public MyException(String message) {
            super(message);
        }
    }
}
