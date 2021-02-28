package com.apple.rpc.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
// https://www.bilibili.com/video/BV1Qp4y1n7EN?t=4&p=171
public class NNServer implements RPCProtocol {
    public static void main(String[] args) throws IOException {
        RPC.Server server = new RPC.Builder(new Configuration())
                .setBindAddress("localhost")
                .setPort(8888)
                .setProtocol(RPCProtocol.class)
                .setInstance(new NNServer())
                .build();
        System.out.println("Server start.");
        server.start();
    }
    @Override
    public void mkdirs(String path) {
        System.out.println("Client requests: " + path);
    }
}
