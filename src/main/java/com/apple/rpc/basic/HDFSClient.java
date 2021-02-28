package com.apple.rpc.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;
// https://www.bilibili.com/video/BV1Qp4y1n7EN?t=4&p=171
public class HDFSClient {
    public static void main(String[] args) throws IOException {
        RPCProtocol client = RPC.getProxy(RPCProtocol.class, RPCProtocol.versionID, new InetSocketAddress("localhost", 8888), new Configuration());
        System.out.println("Client start.");
        client.mkdirs("/data");
    }
}
