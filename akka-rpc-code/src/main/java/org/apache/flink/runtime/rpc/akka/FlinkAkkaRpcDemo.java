package org.apache.flink.runtime.rpc.akka;

import akka.actor.ActorSystem;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.concurrent.ExecutionException;

public class FlinkAkkaRpcDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // 1 启动 RPC 服务
        ActorSystem defaultActorSystem = AkkaUtils.createDefaultActorSystem();
        AkkaRpcService akkaRpcService = new AkkaRpcService(defaultActorSystem,
                AkkaRpcServiceConfiguration.defaultConfiguration());

        // 2 创建 RpcEndpoint 实例，启动 RPC 服务
        GetNowRpcEndpoint getNowRpcEndpoint = new GetNowRpcEndpoint(akkaRpcService);
        getNowRpcEndpoint.start();

        // 3 客户端和服务端在同一个节点： 通过 selfGateway 调用 RPC 服务
        GetNowGateway selfGateway = getNowRpcEndpoint.getSelfGateway(GetNowGateway.class);
        // 发送 RPC 请求
        String getNowResult1 = selfGateway.getNow();
        System.out.println(getNowResult1);

        // 3 客户端和服务端在不同节点： 通过 RpcEndpoint 地址获得代理
        GetNowGateway getNowGateway = akkaRpcService.connect(getNowRpcEndpoint.getAddress(), GetNowGateway.class).get();
        // 发送 RPC 请求
        String getNowResult2 = getNowGateway.getNow();
        System.out.println(getNowResult1);

    }

    // 定义 Gateway 就类似于 Hadoop 定义 Protocol 定义通信协议
    public interface GetNowGateway extends RpcGateway {
        String getNow();
    }

    // 运行在服务端中的一个 RpcEndpoint 工作组件，实现了 GetNowGateway 通信协议，其实就是为 GetNowGateway.getNow() 提供 RPC 请求的具体实现
    static class GetNowRpcEndpoint extends RpcEndpoint implements GetNowGateway {
        // 当通过 RPC 服务端启动了一个 RpcEndpoint 组件的时候，其实首先就是调用了 onStart()
        @Override
        protected void onStart() throws Exception {
            System.out.println("GetNowRpcEndpoint 启动");
        }

        protected GetNowRpcEndpoint(RpcService rpcService) {
            super(rpcService);
        }

        @Override
        public String getNow() {
            return "2023-06-29 10:10:10";
        }
    }

}
