package com.actiontech.dble.cluster.general.impl.ushard;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.4.0)",
    comments = "Source: dbleCluster.proto")
public final class DbleClusterGrpc {

  private DbleClusterGrpc() {}

  public static final String SERVICE_NAME = "ushard.DbleCluster";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.PutKvInput,
      com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> METHOD_PUT_KV =
      io.grpc.MethodDescriptor.<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.PutKvInput, com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "ushard.DbleCluster", "PutKv"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.PutKvInput.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvInput,
      com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvOutput> METHOD_GET_KV =
      io.grpc.MethodDescriptor.<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvInput, com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvOutput>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "ushard.DbleCluster", "GetKv"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvInput.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvOutput.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.DeleteKvInput,
      com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> METHOD_DELETE_KV =
      io.grpc.MethodDescriptor.<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.DeleteKvInput, com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "ushard.DbleCluster", "DeleteKv"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.DeleteKvInput.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.DeleteKvTreeInput,
      com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> METHOD_DELETE_KV_TREE =
      io.grpc.MethodDescriptor.<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.DeleteKvTreeInput, com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "ushard.DbleCluster", "DeleteKvTree"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.DeleteKvTreeInput.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.SubscribeKvPrefixInput,
      com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.SubscribeKvPrefixOutput> METHOD_SUBSCRIBE_KV_PREFIX =
      io.grpc.MethodDescriptor.<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.SubscribeKvPrefixInput, com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.SubscribeKvPrefixOutput>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "ushard.DbleCluster", "SubscribeKvPrefix"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.SubscribeKvPrefixInput.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.SubscribeKvPrefixOutput.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvTreeInput,
      com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvTreeOutput> METHOD_GET_KV_TREE =
      io.grpc.MethodDescriptor.<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvTreeInput, com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvTreeOutput>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "ushard.DbleCluster", "GetKvTree"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvTreeInput.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvTreeOutput.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.AlertInput,
      com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> METHOD_ALERT =
      io.grpc.MethodDescriptor.<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.AlertInput, com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "ushard.DbleCluster", "Alert"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.AlertInput.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.AlertInput,
      com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> METHOD_ALERT_RESOLVE =
      io.grpc.MethodDescriptor.<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.AlertInput, com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "ushard.DbleCluster", "AlertResolve"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.AlertInput.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.LockOnSessionInput,
      com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.LockOnSessionOutput> METHOD_LOCK_ON_SESSION =
      io.grpc.MethodDescriptor.<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.LockOnSessionInput, com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.LockOnSessionOutput>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "ushard.DbleCluster", "LockOnSession"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.LockOnSessionInput.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.LockOnSessionOutput.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.RenewSessionInput,
      com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> METHOD_RENEW_SESSION =
      io.grpc.MethodDescriptor.<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.RenewSessionInput, com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "ushard.DbleCluster", "RenewSession"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.RenewSessionInput.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty.getDefaultInstance()))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.UnlockOnSessionInput,
      com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> METHOD_UNLOCK_ON_SESSION =
      io.grpc.MethodDescriptor.<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.UnlockOnSessionInput, com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "ushard.DbleCluster", "UnlockOnSession"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.UnlockOnSessionInput.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty.getDefaultInstance()))
          .build();

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static DbleClusterStub newStub(io.grpc.Channel channel) {
    return new DbleClusterStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static DbleClusterBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new DbleClusterBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static DbleClusterFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new DbleClusterFutureStub(channel);
  }

  /**
   */
  public static abstract class DbleClusterImplBase implements io.grpc.BindableService {

    /**
     */
    public void putKv(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.PutKvInput request,
                      io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_PUT_KV, responseObserver);
    }

    /**
     */
    public void getKv(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvInput request,
                      io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvOutput> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_KV, responseObserver);
    }

    /**
     */
    public void deleteKv(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.DeleteKvInput request,
                         io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DELETE_KV, responseObserver);
    }

    /**
     */
    public void deleteKvTree(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.DeleteKvTreeInput request,
                             io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DELETE_KV_TREE, responseObserver);
    }

    /**
     */
    public void subscribeKvPrefix(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.SubscribeKvPrefixInput request,
                                  io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.SubscribeKvPrefixOutput> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_SUBSCRIBE_KV_PREFIX, responseObserver);
    }

    /**
     */
    public void getKvTree(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvTreeInput request,
                          io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvTreeOutput> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_KV_TREE, responseObserver);
    }

    /**
     */
    public void alert(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.AlertInput request,
                      io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_ALERT, responseObserver);
    }

    /**
     */
    public void alertResolve(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.AlertInput request,
                             io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_ALERT_RESOLVE, responseObserver);
    }

    /**
     */
    public void lockOnSession(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.LockOnSessionInput request,
                              io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.LockOnSessionOutput> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_LOCK_ON_SESSION, responseObserver);
    }

    /**
     */
    public void renewSession(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.RenewSessionInput request,
                             io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_RENEW_SESSION, responseObserver);
    }

    /**
     */
    public void unlockOnSession(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.UnlockOnSessionInput request,
                                io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_UNLOCK_ON_SESSION, responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_PUT_KV,
            asyncUnaryCall(
              new MethodHandlers<
                com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.PutKvInput,
                com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty>(
                  this, METHODID_PUT_KV)))
          .addMethod(
            METHOD_GET_KV,
            asyncUnaryCall(
              new MethodHandlers<
                com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvInput,
                com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvOutput>(
                  this, METHODID_GET_KV)))
          .addMethod(
            METHOD_DELETE_KV,
            asyncUnaryCall(
              new MethodHandlers<
                com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.DeleteKvInput,
                com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty>(
                  this, METHODID_DELETE_KV)))
          .addMethod(
            METHOD_DELETE_KV_TREE,
            asyncUnaryCall(
              new MethodHandlers<
                com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.DeleteKvTreeInput,
                com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty>(
                  this, METHODID_DELETE_KV_TREE)))
          .addMethod(
            METHOD_SUBSCRIBE_KV_PREFIX,
            asyncUnaryCall(
              new MethodHandlers<
                com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.SubscribeKvPrefixInput,
                com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.SubscribeKvPrefixOutput>(
                  this, METHODID_SUBSCRIBE_KV_PREFIX)))
          .addMethod(
            METHOD_GET_KV_TREE,
            asyncUnaryCall(
              new MethodHandlers<
                com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvTreeInput,
                com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvTreeOutput>(
                  this, METHODID_GET_KV_TREE)))
          .addMethod(
            METHOD_ALERT,
            asyncUnaryCall(
              new MethodHandlers<
                com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.AlertInput,
                com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty>(
                  this, METHODID_ALERT)))
          .addMethod(
            METHOD_ALERT_RESOLVE,
            asyncUnaryCall(
              new MethodHandlers<
                com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.AlertInput,
                com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty>(
                  this, METHODID_ALERT_RESOLVE)))
          .addMethod(
            METHOD_LOCK_ON_SESSION,
            asyncUnaryCall(
              new MethodHandlers<
                com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.LockOnSessionInput,
                com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.LockOnSessionOutput>(
                  this, METHODID_LOCK_ON_SESSION)))
          .addMethod(
            METHOD_RENEW_SESSION,
            asyncUnaryCall(
              new MethodHandlers<
                com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.RenewSessionInput,
                com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty>(
                  this, METHODID_RENEW_SESSION)))
          .addMethod(
            METHOD_UNLOCK_ON_SESSION,
            asyncUnaryCall(
              new MethodHandlers<
                com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.UnlockOnSessionInput,
                com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty>(
                  this, METHODID_UNLOCK_ON_SESSION)))
          .build();
    }
  }

  /**
   */
  public static final class DbleClusterStub extends io.grpc.stub.AbstractStub<DbleClusterStub> {
    private DbleClusterStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DbleClusterStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DbleClusterStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DbleClusterStub(channel, callOptions);
    }

    /**
     */
    public void putKv(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.PutKvInput request,
                      io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_PUT_KV, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getKv(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvInput request,
                      io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvOutput> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_KV, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deleteKv(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.DeleteKvInput request,
                         io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DELETE_KV, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deleteKvTree(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.DeleteKvTreeInput request,
                             io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DELETE_KV_TREE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void subscribeKvPrefix(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.SubscribeKvPrefixInput request,
                                  io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.SubscribeKvPrefixOutput> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_SUBSCRIBE_KV_PREFIX, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getKvTree(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvTreeInput request,
                          io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvTreeOutput> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_KV_TREE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void alert(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.AlertInput request,
                      io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_ALERT, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void alertResolve(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.AlertInput request,
                             io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_ALERT_RESOLVE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void lockOnSession(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.LockOnSessionInput request,
                              io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.LockOnSessionOutput> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_LOCK_ON_SESSION, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void renewSession(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.RenewSessionInput request,
                             io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_RENEW_SESSION, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void unlockOnSession(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.UnlockOnSessionInput request,
                                io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_UNLOCK_ON_SESSION, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class DbleClusterBlockingStub extends io.grpc.stub.AbstractStub<DbleClusterBlockingStub> {
    private DbleClusterBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DbleClusterBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DbleClusterBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DbleClusterBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty putKv(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.PutKvInput request) {
      return blockingUnaryCall(
          getChannel(), METHOD_PUT_KV, getCallOptions(), request);
    }

    /**
     */
    public com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvOutput getKv(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvInput request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_KV, getCallOptions(), request);
    }

    /**
     */
    public com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty deleteKv(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.DeleteKvInput request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DELETE_KV, getCallOptions(), request);
    }

    /**
     */
    public com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty deleteKvTree(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.DeleteKvTreeInput request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DELETE_KV_TREE, getCallOptions(), request);
    }

    /**
     */
    public com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.SubscribeKvPrefixOutput subscribeKvPrefix(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.SubscribeKvPrefixInput request) {
      return blockingUnaryCall(
          getChannel(), METHOD_SUBSCRIBE_KV_PREFIX, getCallOptions(), request);
    }

    /**
     */
    public com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvTreeOutput getKvTree(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvTreeInput request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_KV_TREE, getCallOptions(), request);
    }

    /**
     */
    public com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty alert(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.AlertInput request) {
      return blockingUnaryCall(
          getChannel(), METHOD_ALERT, getCallOptions(), request);
    }

    /**
     */
    public com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty alertResolve(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.AlertInput request) {
      return blockingUnaryCall(
          getChannel(), METHOD_ALERT_RESOLVE, getCallOptions(), request);
    }

    /**
     */
    public com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.LockOnSessionOutput lockOnSession(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.LockOnSessionInput request) {
      return blockingUnaryCall(
          getChannel(), METHOD_LOCK_ON_SESSION, getCallOptions(), request);
    }

    /**
     */
    public com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty renewSession(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.RenewSessionInput request) {
      return blockingUnaryCall(
          getChannel(), METHOD_RENEW_SESSION, getCallOptions(), request);
    }

    /**
     */
    public com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty unlockOnSession(com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.UnlockOnSessionInput request) {
      return blockingUnaryCall(
          getChannel(), METHOD_UNLOCK_ON_SESSION, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class DbleClusterFutureStub extends io.grpc.stub.AbstractStub<DbleClusterFutureStub> {
    private DbleClusterFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DbleClusterFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DbleClusterFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DbleClusterFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> putKv(
        com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.PutKvInput request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_PUT_KV, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvOutput> getKv(
        com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvInput request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_KV, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> deleteKv(
        com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.DeleteKvInput request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DELETE_KV, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> deleteKvTree(
        com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.DeleteKvTreeInput request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DELETE_KV_TREE, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.SubscribeKvPrefixOutput> subscribeKvPrefix(
        com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.SubscribeKvPrefixInput request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_SUBSCRIBE_KV_PREFIX, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvTreeOutput> getKvTree(
        com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvTreeInput request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_KV_TREE, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> alert(
        com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.AlertInput request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_ALERT, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> alertResolve(
        com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.AlertInput request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_ALERT_RESOLVE, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.LockOnSessionOutput> lockOnSession(
        com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.LockOnSessionInput request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_LOCK_ON_SESSION, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> renewSession(
        com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.RenewSessionInput request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_RENEW_SESSION, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty> unlockOnSession(
        com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.UnlockOnSessionInput request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_UNLOCK_ON_SESSION, getCallOptions()), request);
    }
  }

  private static final int METHODID_PUT_KV = 0;
  private static final int METHODID_GET_KV = 1;
  private static final int METHODID_DELETE_KV = 2;
  private static final int METHODID_DELETE_KV_TREE = 3;
  private static final int METHODID_SUBSCRIBE_KV_PREFIX = 4;
  private static final int METHODID_GET_KV_TREE = 5;
  private static final int METHODID_ALERT = 6;
  private static final int METHODID_ALERT_RESOLVE = 7;
  private static final int METHODID_LOCK_ON_SESSION = 8;
  private static final int METHODID_RENEW_SESSION = 9;
  private static final int METHODID_UNLOCK_ON_SESSION = 10;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final DbleClusterImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(DbleClusterImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_PUT_KV:
          serviceImpl.putKv((com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.PutKvInput) request,
              (io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty>) responseObserver);
          break;
        case METHODID_GET_KV:
          serviceImpl.getKv((com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvInput) request,
              (io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvOutput>) responseObserver);
          break;
        case METHODID_DELETE_KV:
          serviceImpl.deleteKv((com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.DeleteKvInput) request,
              (io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty>) responseObserver);
          break;
        case METHODID_DELETE_KV_TREE:
          serviceImpl.deleteKvTree((com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.DeleteKvTreeInput) request,
              (io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty>) responseObserver);
          break;
        case METHODID_SUBSCRIBE_KV_PREFIX:
          serviceImpl.subscribeKvPrefix((com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.SubscribeKvPrefixInput) request,
              (io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.SubscribeKvPrefixOutput>) responseObserver);
          break;
        case METHODID_GET_KV_TREE:
          serviceImpl.getKvTree((com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvTreeInput) request,
              (io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.GetKvTreeOutput>) responseObserver);
          break;
        case METHODID_ALERT:
          serviceImpl.alert((com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.AlertInput) request,
              (io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty>) responseObserver);
          break;
        case METHODID_ALERT_RESOLVE:
          serviceImpl.alertResolve((com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.AlertInput) request,
              (io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty>) responseObserver);
          break;
        case METHODID_LOCK_ON_SESSION:
          serviceImpl.lockOnSession((com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.LockOnSessionInput) request,
              (io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.LockOnSessionOutput>) responseObserver);
          break;
        case METHODID_RENEW_SESSION:
          serviceImpl.renewSession((com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.RenewSessionInput) request,
              (io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty>) responseObserver);
          break;
        case METHODID_UNLOCK_ON_SESSION:
          serviceImpl.unlockOnSession((com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.UnlockOnSessionInput) request,
              (io.grpc.stub.StreamObserver<com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.Empty>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static final class DbleClusterDescriptorSupplier implements io.grpc.protobuf.ProtoFileDescriptorSupplier {
    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.actiontech.dble.cluster.general.impl.ushard.UshardInterface.getDescriptor();
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (DbleClusterGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new DbleClusterDescriptorSupplier())
              .addMethod(METHOD_PUT_KV)
              .addMethod(METHOD_GET_KV)
              .addMethod(METHOD_DELETE_KV)
              .addMethod(METHOD_DELETE_KV_TREE)
              .addMethod(METHOD_SUBSCRIBE_KV_PREFIX)
              .addMethod(METHOD_GET_KV_TREE)
              .addMethod(METHOD_ALERT)
              .addMethod(METHOD_ALERT_RESOLVE)
              .addMethod(METHOD_LOCK_ON_SESSION)
              .addMethod(METHOD_RENEW_SESSION)
              .addMethod(METHOD_UNLOCK_ON_SESSION)
              .build();
        }
      }
    }
    return result;
  }
}
