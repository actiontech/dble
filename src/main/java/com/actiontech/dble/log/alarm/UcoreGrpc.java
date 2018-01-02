/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.log.alarm;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

/**
 */
@javax.annotation.Generated(
        value = "by gRPC proto compiler (version 1.4.0)",
        comments = "Source: ucore.proto")
public final class UcoreGrpc {

    private UcoreGrpc() {
    }

    public static final String SERVICE_NAME = "ucore.Ucore";

    // Static method descriptors that strictly reflect the proto.
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.PutKvInput,
            com.actiontech.dble.log.alarm.UcoreInterface.Empty> METHOD_PUT_KV =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.PutKvInput, com.actiontech.dble.log.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "PutKv"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.PutKvInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.GetKvInput,
            com.actiontech.dble.log.alarm.UcoreInterface.GetKvOutput> METHOD_GET_KV =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.GetKvInput, com.actiontech.dble.log.alarm.UcoreInterface.GetKvOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "GetKv"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.GetKvInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.GetKvOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvInput,
            com.actiontech.dble.log.alarm.UcoreInterface.Empty> METHOD_DELETE_KV =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvInput, com.actiontech.dble.log.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "DeleteKv"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvsInput,
            com.actiontech.dble.log.alarm.UcoreInterface.Empty> METHOD_DELETE_KVS =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvsInput, com.actiontech.dble.log.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "DeleteKvs"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvsInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvTreeInput,
            com.actiontech.dble.log.alarm.UcoreInterface.Empty> METHOD_DELETE_KV_TREE =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvTreeInput, com.actiontech.dble.log.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "DeleteKvTree"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvTreeInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvInput,
            com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvOutput> METHOD_SUBSCRIBE_KV =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvInput, com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "SubscribeKv"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixInput,
            com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixOutput> METHOD_SUBSCRIBE_KV_PREFIX =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixInput, com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "SubscribeKvPrefix"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixsInput,
            com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixsOutput> METHOD_SUBSCRIBE_KV_PREFIXS =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixsInput, com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixsOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "SubscribeKvPrefixs"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixsInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixsOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeNodesInput,
            com.actiontech.dble.log.alarm.UcoreInterface.SubscribeNodesOutput> METHOD_SUBSCRIBE_NODES =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeNodesInput, com.actiontech.dble.log.alarm.UcoreInterface.SubscribeNodesOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "SubscribeNodes"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.SubscribeNodesInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.SubscribeNodesOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeInput,
            com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeOutput> METHOD_GET_KV_TREE =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeInput, com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "GetKvTree"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeInput,
            com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeOutput> METHOD_GET_KV_TREE_RAW =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeInput, com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "GetKvTreeRaw"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.ListKvKeysInput,
            com.actiontech.dble.log.alarm.UcoreInterface.ListKvKeysOutput> METHOD_LIST_KV_KEYS =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.ListKvKeysInput, com.actiontech.dble.log.alarm.UcoreInterface.ListKvKeysOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "ListKvKeys"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.ListKvKeysInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.ListKvKeysOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.PutKvsInput,
            com.actiontech.dble.log.alarm.UcoreInterface.Empty> METHOD_PUT_KVS =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.PutKvsInput, com.actiontech.dble.log.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "PutKvs"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.PutKvsInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.ServiceHeartbeatInput,
            com.actiontech.dble.log.alarm.UcoreInterface.Empty> METHOD_SERVICE_HEARTBEAT =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.ServiceHeartbeatInput, com.actiontech.dble.log.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "ServiceHeartbeat"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.ServiceHeartbeatInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.RegisterServiceInput,
            com.actiontech.dble.log.alarm.UcoreInterface.Empty> METHOD_REGISTER_SERVICE =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.RegisterServiceInput, com.actiontech.dble.log.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "RegisterService"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.RegisterServiceInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigStartInput,
            com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigStartOutput> METHOD_UPDATE_CONFIG_START =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigStartInput, com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigStartOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "UpdateConfigStart"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigStartInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigStartOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigCommitInput,
            com.actiontech.dble.log.alarm.UcoreInterface.Empty> METHOD_UPDATE_CONFIG_COMMIT =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigCommitInput, com.actiontech.dble.log.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "UpdateConfigCommit"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigCommitInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigRollbackInput,
            com.actiontech.dble.log.alarm.UcoreInterface.Empty> METHOD_UPDATE_CONFIG_ROLLBACK =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigRollbackInput, com.actiontech.dble.log.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "UpdateConfigRollback"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigRollbackInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.AlertInput,
            com.actiontech.dble.log.alarm.UcoreInterface.Empty> METHOD_ALERT =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.AlertInput, com.actiontech.dble.log.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "Alert"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.AlertInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.AlertInput,
            com.actiontech.dble.log.alarm.UcoreInterface.Empty> METHOD_ALERT_RESOLVE =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.AlertInput, com.actiontech.dble.log.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "AlertResolve"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.AlertInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.AlertResolveByFingerprintInput,
            com.actiontech.dble.log.alarm.UcoreInterface.Empty> METHOD_ALERT_RESOLVE_BY_FINGERPRINT =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.AlertResolveByFingerprintInput, com.actiontech.dble.log.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "AlertResolveByFingerprint"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.AlertResolveByFingerprintInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.SendAlertTestMessageInput,
            com.actiontech.dble.log.alarm.UcoreInterface.Empty> METHOD_SEND_ALERT_TEST_MESSAGE =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.SendAlertTestMessageInput, com.actiontech.dble.log.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "SendAlertTestMessage"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.SendAlertTestMessageInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.Empty,
            com.actiontech.dble.log.alarm.UcoreInterface.GetUsageOutput> METHOD_GET_USAGE =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.Empty, com.actiontech.dble.log.alarm.UcoreInterface.GetUsageOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "GetUsage"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.GetUsageOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.RequestServerCertificateInput,
            com.actiontech.dble.log.alarm.UcoreInterface.RequestServerCertificateOutput> METHOD_REQUEST_SERVER_CERTIFICATE =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.RequestServerCertificateInput, com.actiontech.dble.log.alarm.UcoreInterface.RequestServerCertificateOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "RequestServerCertificate"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.RequestServerCertificateInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.RequestServerCertificateOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.Empty,
            com.actiontech.dble.log.alarm.UcoreInterface.GetCaCertificateOutput> METHOD_GET_CA_CERTIFICATE =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.Empty, com.actiontech.dble.log.alarm.UcoreInterface.GetCaCertificateOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "GetCaCertificate"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.GetCaCertificateOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.Empty,
            com.actiontech.dble.log.alarm.UcoreInterface.Empty> METHOD_IS_UCORE_INIT =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.Empty, com.actiontech.dble.log.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "IsUcoreInit"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.CollectClusterInfosInput,
            com.actiontech.dble.log.alarm.UcoreInterface.CollectClusterInfosOutput> METHOD_COLLECT_CLUSTER_INFOS =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.CollectClusterInfosInput, com.actiontech.dble.log.alarm.UcoreInterface.CollectClusterInfosOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "CollectClusterInfos"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.CollectClusterInfosInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.CollectClusterInfosOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.actiontech.dble.log.alarm.UcoreInterface.Empty,
            com.actiontech.dble.log.alarm.UcoreInterface.Empty> METHOD_UCORE_HEALTH_CHECK =
            io.grpc.MethodDescriptor.<com.actiontech.dble.log.alarm.UcoreInterface.Empty, com.actiontech.dble.log.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "UcoreHealthCheck"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.actiontech.dble.log.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();

    /**
     * Creates a new async stub that supports all call types for the service
     */
    public static UcoreStub newStub(io.grpc.Channel channel) {
        return new UcoreStub(channel);
    }

    /**
     * Creates a new blocking-style stub that supports unary and streaming output calls on the service
     */
    public static UcoreBlockingStub newBlockingStub(
            io.grpc.Channel channel) {
        return new UcoreBlockingStub(channel);
    }

    /**
     * Creates a new ListenableFuture-style stub that supports unary calls on the service
     */
    public static UcoreFutureStub newFutureStub(
            io.grpc.Channel channel) {
        return new UcoreFutureStub(channel);
    }

    /**
     */
    public static abstract class UcoreImplBase implements io.grpc.BindableService {

        /**
         * <pre>
         * PutKv guarantee atomic.
         * </pre>
         */
        public void putKv(com.actiontech.dble.log.alarm.UcoreInterface.PutKvInput request,
                          io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_PUT_KV, responseObserver);
        }

        /**
         * <pre>
         * GetKv guarantee atomic.
         * </pre>
         */
        public void getKv(com.actiontech.dble.log.alarm.UcoreInterface.GetKvInput request,
                          io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.GetKvOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_GET_KV, responseObserver);
        }

        /**
         * <pre>
         * DeleteKv guarantee atomic.
         * </pre>
         */
        public void deleteKv(com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvInput request,
                             io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_DELETE_KV, responseObserver);
        }

        /**
         * <pre>
         * DeleteKvs guarantee atomic.
         * </pre>
         */
        public void deleteKvs(com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvsInput request,
                              io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_DELETE_KVS, responseObserver);
        }

        /**
         * <pre>
         * DeleteKvTree guarantee atomic.
         * </pre>
         */
        public void deleteKvTree(com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvTreeInput request,
                                 io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_DELETE_KV_TREE, responseObserver);
        }

        /**
         * <pre>
         * SubscribeKv returns when k-v changes or timeout.
         * </pre>
         */
        public void subscribeKv(com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvInput request,
                                io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_SUBSCRIBE_KV, responseObserver);
        }

        /**
         * <pre>
         * SubscribeKvPrefix returns when k-v with prefix changes or timeout.
         * </pre>
         */
        public void subscribeKvPrefix(com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixInput request,
                                      io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_SUBSCRIBE_KV_PREFIX, responseObserver);
        }

        /**
         * <pre>
         * SubscribeKvPrefixs subscribe multi k-v prefixs, returns targets changes or timeout.
         * </pre>
         */
        public void subscribeKvPrefixs(com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixsInput request,
                                       io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixsOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_SUBSCRIBE_KV_PREFIXS, responseObserver);
        }

        /**
         * <pre>
         * SubscribeKvPrefixs returns when consul nodes changes or timeout.
         * </pre>
         */
        public void subscribeNodes(com.actiontech.dble.log.alarm.UcoreInterface.SubscribeNodesInput request,
                                   io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeNodesOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_SUBSCRIBE_NODES, responseObserver);
        }

        /**
         * <pre>
         * GetKvTree guarantee atomic.
         * </pre>
         */
        public void getKvTree(com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeInput request,
                              io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_GET_KV_TREE, responseObserver);
        }

        /**
         * <pre>
         * GetKvTreeRaw guarantee atomic.
         * </pre>
         */
        public void getKvTreeRaw(com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeInput request,
                                 io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_GET_KV_TREE_RAW, responseObserver);
        }

        /**
         * <pre>
         * ListKvKeys guarantee atomic.
         * </pre>
         */
        public void listKvKeys(com.actiontech.dble.log.alarm.UcoreInterface.ListKvKeysInput request,
                               io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.ListKvKeysOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_LIST_KV_KEYS, responseObserver);
        }

        /**
         * <pre>
         * PutKvs guarantee atomic.
         * </pre>
         */
        public void putKvs(com.actiontech.dble.log.alarm.UcoreInterface.PutKvsInput request,
                           io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_PUT_KVS, responseObserver);
        }

        /**
         * <pre>
         * ServiceHeartbeat, ucore-guarded service should report heartbeat via `ServiceHeartbeat`.
         * </pre>
         */
        public void serviceHeartbeat(com.actiontech.dble.log.alarm.UcoreInterface.ServiceHeartbeatInput request,
                                     io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_SERVICE_HEARTBEAT, responseObserver);
        }

        /**
         * <pre>
         * RegisterService mark service as ucore-guarded service.
         * </pre>
         */
        public void registerService(com.actiontech.dble.log.alarm.UcoreInterface.RegisterServiceInput request,
                                    io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_REGISTER_SERVICE, responseObserver);
        }

        /**
         * <pre>
         * UpdateConfigStart is of 3-phase commit.
         * </pre>
         */
        public void updateConfigStart(com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigStartInput request,
                                      io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigStartOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_UPDATE_CONFIG_START, responseObserver);
        }

        /**
         * <pre>
         * UpdateConfigCommit is of 3-phase commit.
         * </pre>
         */
        public void updateConfigCommit(com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigCommitInput request,
                                       io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_UPDATE_CONFIG_COMMIT, responseObserver);
        }

        /**
         * <pre>
         * UpdateConfigRollback is of 3-phase commit.
         * </pre>
         */
        public void updateConfigRollback(com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigRollbackInput request,
                                         io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_UPDATE_CONFIG_ROLLBACK, responseObserver);
        }

        /**
         * <pre>
         * Alert record an alert message in consul, alertmanager should send notification after some time.
         * </pre>
         */
        public void alert(com.actiontech.dble.log.alarm.UcoreInterface.AlertInput request,
                          io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_ALERT, responseObserver);
        }

        /**
         * <pre>
         * AlertResolve mark some alerts as resolved.
         * </pre>
         */
        public void alertResolve(com.actiontech.dble.log.alarm.UcoreInterface.AlertInput request,
                                 io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_ALERT_RESOLVE, responseObserver);
        }

        /**
         * <pre>
         * AlertResolveByFingerprint mark the alert as resolved.
         * </pre>
         */
        public void alertResolveByFingerprint(com.actiontech.dble.log.alarm.UcoreInterface.AlertResolveByFingerprintInput request,
                                              io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_ALERT_RESOLVE_BY_FINGERPRINT, responseObserver);
        }

        /**
         */
        public void sendAlertTestMessage(com.actiontech.dble.log.alarm.UcoreInterface.SendAlertTestMessageInput request,
                                         io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_SEND_ALERT_TEST_MESSAGE, responseObserver);
        }

        /**
         * <pre>
         * GetUsage returns ucore rpc call usages.
         * </pre>
         */
        public void getUsage(com.actiontech.dble.log.alarm.UcoreInterface.Empty request,
                             io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.GetUsageOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_GET_USAGE, responseObserver);
        }

        /**
         */
        public void requestServerCertificate(com.actiontech.dble.log.alarm.UcoreInterface.RequestServerCertificateInput request,
                                             io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.RequestServerCertificateOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_REQUEST_SERVER_CERTIFICATE, responseObserver);
        }

        /**
         */
        public void getCaCertificate(com.actiontech.dble.log.alarm.UcoreInterface.Empty request,
                                     io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.GetCaCertificateOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_GET_CA_CERTIFICATE, responseObserver);
        }

        /**
         * <pre>
         * IsUcoreInit returns if consul is started.
         * </pre>
         */
        public void isUcoreInit(com.actiontech.dble.log.alarm.UcoreInterface.Empty request,
                                io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_IS_UCORE_INIT, responseObserver);
        }

        /**
         * <pre>
         * Collect local nodes infos
         * </pre>
         */
        public void collectClusterInfos(com.actiontech.dble.log.alarm.UcoreInterface.CollectClusterInfosInput request,
                                        io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.CollectClusterInfosOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_COLLECT_CLUSTER_INFOS, responseObserver);
        }

        /**
         * <pre>
         * UcoreHealthCheck check health of ucore and if its leader
         * </pre>
         */
        public void ucoreHealthCheck(com.actiontech.dble.log.alarm.UcoreInterface.Empty request,
                                     io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_UCORE_HEALTH_CHECK, responseObserver);
        }

        @java.lang.Override
        public final io.grpc.ServerServiceDefinition bindService() {
            return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
                    .addMethod(
                            METHOD_PUT_KV,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.PutKvInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.Empty>(
                                            this, METHODID_PUT_KV)))
                    .addMethod(
                            METHOD_GET_KV,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.GetKvInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.GetKvOutput>(
                                            this, METHODID_GET_KV)))
                    .addMethod(
                            METHOD_DELETE_KV,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.Empty>(
                                            this, METHODID_DELETE_KV)))
                    .addMethod(
                            METHOD_DELETE_KVS,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvsInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.Empty>(
                                            this, METHODID_DELETE_KVS)))
                    .addMethod(
                            METHOD_DELETE_KV_TREE,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvTreeInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.Empty>(
                                            this, METHODID_DELETE_KV_TREE)))
                    .addMethod(
                            METHOD_SUBSCRIBE_KV,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvOutput>(
                                            this, METHODID_SUBSCRIBE_KV)))
                    .addMethod(
                            METHOD_SUBSCRIBE_KV_PREFIX,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixOutput>(
                                            this, METHODID_SUBSCRIBE_KV_PREFIX)))
                    .addMethod(
                            METHOD_SUBSCRIBE_KV_PREFIXS,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixsInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixsOutput>(
                                            this, METHODID_SUBSCRIBE_KV_PREFIXS)))
                    .addMethod(
                            METHOD_SUBSCRIBE_NODES,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.SubscribeNodesInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.SubscribeNodesOutput>(
                                            this, METHODID_SUBSCRIBE_NODES)))
                    .addMethod(
                            METHOD_GET_KV_TREE,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeOutput>(
                                            this, METHODID_GET_KV_TREE)))
                    .addMethod(
                            METHOD_GET_KV_TREE_RAW,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeOutput>(
                                            this, METHODID_GET_KV_TREE_RAW)))
                    .addMethod(
                            METHOD_LIST_KV_KEYS,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.ListKvKeysInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.ListKvKeysOutput>(
                                            this, METHODID_LIST_KV_KEYS)))
                    .addMethod(
                            METHOD_PUT_KVS,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.PutKvsInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.Empty>(
                                            this, METHODID_PUT_KVS)))
                    .addMethod(
                            METHOD_SERVICE_HEARTBEAT,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.ServiceHeartbeatInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.Empty>(
                                            this, METHODID_SERVICE_HEARTBEAT)))
                    .addMethod(
                            METHOD_REGISTER_SERVICE,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.RegisterServiceInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.Empty>(
                                            this, METHODID_REGISTER_SERVICE)))
                    .addMethod(
                            METHOD_UPDATE_CONFIG_START,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigStartInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigStartOutput>(
                                            this, METHODID_UPDATE_CONFIG_START)))
                    .addMethod(
                            METHOD_UPDATE_CONFIG_COMMIT,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigCommitInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.Empty>(
                                            this, METHODID_UPDATE_CONFIG_COMMIT)))
                    .addMethod(
                            METHOD_UPDATE_CONFIG_ROLLBACK,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigRollbackInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.Empty>(
                                            this, METHODID_UPDATE_CONFIG_ROLLBACK)))
                    .addMethod(
                            METHOD_ALERT,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.AlertInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.Empty>(
                                            this, METHODID_ALERT)))
                    .addMethod(
                            METHOD_ALERT_RESOLVE,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.AlertInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.Empty>(
                                            this, METHODID_ALERT_RESOLVE)))
                    .addMethod(
                            METHOD_ALERT_RESOLVE_BY_FINGERPRINT,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.AlertResolveByFingerprintInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.Empty>(
                                            this, METHODID_ALERT_RESOLVE_BY_FINGERPRINT)))
                    .addMethod(
                            METHOD_SEND_ALERT_TEST_MESSAGE,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.SendAlertTestMessageInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.Empty>(
                                            this, METHODID_SEND_ALERT_TEST_MESSAGE)))
                    .addMethod(
                            METHOD_GET_USAGE,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.Empty,
                                            com.actiontech.dble.log.alarm.UcoreInterface.GetUsageOutput>(
                                            this, METHODID_GET_USAGE)))
                    .addMethod(
                            METHOD_REQUEST_SERVER_CERTIFICATE,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.RequestServerCertificateInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.RequestServerCertificateOutput>(
                                            this, METHODID_REQUEST_SERVER_CERTIFICATE)))
                    .addMethod(
                            METHOD_GET_CA_CERTIFICATE,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.Empty,
                                            com.actiontech.dble.log.alarm.UcoreInterface.GetCaCertificateOutput>(
                                            this, METHODID_GET_CA_CERTIFICATE)))
                    .addMethod(
                            METHOD_IS_UCORE_INIT,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.Empty,
                                            com.actiontech.dble.log.alarm.UcoreInterface.Empty>(
                                            this, METHODID_IS_UCORE_INIT)))
                    .addMethod(
                            METHOD_COLLECT_CLUSTER_INFOS,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.CollectClusterInfosInput,
                                            com.actiontech.dble.log.alarm.UcoreInterface.CollectClusterInfosOutput>(
                                            this, METHODID_COLLECT_CLUSTER_INFOS)))
                    .addMethod(
                            METHOD_UCORE_HEALTH_CHECK,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.actiontech.dble.log.alarm.UcoreInterface.Empty,
                                            com.actiontech.dble.log.alarm.UcoreInterface.Empty>(
                                            this, METHODID_UCORE_HEALTH_CHECK)))
                    .build();
        }
    }

    /**
     */
    public static final class UcoreStub extends io.grpc.stub.AbstractStub<UcoreStub> {
        private UcoreStub(io.grpc.Channel channel) {
            super(channel);
        }

        private UcoreStub(io.grpc.Channel channel,
                          io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected UcoreStub build(io.grpc.Channel channel,
                                  io.grpc.CallOptions callOptions) {
            return new UcoreStub(channel, callOptions);
        }

        /**
         * <pre>
         * PutKv guarantee atomic.
         * </pre>
         */
        public void putKv(com.actiontech.dble.log.alarm.UcoreInterface.PutKvInput request,
                          io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_PUT_KV, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * GetKv guarantee atomic.
         * </pre>
         */
        public void getKv(com.actiontech.dble.log.alarm.UcoreInterface.GetKvInput request,
                          io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.GetKvOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_GET_KV, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * DeleteKv guarantee atomic.
         * </pre>
         */
        public void deleteKv(com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvInput request,
                             io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_DELETE_KV, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * DeleteKvs guarantee atomic.
         * </pre>
         */
        public void deleteKvs(com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvsInput request,
                              io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_DELETE_KVS, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * DeleteKvTree guarantee atomic.
         * </pre>
         */
        public void deleteKvTree(com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvTreeInput request,
                                 io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_DELETE_KV_TREE, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * SubscribeKv returns when k-v changes or timeout.
         * </pre>
         */
        public void subscribeKv(com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvInput request,
                                io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_SUBSCRIBE_KV, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * SubscribeKvPrefix returns when k-v with prefix changes or timeout.
         * </pre>
         */
        public void subscribeKvPrefix(com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixInput request,
                                      io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_SUBSCRIBE_KV_PREFIX, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * SubscribeKvPrefixs subscribe multi k-v prefixs, returns targets changes or timeout.
         * </pre>
         */
        public void subscribeKvPrefixs(com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixsInput request,
                                       io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixsOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_SUBSCRIBE_KV_PREFIXS, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * SubscribeKvPrefixs returns when consul nodes changes or timeout.
         * </pre>
         */
        public void subscribeNodes(com.actiontech.dble.log.alarm.UcoreInterface.SubscribeNodesInput request,
                                   io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeNodesOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_SUBSCRIBE_NODES, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * GetKvTree guarantee atomic.
         * </pre>
         */
        public void getKvTree(com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeInput request,
                              io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_GET_KV_TREE, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * GetKvTreeRaw guarantee atomic.
         * </pre>
         */
        public void getKvTreeRaw(com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeInput request,
                                 io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_GET_KV_TREE_RAW, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * ListKvKeys guarantee atomic.
         * </pre>
         */
        public void listKvKeys(com.actiontech.dble.log.alarm.UcoreInterface.ListKvKeysInput request,
                               io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.ListKvKeysOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_LIST_KV_KEYS, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * PutKvs guarantee atomic.
         * </pre>
         */
        public void putKvs(com.actiontech.dble.log.alarm.UcoreInterface.PutKvsInput request,
                           io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_PUT_KVS, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * ServiceHeartbeat, ucore-guarded service should report heartbeat via `ServiceHeartbeat`.
         * </pre>
         */
        public void serviceHeartbeat(com.actiontech.dble.log.alarm.UcoreInterface.ServiceHeartbeatInput request,
                                     io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_SERVICE_HEARTBEAT, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * RegisterService mark service as ucore-guarded service.
         * </pre>
         */
        public void registerService(com.actiontech.dble.log.alarm.UcoreInterface.RegisterServiceInput request,
                                    io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_REGISTER_SERVICE, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * UpdateConfigStart is of 3-phase commit.
         * </pre>
         */
        public void updateConfigStart(com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigStartInput request,
                                      io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigStartOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_UPDATE_CONFIG_START, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * UpdateConfigCommit is of 3-phase commit.
         * </pre>
         */
        public void updateConfigCommit(com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigCommitInput request,
                                       io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_UPDATE_CONFIG_COMMIT, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * UpdateConfigRollback is of 3-phase commit.
         * </pre>
         */
        public void updateConfigRollback(com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigRollbackInput request,
                                         io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_UPDATE_CONFIG_ROLLBACK, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * Alert record an alert message in consul, alertmanager should send notification after some time.
         * </pre>
         */
        public void alert(com.actiontech.dble.log.alarm.UcoreInterface.AlertInput request,
                          io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_ALERT, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * AlertResolve mark some alerts as resolved.
         * </pre>
         */
        public void alertResolve(com.actiontech.dble.log.alarm.UcoreInterface.AlertInput request,
                                 io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_ALERT_RESOLVE, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * AlertResolveByFingerprint mark the alert as resolved.
         * </pre>
         */
        public void alertResolveByFingerprint(com.actiontech.dble.log.alarm.UcoreInterface.AlertResolveByFingerprintInput request,
                                              io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_ALERT_RESOLVE_BY_FINGERPRINT, getCallOptions()), request, responseObserver);
        }

        /**
         */
        public void sendAlertTestMessage(com.actiontech.dble.log.alarm.UcoreInterface.SendAlertTestMessageInput request,
                                         io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_SEND_ALERT_TEST_MESSAGE, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * GetUsage returns ucore rpc call usages.
         * </pre>
         */
        public void getUsage(com.actiontech.dble.log.alarm.UcoreInterface.Empty request,
                             io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.GetUsageOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_GET_USAGE, getCallOptions()), request, responseObserver);
        }

        /**
         */
        public void requestServerCertificate(com.actiontech.dble.log.alarm.UcoreInterface.RequestServerCertificateInput request,
                                             io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.RequestServerCertificateOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_REQUEST_SERVER_CERTIFICATE, getCallOptions()), request, responseObserver);
        }

        /**
         */
        public void getCaCertificate(com.actiontech.dble.log.alarm.UcoreInterface.Empty request,
                                     io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.GetCaCertificateOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_GET_CA_CERTIFICATE, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * IsUcoreInit returns if consul is started.
         * </pre>
         */
        public void isUcoreInit(com.actiontech.dble.log.alarm.UcoreInterface.Empty request,
                                io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_IS_UCORE_INIT, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * Collect local nodes infos
         * </pre>
         */
        public void collectClusterInfos(com.actiontech.dble.log.alarm.UcoreInterface.CollectClusterInfosInput request,
                                        io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.CollectClusterInfosOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_COLLECT_CLUSTER_INFOS, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * UcoreHealthCheck check health of ucore and if its leader
         * </pre>
         */
        public void ucoreHealthCheck(com.actiontech.dble.log.alarm.UcoreInterface.Empty request,
                                     io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_UCORE_HEALTH_CHECK, getCallOptions()), request, responseObserver);
        }
    }

    /**
     */
    public static final class UcoreBlockingStub extends io.grpc.stub.AbstractStub<UcoreBlockingStub> {
        private UcoreBlockingStub(io.grpc.Channel channel) {
            super(channel);
        }

        private UcoreBlockingStub(io.grpc.Channel channel,
                                  io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected UcoreBlockingStub build(io.grpc.Channel channel,
                                          io.grpc.CallOptions callOptions) {
            return new UcoreBlockingStub(channel, callOptions);
        }

        /**
         * <pre>
         * PutKv guarantee atomic.
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.Empty putKv(com.actiontech.dble.log.alarm.UcoreInterface.PutKvInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_PUT_KV, getCallOptions(), request);
        }

        /**
         * <pre>
         * GetKv guarantee atomic.
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.GetKvOutput getKv(com.actiontech.dble.log.alarm.UcoreInterface.GetKvInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_GET_KV, getCallOptions(), request);
        }

        /**
         * <pre>
         * DeleteKv guarantee atomic.
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.Empty deleteKv(com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_DELETE_KV, getCallOptions(), request);
        }

        /**
         * <pre>
         * DeleteKvs guarantee atomic.
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.Empty deleteKvs(com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvsInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_DELETE_KVS, getCallOptions(), request);
        }

        /**
         * <pre>
         * DeleteKvTree guarantee atomic.
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.Empty deleteKvTree(com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvTreeInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_DELETE_KV_TREE, getCallOptions(), request);
        }

        /**
         * <pre>
         * SubscribeKv returns when k-v changes or timeout.
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvOutput subscribeKv(com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_SUBSCRIBE_KV, getCallOptions(), request);
        }

        /**
         * <pre>
         * SubscribeKvPrefix returns when k-v with prefix changes or timeout.
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixOutput subscribeKvPrefix(com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_SUBSCRIBE_KV_PREFIX, getCallOptions(), request);
        }

        /**
         * <pre>
         * SubscribeKvPrefixs subscribe multi k-v prefixs, returns targets changes or timeout.
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixsOutput subscribeKvPrefixs(com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixsInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_SUBSCRIBE_KV_PREFIXS, getCallOptions(), request);
        }

        /**
         * <pre>
         * SubscribeKvPrefixs returns when consul nodes changes or timeout.
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.SubscribeNodesOutput subscribeNodes(com.actiontech.dble.log.alarm.UcoreInterface.SubscribeNodesInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_SUBSCRIBE_NODES, getCallOptions(), request);
        }

        /**
         * <pre>
         * GetKvTree guarantee atomic.
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeOutput getKvTree(com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_GET_KV_TREE, getCallOptions(), request);
        }

        /**
         * <pre>
         * GetKvTreeRaw guarantee atomic.
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeOutput getKvTreeRaw(com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_GET_KV_TREE_RAW, getCallOptions(), request);
        }

        /**
         * <pre>
         * ListKvKeys guarantee atomic.
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.ListKvKeysOutput listKvKeys(com.actiontech.dble.log.alarm.UcoreInterface.ListKvKeysInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_LIST_KV_KEYS, getCallOptions(), request);
        }

        /**
         * <pre>
         * PutKvs guarantee atomic.
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.Empty putKvs(com.actiontech.dble.log.alarm.UcoreInterface.PutKvsInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_PUT_KVS, getCallOptions(), request);
        }

        /**
         * <pre>
         * ServiceHeartbeat, ucore-guarded service should report heartbeat via `ServiceHeartbeat`.
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.Empty serviceHeartbeat(com.actiontech.dble.log.alarm.UcoreInterface.ServiceHeartbeatInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_SERVICE_HEARTBEAT, getCallOptions(), request);
        }

        /**
         * <pre>
         * RegisterService mark service as ucore-guarded service.
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.Empty registerService(com.actiontech.dble.log.alarm.UcoreInterface.RegisterServiceInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_REGISTER_SERVICE, getCallOptions(), request);
        }

        /**
         * <pre>
         * UpdateConfigStart is of 3-phase commit.
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigStartOutput updateConfigStart(com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigStartInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_UPDATE_CONFIG_START, getCallOptions(), request);
        }

        /**
         * <pre>
         * UpdateConfigCommit is of 3-phase commit.
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.Empty updateConfigCommit(com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigCommitInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_UPDATE_CONFIG_COMMIT, getCallOptions(), request);
        }

        /**
         * <pre>
         * UpdateConfigRollback is of 3-phase commit.
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.Empty updateConfigRollback(com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigRollbackInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_UPDATE_CONFIG_ROLLBACK, getCallOptions(), request);
        }

        /**
         * <pre>
         * Alert record an alert message in consul, alertmanager should send notification after some time.
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.Empty alert(com.actiontech.dble.log.alarm.UcoreInterface.AlertInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_ALERT, getCallOptions(), request);
        }

        /**
         * <pre>
         * AlertResolve mark some alerts as resolved.
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.Empty alertResolve(com.actiontech.dble.log.alarm.UcoreInterface.AlertInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_ALERT_RESOLVE, getCallOptions(), request);
        }

        /**
         * <pre>
         * AlertResolveByFingerprint mark the alert as resolved.
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.Empty alertResolveByFingerprint(com.actiontech.dble.log.alarm.UcoreInterface.AlertResolveByFingerprintInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_ALERT_RESOLVE_BY_FINGERPRINT, getCallOptions(), request);
        }

        /**
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.Empty sendAlertTestMessage(com.actiontech.dble.log.alarm.UcoreInterface.SendAlertTestMessageInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_SEND_ALERT_TEST_MESSAGE, getCallOptions(), request);
        }

        /**
         * <pre>
         * GetUsage returns ucore rpc call usages.
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.GetUsageOutput getUsage(com.actiontech.dble.log.alarm.UcoreInterface.Empty request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_GET_USAGE, getCallOptions(), request);
        }

        /**
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.RequestServerCertificateOutput requestServerCertificate(com.actiontech.dble.log.alarm.UcoreInterface.RequestServerCertificateInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_REQUEST_SERVER_CERTIFICATE, getCallOptions(), request);
        }

        /**
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.GetCaCertificateOutput getCaCertificate(com.actiontech.dble.log.alarm.UcoreInterface.Empty request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_GET_CA_CERTIFICATE, getCallOptions(), request);
        }

        /**
         * <pre>
         * IsUcoreInit returns if consul is started.
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.Empty isUcoreInit(com.actiontech.dble.log.alarm.UcoreInterface.Empty request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_IS_UCORE_INIT, getCallOptions(), request);
        }

        /**
         * <pre>
         * Collect local nodes infos
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.CollectClusterInfosOutput collectClusterInfos(com.actiontech.dble.log.alarm.UcoreInterface.CollectClusterInfosInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_COLLECT_CLUSTER_INFOS, getCallOptions(), request);
        }

        /**
         * <pre>
         * UcoreHealthCheck check health of ucore and if its leader
         * </pre>
         */
        public com.actiontech.dble.log.alarm.UcoreInterface.Empty ucoreHealthCheck(com.actiontech.dble.log.alarm.UcoreInterface.Empty request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_UCORE_HEALTH_CHECK, getCallOptions(), request);
        }
    }

    /**
     */
    public static final class UcoreFutureStub extends io.grpc.stub.AbstractStub<UcoreFutureStub> {
        private UcoreFutureStub(io.grpc.Channel channel) {
            super(channel);
        }

        private UcoreFutureStub(io.grpc.Channel channel,
                                io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected UcoreFutureStub build(io.grpc.Channel channel,
                                        io.grpc.CallOptions callOptions) {
            return new UcoreFutureStub(channel, callOptions);
        }

        /**
         * <pre>
         * PutKv guarantee atomic.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.Empty> putKv(
                com.actiontech.dble.log.alarm.UcoreInterface.PutKvInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_PUT_KV, getCallOptions()), request);
        }

        /**
         * <pre>
         * GetKv guarantee atomic.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.GetKvOutput> getKv(
                com.actiontech.dble.log.alarm.UcoreInterface.GetKvInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_GET_KV, getCallOptions()), request);
        }

        /**
         * <pre>
         * DeleteKv guarantee atomic.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.Empty> deleteKv(
                com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_DELETE_KV, getCallOptions()), request);
        }

        /**
         * <pre>
         * DeleteKvs guarantee atomic.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.Empty> deleteKvs(
                com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvsInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_DELETE_KVS, getCallOptions()), request);
        }

        /**
         * <pre>
         * DeleteKvTree guarantee atomic.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.Empty> deleteKvTree(
                com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvTreeInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_DELETE_KV_TREE, getCallOptions()), request);
        }

        /**
         * <pre>
         * SubscribeKv returns when k-v changes or timeout.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvOutput> subscribeKv(
                com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_SUBSCRIBE_KV, getCallOptions()), request);
        }

        /**
         * <pre>
         * SubscribeKvPrefix returns when k-v with prefix changes or timeout.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixOutput> subscribeKvPrefix(
                com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_SUBSCRIBE_KV_PREFIX, getCallOptions()), request);
        }

        /**
         * <pre>
         * SubscribeKvPrefixs subscribe multi k-v prefixs, returns targets changes or timeout.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixsOutput> subscribeKvPrefixs(
                com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixsInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_SUBSCRIBE_KV_PREFIXS, getCallOptions()), request);
        }

        /**
         * <pre>
         * SubscribeKvPrefixs returns when consul nodes changes or timeout.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeNodesOutput> subscribeNodes(
                com.actiontech.dble.log.alarm.UcoreInterface.SubscribeNodesInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_SUBSCRIBE_NODES, getCallOptions()), request);
        }

        /**
         * <pre>
         * GetKvTree guarantee atomic.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeOutput> getKvTree(
                com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_GET_KV_TREE, getCallOptions()), request);
        }

        /**
         * <pre>
         * GetKvTreeRaw guarantee atomic.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeOutput> getKvTreeRaw(
                com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_GET_KV_TREE_RAW, getCallOptions()), request);
        }

        /**
         * <pre>
         * ListKvKeys guarantee atomic.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.ListKvKeysOutput> listKvKeys(
                com.actiontech.dble.log.alarm.UcoreInterface.ListKvKeysInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_LIST_KV_KEYS, getCallOptions()), request);
        }

        /**
         * <pre>
         * PutKvs guarantee atomic.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.Empty> putKvs(
                com.actiontech.dble.log.alarm.UcoreInterface.PutKvsInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_PUT_KVS, getCallOptions()), request);
        }

        /**
         * <pre>
         * ServiceHeartbeat, ucore-guarded service should report heartbeat via `ServiceHeartbeat`.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.Empty> serviceHeartbeat(
                com.actiontech.dble.log.alarm.UcoreInterface.ServiceHeartbeatInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_SERVICE_HEARTBEAT, getCallOptions()), request);
        }

        /**
         * <pre>
         * RegisterService mark service as ucore-guarded service.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.Empty> registerService(
                com.actiontech.dble.log.alarm.UcoreInterface.RegisterServiceInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_REGISTER_SERVICE, getCallOptions()), request);
        }

        /**
         * <pre>
         * UpdateConfigStart is of 3-phase commit.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigStartOutput> updateConfigStart(
                com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigStartInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_UPDATE_CONFIG_START, getCallOptions()), request);
        }

        /**
         * <pre>
         * UpdateConfigCommit is of 3-phase commit.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.Empty> updateConfigCommit(
                com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigCommitInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_UPDATE_CONFIG_COMMIT, getCallOptions()), request);
        }

        /**
         * <pre>
         * UpdateConfigRollback is of 3-phase commit.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.Empty> updateConfigRollback(
                com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigRollbackInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_UPDATE_CONFIG_ROLLBACK, getCallOptions()), request);
        }

        /**
         * <pre>
         * Alert record an alert message in consul, alertmanager should send notification after some time.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.Empty> alert(
                com.actiontech.dble.log.alarm.UcoreInterface.AlertInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_ALERT, getCallOptions()), request);
        }

        /**
         * <pre>
         * AlertResolve mark some alerts as resolved.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.Empty> alertResolve(
                com.actiontech.dble.log.alarm.UcoreInterface.AlertInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_ALERT_RESOLVE, getCallOptions()), request);
        }

        /**
         * <pre>
         * AlertResolveByFingerprint mark the alert as resolved.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.Empty> alertResolveByFingerprint(
                com.actiontech.dble.log.alarm.UcoreInterface.AlertResolveByFingerprintInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_ALERT_RESOLVE_BY_FINGERPRINT, getCallOptions()), request);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.Empty> sendAlertTestMessage(
                com.actiontech.dble.log.alarm.UcoreInterface.SendAlertTestMessageInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_SEND_ALERT_TEST_MESSAGE, getCallOptions()), request);
        }

        /**
         * <pre>
         * GetUsage returns ucore rpc call usages.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.GetUsageOutput> getUsage(
                com.actiontech.dble.log.alarm.UcoreInterface.Empty request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_GET_USAGE, getCallOptions()), request);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.RequestServerCertificateOutput> requestServerCertificate(
                com.actiontech.dble.log.alarm.UcoreInterface.RequestServerCertificateInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_REQUEST_SERVER_CERTIFICATE, getCallOptions()), request);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.GetCaCertificateOutput> getCaCertificate(
                com.actiontech.dble.log.alarm.UcoreInterface.Empty request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_GET_CA_CERTIFICATE, getCallOptions()), request);
        }

        /**
         * <pre>
         * IsUcoreInit returns if consul is started.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.Empty> isUcoreInit(
                com.actiontech.dble.log.alarm.UcoreInterface.Empty request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_IS_UCORE_INIT, getCallOptions()), request);
        }

        /**
         * <pre>
         * Collect local nodes infos
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.CollectClusterInfosOutput> collectClusterInfos(
                com.actiontech.dble.log.alarm.UcoreInterface.CollectClusterInfosInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_COLLECT_CLUSTER_INFOS, getCallOptions()), request);
        }

        /**
         * <pre>
         * UcoreHealthCheck check health of ucore and if its leader
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.actiontech.dble.log.alarm.UcoreInterface.Empty> ucoreHealthCheck(
                com.actiontech.dble.log.alarm.UcoreInterface.Empty request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_UCORE_HEALTH_CHECK, getCallOptions()), request);
        }
    }

    private static final int METHODID_PUT_KV = 0;
    private static final int METHODID_GET_KV = 1;
    private static final int METHODID_DELETE_KV = 2;
    private static final int METHODID_DELETE_KVS = 3;
    private static final int METHODID_DELETE_KV_TREE = 4;
    private static final int METHODID_SUBSCRIBE_KV = 5;
    private static final int METHODID_SUBSCRIBE_KV_PREFIX = 6;
    private static final int METHODID_SUBSCRIBE_KV_PREFIXS = 7;
    private static final int METHODID_SUBSCRIBE_NODES = 8;
    private static final int METHODID_GET_KV_TREE = 9;
    private static final int METHODID_GET_KV_TREE_RAW = 10;
    private static final int METHODID_LIST_KV_KEYS = 11;
    private static final int METHODID_PUT_KVS = 12;
    private static final int METHODID_SERVICE_HEARTBEAT = 13;
    private static final int METHODID_REGISTER_SERVICE = 14;
    private static final int METHODID_UPDATE_CONFIG_START = 15;
    private static final int METHODID_UPDATE_CONFIG_COMMIT = 16;
    private static final int METHODID_UPDATE_CONFIG_ROLLBACK = 17;
    private static final int METHODID_ALERT = 18;
    private static final int METHODID_ALERT_RESOLVE = 19;
    private static final int METHODID_ALERT_RESOLVE_BY_FINGERPRINT = 20;
    private static final int METHODID_SEND_ALERT_TEST_MESSAGE = 21;
    private static final int METHODID_GET_USAGE = 22;
    private static final int METHODID_REQUEST_SERVER_CERTIFICATE = 23;
    private static final int METHODID_GET_CA_CERTIFICATE = 24;
    private static final int METHODID_IS_UCORE_INIT = 25;
    private static final int METHODID_COLLECT_CLUSTER_INFOS = 26;
    private static final int METHODID_UCORE_HEALTH_CHECK = 27;

    private static final class MethodHandlers<Req, Resp> implements
            io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
            io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
            io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
            io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
        private final UcoreImplBase serviceImpl;
        private final int methodId;

        MethodHandlers(UcoreImplBase serviceImpl, int methodId) {
            this.serviceImpl = serviceImpl;
            this.methodId = methodId;
        }

        @java.lang.Override
        @java.lang.SuppressWarnings("unchecked")
        public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
            switch (methodId) {
                case METHODID_PUT_KV:
                    serviceImpl.putKv((com.actiontech.dble.log.alarm.UcoreInterface.PutKvInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_GET_KV:
                    serviceImpl.getKv((com.actiontech.dble.log.alarm.UcoreInterface.GetKvInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.GetKvOutput>) responseObserver);
                    break;
                case METHODID_DELETE_KV:
                    serviceImpl.deleteKv((com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_DELETE_KVS:
                    serviceImpl.deleteKvs((com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvsInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_DELETE_KV_TREE:
                    serviceImpl.deleteKvTree((com.actiontech.dble.log.alarm.UcoreInterface.DeleteKvTreeInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_SUBSCRIBE_KV:
                    serviceImpl.subscribeKv((com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvOutput>) responseObserver);
                    break;
                case METHODID_SUBSCRIBE_KV_PREFIX:
                    serviceImpl.subscribeKvPrefix((com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixOutput>) responseObserver);
                    break;
                case METHODID_SUBSCRIBE_KV_PREFIXS:
                    serviceImpl.subscribeKvPrefixs((com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixsInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeKvPrefixsOutput>) responseObserver);
                    break;
                case METHODID_SUBSCRIBE_NODES:
                    serviceImpl.subscribeNodes((com.actiontech.dble.log.alarm.UcoreInterface.SubscribeNodesInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.SubscribeNodesOutput>) responseObserver);
                    break;
                case METHODID_GET_KV_TREE:
                    serviceImpl.getKvTree((com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeOutput>) responseObserver);
                    break;
                case METHODID_GET_KV_TREE_RAW:
                    serviceImpl.getKvTreeRaw((com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.GetKvTreeOutput>) responseObserver);
                    break;
                case METHODID_LIST_KV_KEYS:
                    serviceImpl.listKvKeys((com.actiontech.dble.log.alarm.UcoreInterface.ListKvKeysInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.ListKvKeysOutput>) responseObserver);
                    break;
                case METHODID_PUT_KVS:
                    serviceImpl.putKvs((com.actiontech.dble.log.alarm.UcoreInterface.PutKvsInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_SERVICE_HEARTBEAT:
                    serviceImpl.serviceHeartbeat((com.actiontech.dble.log.alarm.UcoreInterface.ServiceHeartbeatInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_REGISTER_SERVICE:
                    serviceImpl.registerService((com.actiontech.dble.log.alarm.UcoreInterface.RegisterServiceInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_UPDATE_CONFIG_START:
                    serviceImpl.updateConfigStart((com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigStartInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigStartOutput>) responseObserver);
                    break;
                case METHODID_UPDATE_CONFIG_COMMIT:
                    serviceImpl.updateConfigCommit((com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigCommitInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_UPDATE_CONFIG_ROLLBACK:
                    serviceImpl.updateConfigRollback((com.actiontech.dble.log.alarm.UcoreInterface.UpdateConfigRollbackInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_ALERT:
                    serviceImpl.alert((com.actiontech.dble.log.alarm.UcoreInterface.AlertInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_ALERT_RESOLVE:
                    serviceImpl.alertResolve((com.actiontech.dble.log.alarm.UcoreInterface.AlertInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_ALERT_RESOLVE_BY_FINGERPRINT:
                    serviceImpl.alertResolveByFingerprint((com.actiontech.dble.log.alarm.UcoreInterface.AlertResolveByFingerprintInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_SEND_ALERT_TEST_MESSAGE:
                    serviceImpl.sendAlertTestMessage((com.actiontech.dble.log.alarm.UcoreInterface.SendAlertTestMessageInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_GET_USAGE:
                    serviceImpl.getUsage((com.actiontech.dble.log.alarm.UcoreInterface.Empty) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.GetUsageOutput>) responseObserver);
                    break;
                case METHODID_REQUEST_SERVER_CERTIFICATE:
                    serviceImpl.requestServerCertificate((com.actiontech.dble.log.alarm.UcoreInterface.RequestServerCertificateInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.RequestServerCertificateOutput>) responseObserver);
                    break;
                case METHODID_GET_CA_CERTIFICATE:
                    serviceImpl.getCaCertificate((com.actiontech.dble.log.alarm.UcoreInterface.Empty) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.GetCaCertificateOutput>) responseObserver);
                    break;
                case METHODID_IS_UCORE_INIT:
                    serviceImpl.isUcoreInit((com.actiontech.dble.log.alarm.UcoreInterface.Empty) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_COLLECT_CLUSTER_INFOS:
                    serviceImpl.collectClusterInfos((com.actiontech.dble.log.alarm.UcoreInterface.CollectClusterInfosInput) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.CollectClusterInfosOutput>) responseObserver);
                    break;
                case METHODID_UCORE_HEALTH_CHECK:
                    serviceImpl.ucoreHealthCheck((com.actiontech.dble.log.alarm.UcoreInterface.Empty) request,
                            (io.grpc.stub.StreamObserver<com.actiontech.dble.log.alarm.UcoreInterface.Empty>) responseObserver);
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

    private static final class UcoreDescriptorSupplier implements io.grpc.protobuf.ProtoFileDescriptorSupplier {
        @java.lang.Override
        public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
            return com.actiontech.dble.log.alarm.UcoreInterface.getDescriptor();
        }
    }

    private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

    public static io.grpc.ServiceDescriptor getServiceDescriptor() {
        io.grpc.ServiceDescriptor result = serviceDescriptor;
        if (result == null) {
            synchronized (UcoreGrpc.class) {
                result = serviceDescriptor;
                if (result == null) {
                    serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
                            .setSchemaDescriptor(new UcoreDescriptorSupplier())
                            .addMethod(METHOD_PUT_KV)
                            .addMethod(METHOD_GET_KV)
                            .addMethod(METHOD_DELETE_KV)
                            .addMethod(METHOD_DELETE_KVS)
                            .addMethod(METHOD_DELETE_KV_TREE)
                            .addMethod(METHOD_SUBSCRIBE_KV)
                            .addMethod(METHOD_SUBSCRIBE_KV_PREFIX)
                            .addMethod(METHOD_SUBSCRIBE_KV_PREFIXS)
                            .addMethod(METHOD_SUBSCRIBE_NODES)
                            .addMethod(METHOD_GET_KV_TREE)
                            .addMethod(METHOD_GET_KV_TREE_RAW)
                            .addMethod(METHOD_LIST_KV_KEYS)
                            .addMethod(METHOD_PUT_KVS)
                            .addMethod(METHOD_SERVICE_HEARTBEAT)
                            .addMethod(METHOD_REGISTER_SERVICE)
                            .addMethod(METHOD_UPDATE_CONFIG_START)
                            .addMethod(METHOD_UPDATE_CONFIG_COMMIT)
                            .addMethod(METHOD_UPDATE_CONFIG_ROLLBACK)
                            .addMethod(METHOD_ALERT)
                            .addMethod(METHOD_ALERT_RESOLVE)
                            .addMethod(METHOD_ALERT_RESOLVE_BY_FINGERPRINT)
                            .addMethod(METHOD_SEND_ALERT_TEST_MESSAGE)
                            .addMethod(METHOD_GET_USAGE)
                            .addMethod(METHOD_REQUEST_SERVER_CERTIFICATE)
                            .addMethod(METHOD_GET_CA_CERTIFICATE)
                            .addMethod(METHOD_IS_UCORE_INIT)
                            .addMethod(METHOD_COLLECT_CLUSTER_INFOS)
                            .addMethod(METHOD_UCORE_HEALTH_CHECK)
                            .build();
                }
            }
        }
        return result;
    }
}
