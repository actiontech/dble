/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.alarm;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 *
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
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.PutKvInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> METHOD_PUT_KV =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.PutKvInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "PutKv"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.PutKvInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvOutput> METHOD_GET_KV =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "GetKv"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> METHOD_DELETE_KV =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "DeleteKv"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvsInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> METHOD_DELETE_KVS =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvsInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "DeleteKvs"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvsInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvTreeInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> METHOD_DELETE_KV_TREE =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvTreeInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "DeleteKvTree"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvTreeInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvOutput> METHOD_SUBSCRIBE_KV =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "SubscribeKv"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixOutput> METHOD_SUBSCRIBE_KV_PREFIX =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "SubscribeKvPrefix"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixsInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixsOutput> METHOD_SUBSCRIBE_KV_PREFIXS =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixsInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixsOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "SubscribeKvPrefixs"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixsInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixsOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeNodesInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeNodesOutput> METHOD_SUBSCRIBE_NODES =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeNodesInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeNodesOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "SubscribeNodes"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeNodesInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeNodesOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeOutput> METHOD_GET_KV_TREE =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "GetKvTree"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeOutput> METHOD_GET_KV_TREE_RAW =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "GetKvTreeRaw"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvKeysInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvKeysOutput> METHOD_LIST_KV_KEYS =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvKeysInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvKeysOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "ListKvKeys"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvKeysInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvKeysOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.PutKvsInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> METHOD_PUT_KVS =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.PutKvsInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "PutKvs"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.PutKvsInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.ServiceHeartbeatInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> METHOD_SERVICE_HEARTBEAT =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.ServiceHeartbeatInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "ServiceHeartbeat"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.ServiceHeartbeatInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.RegisterServiceInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> METHOD_REGISTER_SERVICE =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.RegisterServiceInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "RegisterService"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.RegisterServiceInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigStartInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigStartOutput> METHOD_UPDATE_CONFIG_START =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigStartInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigStartOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "UpdateConfigStart"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigStartInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigStartOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigCommitInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> METHOD_UPDATE_CONFIG_COMMIT =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigCommitInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "UpdateConfigCommit"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigCommitInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigRollbackInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> METHOD_UPDATE_CONFIG_ROLLBACK =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigRollbackInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "UpdateConfigRollback"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigRollbackInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> METHOD_ALERT =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "Alert"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> METHOD_ALERT_RESOLVE =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "AlertResolve"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertResolveByFingerprintInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> METHOD_ALERT_RESOLVE_BY_FINGERPRINT =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertResolveByFingerprintInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "AlertResolveByFingerprint"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertResolveByFingerprintInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUnresolvedAlertsNumInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUnresolvedAlertsNumOutput> METHOD_GET_UNRESOLVED_ALERTS_NUM =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUnresolvedAlertsNumInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUnresolvedAlertsNumOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "GetUnresolvedAlertsNum"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUnresolvedAlertsNumInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUnresolvedAlertsNumOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.SendAlertTestMessageInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> METHOD_SEND_ALERT_TEST_MESSAGE =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.SendAlertTestMessageInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "SendAlertTestMessage"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.SendAlertTestMessageInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUsageOutput> METHOD_GET_USAGE =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty, com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUsageOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "GetUsage"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUsageOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.RequestServerCertificateInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.RequestServerCertificateOutput> METHOD_REQUEST_SERVER_CERTIFICATE =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.RequestServerCertificateInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.RequestServerCertificateOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "RequestServerCertificate"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.RequestServerCertificateInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.RequestServerCertificateOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetCaCertificateOutput> METHOD_GET_CA_CERTIFICATE =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty, com.oceanbase.obsharding_d.alarm.UcoreInterface.GetCaCertificateOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "GetCaCertificate"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetCaCertificateOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> METHOD_IS_UCORE_INIT =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty, com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "IsUcoreInit"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.CollectClusterInfosInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.CollectClusterInfosOutput> METHOD_COLLECT_CLUSTER_INFOS =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.CollectClusterInfosInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.CollectClusterInfosOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "CollectClusterInfos"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.CollectClusterInfosInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.CollectClusterInfosOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> METHOD_UCORE_HEALTH_CHECK =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty, com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "UcoreHealthCheck"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvNextLevelRawInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvNextLevelRawOutput> METHOD_LIST_KV_NEXT_LEVEL_RAW =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvNextLevelRawInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvNextLevelRawOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "ListKvNextLevelRaw"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvNextLevelRawInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvNextLevelRawOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvByPathRawInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvByPathRawOutput> METHOD_LIST_KV_BY_PATH_RAW =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvByPathRawInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvByPathRawOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "ListKvByPathRaw"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvByPathRawInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvByPathRawOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.ReportUguardExerciseResultInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> METHOD_REPORT_UGUARD_EXERCISE_RESULT =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.ReportUguardExerciseResultInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "ReportUguardExerciseResult"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.ReportUguardExerciseResultInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.UcoreLearderOutput> METHOD_UCORE_LEARDER =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty, com.oceanbase.obsharding_d.alarm.UcoreInterface.UcoreLearderOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "UcoreLearder"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.UcoreLearderOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUcoreStatsOutput> METHOD_GET_UCORE_STATS =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty, com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUcoreStatsOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "GetUcoreStats"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUcoreStatsOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.ForceLeaveUcoreInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> METHOD_FORCE_LEAVE_UCORE =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.ForceLeaveUcoreInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "ForceLeaveUcore"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.ForceLeaveUcoreInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeWithLimitInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeWithLimitOutput> METHOD_GET_KV_TREE_WITH_LIMIT =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeWithLimitInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeWithLimitOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "GetKvTreeWithLimit"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeWithLimitInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeWithLimitOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvsTreeInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvsTreeOutput> METHOD_GET_KVS_TREE =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvsTreeInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvsTreeOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "GetKvsTree"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvsTreeInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvsTreeOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.LockOnSessionInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.LockOnSessionOutput> METHOD_LOCK_ON_SESSION =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.LockOnSessionInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.LockOnSessionOutput>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "LockOnSession"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.LockOnSessionInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.LockOnSessionOutput.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.RenewSessionInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> METHOD_RENEW_SESSION =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.RenewSessionInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "RenewSession"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.RenewSessionInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
                    .build();
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<com.oceanbase.obsharding_d.alarm.UcoreInterface.UnlockOnSessionInput,
            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> METHOD_UNLOCK_ON_SESSION =
            io.grpc.MethodDescriptor.<com.oceanbase.obsharding_d.alarm.UcoreInterface.UnlockOnSessionInput, com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName(
                            "ucore.Ucore", "UnlockOnSession"))
                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.UnlockOnSessionInput.getDefaultInstance()))
                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty.getDefaultInstance()))
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
     *
     */
    public static abstract class UcoreImplBase implements io.grpc.BindableService {

        /**
         * <pre>
         * PutKv guarantee atomic.
         * </pre>
         */
        public void putKv(com.oceanbase.obsharding_d.alarm.UcoreInterface.PutKvInput request,
                          io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_PUT_KV, responseObserver);
        }

        /**
         * <pre>
         * GetKv guarantee atomic.
         * </pre>
         */
        public void getKv(com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvInput request,
                          io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_GET_KV, responseObserver);
        }

        /**
         * <pre>
         * DeleteKv guarantee atomic.
         * </pre>
         */
        public void deleteKv(com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvInput request,
                             io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_DELETE_KV, responseObserver);
        }

        /**
         * <pre>
         * DeleteKvs guarantee atomic.
         * </pre>
         */
        public void deleteKvs(com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvsInput request,
                              io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_DELETE_KVS, responseObserver);
        }

        /**
         * <pre>
         * DeleteKvTree guarantee atomic.
         * </pre>
         */
        public void deleteKvTree(com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvTreeInput request,
                                 io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_DELETE_KV_TREE, responseObserver);
        }

        /**
         * <pre>
         * SubscribeKv returns when k-v changes or timeout.
         * </pre>
         */
        public void subscribeKv(com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvInput request,
                                io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_SUBSCRIBE_KV, responseObserver);
        }

        /**
         * <pre>
         * SubscribeKvPrefix returns when k-v with prefix changes or timeout.
         * </pre>
         */
        public void subscribeKvPrefix(com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixInput request,
                                      io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_SUBSCRIBE_KV_PREFIX, responseObserver);
        }

        /**
         * <pre>
         * SubscribeKvPrefixs subscribe multi k-v prefixs, returns targets changes or timeout.
         * </pre>
         */
        public void subscribeKvPrefixs(com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixsInput request,
                                       io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixsOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_SUBSCRIBE_KV_PREFIXS, responseObserver);
        }

        /**
         * <pre>
         * SubscribeKvPrefixs returns when consul nodes changes or timeout.
         * </pre>
         */
        public void subscribeNodes(com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeNodesInput request,
                                   io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeNodesOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_SUBSCRIBE_NODES, responseObserver);
        }

        /**
         * <pre>
         * GetKvTree guarantee atomic.
         * </pre>
         */
        public void getKvTree(com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeInput request,
                              io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_GET_KV_TREE, responseObserver);
        }

        /**
         * <pre>
         * GetKvTreeRaw guarantee atomic.
         * </pre>
         */
        public void getKvTreeRaw(com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeInput request,
                                 io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_GET_KV_TREE_RAW, responseObserver);
        }

        /**
         * <pre>
         * ListKvKeys guarantee atomic.
         * </pre>
         */
        public void listKvKeys(com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvKeysInput request,
                               io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvKeysOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_LIST_KV_KEYS, responseObserver);
        }

        /**
         * <pre>
         * PutKvs guarantee atomic.
         * </pre>
         */
        public void putKvs(com.oceanbase.obsharding_d.alarm.UcoreInterface.PutKvsInput request,
                           io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_PUT_KVS, responseObserver);
        }

        /**
         * <pre>
         * ServiceHeartbeat, ucore-guarded service should report heartbeat via `ServiceHeartbeat`.
         * </pre>
         */
        public void serviceHeartbeat(com.oceanbase.obsharding_d.alarm.UcoreInterface.ServiceHeartbeatInput request,
                                     io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_SERVICE_HEARTBEAT, responseObserver);
        }

        /**
         * <pre>
         * RegisterService mark service as ucore-guarded service.
         * </pre>
         */
        public void registerService(com.oceanbase.obsharding_d.alarm.UcoreInterface.RegisterServiceInput request,
                                    io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_REGISTER_SERVICE, responseObserver);
        }

        /**
         * <pre>
         * UpdateConfigStart is of 3-phase commit.
         * </pre>
         */
        public void updateConfigStart(com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigStartInput request,
                                      io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigStartOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_UPDATE_CONFIG_START, responseObserver);
        }

        /**
         * <pre>
         * UpdateConfigCommit is of 3-phase commit.
         * </pre>
         */
        public void updateConfigCommit(com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigCommitInput request,
                                       io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_UPDATE_CONFIG_COMMIT, responseObserver);
        }

        /**
         * <pre>
         * UpdateConfigRollback is of 3-phase commit.
         * </pre>
         */
        public void updateConfigRollback(com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigRollbackInput request,
                                         io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_UPDATE_CONFIG_ROLLBACK, responseObserver);
        }

        /**
         * <pre>
         * Alert record an alert message in consul, alertmanager should send notification after some time.
         * </pre>
         */
        public void alert(com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertInput request,
                          io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_ALERT, responseObserver);
        }

        /**
         * <pre>
         * AlertResolve mark some alerts as resolved.
         * </pre>
         */
        public void alertResolve(com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertInput request,
                                 io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_ALERT_RESOLVE, responseObserver);
        }

        /**
         * <pre>
         * AlertResolveByFingerprint mark the alert as resolved.
         * </pre>
         */
        public void alertResolveByFingerprint(com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertResolveByFingerprintInput request,
                                              io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_ALERT_RESOLVE_BY_FINGERPRINT, responseObserver);
        }

        /**
         *
         */
        public void getUnresolvedAlertsNum(com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUnresolvedAlertsNumInput request,
                                           io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUnresolvedAlertsNumOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_GET_UNRESOLVED_ALERTS_NUM, responseObserver);
        }

        /**
         *
         */
        public void sendAlertTestMessage(com.oceanbase.obsharding_d.alarm.UcoreInterface.SendAlertTestMessageInput request,
                                         io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_SEND_ALERT_TEST_MESSAGE, responseObserver);
        }

        /**
         * <pre>
         * GetUsage returns ucore rpc call usages.
         * </pre>
         */
        public void getUsage(com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request,
                             io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUsageOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_GET_USAGE, responseObserver);
        }

        /**
         *
         */
        public void requestServerCertificate(com.oceanbase.obsharding_d.alarm.UcoreInterface.RequestServerCertificateInput request,
                                             io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.RequestServerCertificateOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_REQUEST_SERVER_CERTIFICATE, responseObserver);
        }

        /**
         *
         */
        public void getCaCertificate(com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request,
                                     io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetCaCertificateOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_GET_CA_CERTIFICATE, responseObserver);
        }

        /**
         * <pre>
         * IsUcoreInit returns if consul is started.
         * </pre>
         */
        public void isUcoreInit(com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request,
                                io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_IS_UCORE_INIT, responseObserver);
        }

        /**
         * <pre>
         * Collect local nodes infos
         * </pre>
         */
        public void collectClusterInfos(com.oceanbase.obsharding_d.alarm.UcoreInterface.CollectClusterInfosInput request,
                                        io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.CollectClusterInfosOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_COLLECT_CLUSTER_INFOS, responseObserver);
        }

        /**
         * <pre>
         * UcoreHealthCheck check health of ucore and if its leader
         * </pre>
         */
        public void ucoreHealthCheck(com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request,
                                     io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_UCORE_HEALTH_CHECK, responseObserver);
        }

        /**
         * <pre>
         * ListKvNextLevelRaw not guarantee atomic.
         * </pre>
         */
        public void listKvNextLevelRaw(com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvNextLevelRawInput request,
                                       io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvNextLevelRawOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_LIST_KV_NEXT_LEVEL_RAW, responseObserver);
        }

        /**
         * <pre>
         * ListKvByPathRaw not guarantee atomic.
         * </pre>
         */
        public void listKvByPathRaw(com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvByPathRawInput request,
                                    io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvByPathRawOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_LIST_KV_BY_PATH_RAW, responseObserver);
        }

        /**
         *
         */
        public void reportUguardExerciseResult(com.oceanbase.obsharding_d.alarm.UcoreInterface.ReportUguardExerciseResultInput request,
                                               io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_REPORT_UGUARD_EXERCISE_RESULT, responseObserver);
        }

        /**
         *
         */
        public void ucoreLearder(com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request,
                                 io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.UcoreLearderOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_UCORE_LEARDER, responseObserver);
        }

        /**
         * <pre>
         * GetUcoreStats get ucore cluster status
         * </pre>
         */
        public void getUcoreStats(com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request,
                                  io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUcoreStatsOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_GET_UCORE_STATS, responseObserver);
        }

        /**
         *
         */
        public void forceLeaveUcore(com.oceanbase.obsharding_d.alarm.UcoreInterface.ForceLeaveUcoreInput request,
                                    io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_FORCE_LEAVE_UCORE, responseObserver);
        }

        /**
         *
         */
        public void getKvTreeWithLimit(com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeWithLimitInput request,
                                       io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeWithLimitOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_GET_KV_TREE_WITH_LIMIT, responseObserver);
        }

        /**
         *
         */
        public void getKvsTree(com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvsTreeInput request,
                               io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvsTreeOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_GET_KVS_TREE, responseObserver);
        }

        /**
         *
         */
        public void lockOnSession(com.oceanbase.obsharding_d.alarm.UcoreInterface.LockOnSessionInput request,
                                  io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.LockOnSessionOutput> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_LOCK_ON_SESSION, responseObserver);
        }

        /**
         *
         */
        public void renewSession(com.oceanbase.obsharding_d.alarm.UcoreInterface.RenewSessionInput request,
                                 io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_RENEW_SESSION, responseObserver);
        }

        /**
         *
         */
        public void unlockOnSession(com.oceanbase.obsharding_d.alarm.UcoreInterface.UnlockOnSessionInput request,
                                    io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_UNLOCK_ON_SESSION, responseObserver);
        }

        @java.lang.Override
        public final io.grpc.ServerServiceDefinition bindService() {
            return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
                    .addMethod(
                            METHOD_PUT_KV,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.PutKvInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>(
                                            this, METHODID_PUT_KV)))
                    .addMethod(
                            METHOD_GET_KV,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvOutput>(
                                            this, METHODID_GET_KV)))
                    .addMethod(
                            METHOD_DELETE_KV,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>(
                                            this, METHODID_DELETE_KV)))
                    .addMethod(
                            METHOD_DELETE_KVS,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvsInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>(
                                            this, METHODID_DELETE_KVS)))
                    .addMethod(
                            METHOD_DELETE_KV_TREE,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvTreeInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>(
                                            this, METHODID_DELETE_KV_TREE)))
                    .addMethod(
                            METHOD_SUBSCRIBE_KV,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvOutput>(
                                            this, METHODID_SUBSCRIBE_KV)))
                    .addMethod(
                            METHOD_SUBSCRIBE_KV_PREFIX,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixOutput>(
                                            this, METHODID_SUBSCRIBE_KV_PREFIX)))
                    .addMethod(
                            METHOD_SUBSCRIBE_KV_PREFIXS,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixsInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixsOutput>(
                                            this, METHODID_SUBSCRIBE_KV_PREFIXS)))
                    .addMethod(
                            METHOD_SUBSCRIBE_NODES,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeNodesInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeNodesOutput>(
                                            this, METHODID_SUBSCRIBE_NODES)))
                    .addMethod(
                            METHOD_GET_KV_TREE,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeOutput>(
                                            this, METHODID_GET_KV_TREE)))
                    .addMethod(
                            METHOD_GET_KV_TREE_RAW,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeOutput>(
                                            this, METHODID_GET_KV_TREE_RAW)))
                    .addMethod(
                            METHOD_LIST_KV_KEYS,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvKeysInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvKeysOutput>(
                                            this, METHODID_LIST_KV_KEYS)))
                    .addMethod(
                            METHOD_PUT_KVS,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.PutKvsInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>(
                                            this, METHODID_PUT_KVS)))
                    .addMethod(
                            METHOD_SERVICE_HEARTBEAT,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.ServiceHeartbeatInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>(
                                            this, METHODID_SERVICE_HEARTBEAT)))
                    .addMethod(
                            METHOD_REGISTER_SERVICE,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.RegisterServiceInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>(
                                            this, METHODID_REGISTER_SERVICE)))
                    .addMethod(
                            METHOD_UPDATE_CONFIG_START,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigStartInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigStartOutput>(
                                            this, METHODID_UPDATE_CONFIG_START)))
                    .addMethod(
                            METHOD_UPDATE_CONFIG_COMMIT,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigCommitInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>(
                                            this, METHODID_UPDATE_CONFIG_COMMIT)))
                    .addMethod(
                            METHOD_UPDATE_CONFIG_ROLLBACK,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigRollbackInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>(
                                            this, METHODID_UPDATE_CONFIG_ROLLBACK)))
                    .addMethod(
                            METHOD_ALERT,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>(
                                            this, METHODID_ALERT)))
                    .addMethod(
                            METHOD_ALERT_RESOLVE,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>(
                                            this, METHODID_ALERT_RESOLVE)))
                    .addMethod(
                            METHOD_ALERT_RESOLVE_BY_FINGERPRINT,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertResolveByFingerprintInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>(
                                            this, METHODID_ALERT_RESOLVE_BY_FINGERPRINT)))
                    .addMethod(
                            METHOD_GET_UNRESOLVED_ALERTS_NUM,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUnresolvedAlertsNumInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUnresolvedAlertsNumOutput>(
                                            this, METHODID_GET_UNRESOLVED_ALERTS_NUM)))
                    .addMethod(
                            METHOD_SEND_ALERT_TEST_MESSAGE,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.SendAlertTestMessageInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>(
                                            this, METHODID_SEND_ALERT_TEST_MESSAGE)))
                    .addMethod(
                            METHOD_GET_USAGE,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUsageOutput>(
                                            this, METHODID_GET_USAGE)))
                    .addMethod(
                            METHOD_REQUEST_SERVER_CERTIFICATE,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.RequestServerCertificateInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.RequestServerCertificateOutput>(
                                            this, METHODID_REQUEST_SERVER_CERTIFICATE)))
                    .addMethod(
                            METHOD_GET_CA_CERTIFICATE,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetCaCertificateOutput>(
                                            this, METHODID_GET_CA_CERTIFICATE)))
                    .addMethod(
                            METHOD_IS_UCORE_INIT,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>(
                                            this, METHODID_IS_UCORE_INIT)))
                    .addMethod(
                            METHOD_COLLECT_CLUSTER_INFOS,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.CollectClusterInfosInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.CollectClusterInfosOutput>(
                                            this, METHODID_COLLECT_CLUSTER_INFOS)))
                    .addMethod(
                            METHOD_UCORE_HEALTH_CHECK,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>(
                                            this, METHODID_UCORE_HEALTH_CHECK)))
                    .addMethod(
                            METHOD_LIST_KV_NEXT_LEVEL_RAW,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvNextLevelRawInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvNextLevelRawOutput>(
                                            this, METHODID_LIST_KV_NEXT_LEVEL_RAW)))
                    .addMethod(
                            METHOD_LIST_KV_BY_PATH_RAW,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvByPathRawInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvByPathRawOutput>(
                                            this, METHODID_LIST_KV_BY_PATH_RAW)))
                    .addMethod(
                            METHOD_REPORT_UGUARD_EXERCISE_RESULT,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.ReportUguardExerciseResultInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>(
                                            this, METHODID_REPORT_UGUARD_EXERCISE_RESULT)))
                    .addMethod(
                            METHOD_UCORE_LEARDER,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.UcoreLearderOutput>(
                                            this, METHODID_UCORE_LEARDER)))
                    .addMethod(
                            METHOD_GET_UCORE_STATS,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUcoreStatsOutput>(
                                            this, METHODID_GET_UCORE_STATS)))
                    .addMethod(
                            METHOD_FORCE_LEAVE_UCORE,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.ForceLeaveUcoreInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>(
                                            this, METHODID_FORCE_LEAVE_UCORE)))
                    .addMethod(
                            METHOD_GET_KV_TREE_WITH_LIMIT,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeWithLimitInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeWithLimitOutput>(
                                            this, METHODID_GET_KV_TREE_WITH_LIMIT)))
                    .addMethod(
                            METHOD_GET_KVS_TREE,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvsTreeInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvsTreeOutput>(
                                            this, METHODID_GET_KVS_TREE)))
                    .addMethod(
                            METHOD_LOCK_ON_SESSION,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.LockOnSessionInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.LockOnSessionOutput>(
                                            this, METHODID_LOCK_ON_SESSION)))
                    .addMethod(
                            METHOD_RENEW_SESSION,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.RenewSessionInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>(
                                            this, METHODID_RENEW_SESSION)))
                    .addMethod(
                            METHOD_UNLOCK_ON_SESSION,
                            asyncUnaryCall(
                                    new MethodHandlers<
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.UnlockOnSessionInput,
                                            com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>(
                                            this, METHODID_UNLOCK_ON_SESSION)))
                    .build();
        }
    }

    /**
     *
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
        public void putKv(com.oceanbase.obsharding_d.alarm.UcoreInterface.PutKvInput request,
                          io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_PUT_KV, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * GetKv guarantee atomic.
         * </pre>
         */
        public void getKv(com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvInput request,
                          io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_GET_KV, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * DeleteKv guarantee atomic.
         * </pre>
         */
        public void deleteKv(com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvInput request,
                             io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_DELETE_KV, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * DeleteKvs guarantee atomic.
         * </pre>
         */
        public void deleteKvs(com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvsInput request,
                              io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_DELETE_KVS, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * DeleteKvTree guarantee atomic.
         * </pre>
         */
        public void deleteKvTree(com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvTreeInput request,
                                 io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_DELETE_KV_TREE, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * SubscribeKv returns when k-v changes or timeout.
         * </pre>
         */
        public void subscribeKv(com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvInput request,
                                io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_SUBSCRIBE_KV, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * SubscribeKvPrefix returns when k-v with prefix changes or timeout.
         * </pre>
         */
        public void subscribeKvPrefix(com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixInput request,
                                      io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_SUBSCRIBE_KV_PREFIX, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * SubscribeKvPrefixs subscribe multi k-v prefixs, returns targets changes or timeout.
         * </pre>
         */
        public void subscribeKvPrefixs(com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixsInput request,
                                       io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixsOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_SUBSCRIBE_KV_PREFIXS, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * SubscribeKvPrefixs returns when consul nodes changes or timeout.
         * </pre>
         */
        public void subscribeNodes(com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeNodesInput request,
                                   io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeNodesOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_SUBSCRIBE_NODES, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * GetKvTree guarantee atomic.
         * </pre>
         */
        public void getKvTree(com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeInput request,
                              io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_GET_KV_TREE, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * GetKvTreeRaw guarantee atomic.
         * </pre>
         */
        public void getKvTreeRaw(com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeInput request,
                                 io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_GET_KV_TREE_RAW, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * ListKvKeys guarantee atomic.
         * </pre>
         */
        public void listKvKeys(com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvKeysInput request,
                               io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvKeysOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_LIST_KV_KEYS, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * PutKvs guarantee atomic.
         * </pre>
         */
        public void putKvs(com.oceanbase.obsharding_d.alarm.UcoreInterface.PutKvsInput request,
                           io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_PUT_KVS, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * ServiceHeartbeat, ucore-guarded service should report heartbeat via `ServiceHeartbeat`.
         * </pre>
         */
        public void serviceHeartbeat(com.oceanbase.obsharding_d.alarm.UcoreInterface.ServiceHeartbeatInput request,
                                     io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_SERVICE_HEARTBEAT, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * RegisterService mark service as ucore-guarded service.
         * </pre>
         */
        public void registerService(com.oceanbase.obsharding_d.alarm.UcoreInterface.RegisterServiceInput request,
                                    io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_REGISTER_SERVICE, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * UpdateConfigStart is of 3-phase commit.
         * </pre>
         */
        public void updateConfigStart(com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigStartInput request,
                                      io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigStartOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_UPDATE_CONFIG_START, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * UpdateConfigCommit is of 3-phase commit.
         * </pre>
         */
        public void updateConfigCommit(com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigCommitInput request,
                                       io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_UPDATE_CONFIG_COMMIT, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * UpdateConfigRollback is of 3-phase commit.
         * </pre>
         */
        public void updateConfigRollback(com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigRollbackInput request,
                                         io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_UPDATE_CONFIG_ROLLBACK, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * Alert record an alert message in consul, alertmanager should send notification after some time.
         * </pre>
         */
        public void alert(com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertInput request,
                          io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_ALERT, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * AlertResolve mark some alerts as resolved.
         * </pre>
         */
        public void alertResolve(com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertInput request,
                                 io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_ALERT_RESOLVE, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * AlertResolveByFingerprint mark the alert as resolved.
         * </pre>
         */
        public void alertResolveByFingerprint(com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertResolveByFingerprintInput request,
                                              io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_ALERT_RESOLVE_BY_FINGERPRINT, getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void getUnresolvedAlertsNum(com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUnresolvedAlertsNumInput request,
                                           io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUnresolvedAlertsNumOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_GET_UNRESOLVED_ALERTS_NUM, getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void sendAlertTestMessage(com.oceanbase.obsharding_d.alarm.UcoreInterface.SendAlertTestMessageInput request,
                                         io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_SEND_ALERT_TEST_MESSAGE, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * GetUsage returns ucore rpc call usages.
         * </pre>
         */
        public void getUsage(com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request,
                             io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUsageOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_GET_USAGE, getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void requestServerCertificate(com.oceanbase.obsharding_d.alarm.UcoreInterface.RequestServerCertificateInput request,
                                             io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.RequestServerCertificateOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_REQUEST_SERVER_CERTIFICATE, getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void getCaCertificate(com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request,
                                     io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetCaCertificateOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_GET_CA_CERTIFICATE, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * IsUcoreInit returns if consul is started.
         * </pre>
         */
        public void isUcoreInit(com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request,
                                io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_IS_UCORE_INIT, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * Collect local nodes infos
         * </pre>
         */
        public void collectClusterInfos(com.oceanbase.obsharding_d.alarm.UcoreInterface.CollectClusterInfosInput request,
                                        io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.CollectClusterInfosOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_COLLECT_CLUSTER_INFOS, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * UcoreHealthCheck check health of ucore and if its leader
         * </pre>
         */
        public void ucoreHealthCheck(com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request,
                                     io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_UCORE_HEALTH_CHECK, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * ListKvNextLevelRaw not guarantee atomic.
         * </pre>
         */
        public void listKvNextLevelRaw(com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvNextLevelRawInput request,
                                       io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvNextLevelRawOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_LIST_KV_NEXT_LEVEL_RAW, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * ListKvByPathRaw not guarantee atomic.
         * </pre>
         */
        public void listKvByPathRaw(com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvByPathRawInput request,
                                    io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvByPathRawOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_LIST_KV_BY_PATH_RAW, getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void reportUguardExerciseResult(com.oceanbase.obsharding_d.alarm.UcoreInterface.ReportUguardExerciseResultInput request,
                                               io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_REPORT_UGUARD_EXERCISE_RESULT, getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void ucoreLearder(com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request,
                                 io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.UcoreLearderOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_UCORE_LEARDER, getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * GetUcoreStats get ucore cluster status
         * </pre>
         */
        public void getUcoreStats(com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request,
                                  io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUcoreStatsOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_GET_UCORE_STATS, getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void forceLeaveUcore(com.oceanbase.obsharding_d.alarm.UcoreInterface.ForceLeaveUcoreInput request,
                                    io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_FORCE_LEAVE_UCORE, getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void getKvTreeWithLimit(com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeWithLimitInput request,
                                       io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeWithLimitOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_GET_KV_TREE_WITH_LIMIT, getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void getKvsTree(com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvsTreeInput request,
                               io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvsTreeOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_GET_KVS_TREE, getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void lockOnSession(com.oceanbase.obsharding_d.alarm.UcoreInterface.LockOnSessionInput request,
                                  io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.LockOnSessionOutput> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_LOCK_ON_SESSION, getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void renewSession(com.oceanbase.obsharding_d.alarm.UcoreInterface.RenewSessionInput request,
                                 io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_RENEW_SESSION, getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void unlockOnSession(com.oceanbase.obsharding_d.alarm.UcoreInterface.UnlockOnSessionInput request,
                                    io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_UNLOCK_ON_SESSION, getCallOptions()), request, responseObserver);
        }
    }

    /**
     *
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
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty putKv(com.oceanbase.obsharding_d.alarm.UcoreInterface.PutKvInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_PUT_KV, getCallOptions(), request);
        }

        /**
         * <pre>
         * GetKv guarantee atomic.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvOutput getKv(com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_GET_KV, getCallOptions(), request);
        }

        /**
         * <pre>
         * DeleteKv guarantee atomic.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty deleteKv(com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_DELETE_KV, getCallOptions(), request);
        }

        /**
         * <pre>
         * DeleteKvs guarantee atomic.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty deleteKvs(com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvsInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_DELETE_KVS, getCallOptions(), request);
        }

        /**
         * <pre>
         * DeleteKvTree guarantee atomic.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty deleteKvTree(com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvTreeInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_DELETE_KV_TREE, getCallOptions(), request);
        }

        /**
         * <pre>
         * SubscribeKv returns when k-v changes or timeout.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvOutput subscribeKv(com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_SUBSCRIBE_KV, getCallOptions(), request);
        }

        /**
         * <pre>
         * SubscribeKvPrefix returns when k-v with prefix changes or timeout.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixOutput subscribeKvPrefix(com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_SUBSCRIBE_KV_PREFIX, getCallOptions(), request);
        }

        /**
         * <pre>
         * SubscribeKvPrefixs subscribe multi k-v prefixs, returns targets changes or timeout.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixsOutput subscribeKvPrefixs(com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixsInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_SUBSCRIBE_KV_PREFIXS, getCallOptions(), request);
        }

        /**
         * <pre>
         * SubscribeKvPrefixs returns when consul nodes changes or timeout.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeNodesOutput subscribeNodes(com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeNodesInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_SUBSCRIBE_NODES, getCallOptions(), request);
        }

        /**
         * <pre>
         * GetKvTree guarantee atomic.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeOutput getKvTree(com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_GET_KV_TREE, getCallOptions(), request);
        }

        /**
         * <pre>
         * GetKvTreeRaw guarantee atomic.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeOutput getKvTreeRaw(com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_GET_KV_TREE_RAW, getCallOptions(), request);
        }

        /**
         * <pre>
         * ListKvKeys guarantee atomic.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvKeysOutput listKvKeys(com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvKeysInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_LIST_KV_KEYS, getCallOptions(), request);
        }

        /**
         * <pre>
         * PutKvs guarantee atomic.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty putKvs(com.oceanbase.obsharding_d.alarm.UcoreInterface.PutKvsInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_PUT_KVS, getCallOptions(), request);
        }

        /**
         * <pre>
         * ServiceHeartbeat, ucore-guarded service should report heartbeat via `ServiceHeartbeat`.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty serviceHeartbeat(com.oceanbase.obsharding_d.alarm.UcoreInterface.ServiceHeartbeatInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_SERVICE_HEARTBEAT, getCallOptions(), request);
        }

        /**
         * <pre>
         * RegisterService mark service as ucore-guarded service.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty registerService(com.oceanbase.obsharding_d.alarm.UcoreInterface.RegisterServiceInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_REGISTER_SERVICE, getCallOptions(), request);
        }

        /**
         * <pre>
         * UpdateConfigStart is of 3-phase commit.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigStartOutput updateConfigStart(com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigStartInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_UPDATE_CONFIG_START, getCallOptions(), request);
        }

        /**
         * <pre>
         * UpdateConfigCommit is of 3-phase commit.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty updateConfigCommit(com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigCommitInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_UPDATE_CONFIG_COMMIT, getCallOptions(), request);
        }

        /**
         * <pre>
         * UpdateConfigRollback is of 3-phase commit.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty updateConfigRollback(com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigRollbackInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_UPDATE_CONFIG_ROLLBACK, getCallOptions(), request);
        }

        /**
         * <pre>
         * Alert record an alert message in consul, alertmanager should send notification after some time.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty alert(com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_ALERT, getCallOptions(), request);
        }

        /**
         * <pre>
         * AlertResolve mark some alerts as resolved.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty alertResolve(com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_ALERT_RESOLVE, getCallOptions(), request);
        }

        /**
         * <pre>
         * AlertResolveByFingerprint mark the alert as resolved.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty alertResolveByFingerprint(com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertResolveByFingerprintInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_ALERT_RESOLVE_BY_FINGERPRINT, getCallOptions(), request);
        }

        /**
         *
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUnresolvedAlertsNumOutput getUnresolvedAlertsNum(com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUnresolvedAlertsNumInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_GET_UNRESOLVED_ALERTS_NUM, getCallOptions(), request);
        }

        /**
         *
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty sendAlertTestMessage(com.oceanbase.obsharding_d.alarm.UcoreInterface.SendAlertTestMessageInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_SEND_ALERT_TEST_MESSAGE, getCallOptions(), request);
        }

        /**
         * <pre>
         * GetUsage returns ucore rpc call usages.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUsageOutput getUsage(com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_GET_USAGE, getCallOptions(), request);
        }

        /**
         *
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.RequestServerCertificateOutput requestServerCertificate(com.oceanbase.obsharding_d.alarm.UcoreInterface.RequestServerCertificateInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_REQUEST_SERVER_CERTIFICATE, getCallOptions(), request);
        }

        /**
         *
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.GetCaCertificateOutput getCaCertificate(com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_GET_CA_CERTIFICATE, getCallOptions(), request);
        }

        /**
         * <pre>
         * IsUcoreInit returns if consul is started.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty isUcoreInit(com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_IS_UCORE_INIT, getCallOptions(), request);
        }

        /**
         * <pre>
         * Collect local nodes infos
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.CollectClusterInfosOutput collectClusterInfos(com.oceanbase.obsharding_d.alarm.UcoreInterface.CollectClusterInfosInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_COLLECT_CLUSTER_INFOS, getCallOptions(), request);
        }

        /**
         * <pre>
         * UcoreHealthCheck check health of ucore and if its leader
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty ucoreHealthCheck(com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_UCORE_HEALTH_CHECK, getCallOptions(), request);
        }

        /**
         * <pre>
         * ListKvNextLevelRaw not guarantee atomic.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvNextLevelRawOutput listKvNextLevelRaw(com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvNextLevelRawInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_LIST_KV_NEXT_LEVEL_RAW, getCallOptions(), request);
        }

        /**
         * <pre>
         * ListKvByPathRaw not guarantee atomic.
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvByPathRawOutput listKvByPathRaw(com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvByPathRawInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_LIST_KV_BY_PATH_RAW, getCallOptions(), request);
        }

        /**
         *
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty reportUguardExerciseResult(com.oceanbase.obsharding_d.alarm.UcoreInterface.ReportUguardExerciseResultInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_REPORT_UGUARD_EXERCISE_RESULT, getCallOptions(), request);
        }

        /**
         *
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.UcoreLearderOutput ucoreLearder(com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_UCORE_LEARDER, getCallOptions(), request);
        }

        /**
         * <pre>
         * GetUcoreStats get ucore cluster status
         * </pre>
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUcoreStatsOutput getUcoreStats(com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_GET_UCORE_STATS, getCallOptions(), request);
        }

        /**
         *
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty forceLeaveUcore(com.oceanbase.obsharding_d.alarm.UcoreInterface.ForceLeaveUcoreInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_FORCE_LEAVE_UCORE, getCallOptions(), request);
        }

        /**
         *
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeWithLimitOutput getKvTreeWithLimit(com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeWithLimitInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_GET_KV_TREE_WITH_LIMIT, getCallOptions(), request);
        }

        /**
         *
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvsTreeOutput getKvsTree(com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvsTreeInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_GET_KVS_TREE, getCallOptions(), request);
        }

        /**
         *
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.LockOnSessionOutput lockOnSession(com.oceanbase.obsharding_d.alarm.UcoreInterface.LockOnSessionInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_LOCK_ON_SESSION, getCallOptions(), request);
        }

        /**
         *
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty renewSession(com.oceanbase.obsharding_d.alarm.UcoreInterface.RenewSessionInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_RENEW_SESSION, getCallOptions(), request);
        }

        /**
         *
         */
        public com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty unlockOnSession(com.oceanbase.obsharding_d.alarm.UcoreInterface.UnlockOnSessionInput request) {
            return blockingUnaryCall(
                    getChannel(), METHOD_UNLOCK_ON_SESSION, getCallOptions(), request);
        }
    }

    /**
     *
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
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> putKv(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.PutKvInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_PUT_KV, getCallOptions()), request);
        }

        /**
         * <pre>
         * GetKv guarantee atomic.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvOutput> getKv(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_GET_KV, getCallOptions()), request);
        }

        /**
         * <pre>
         * DeleteKv guarantee atomic.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> deleteKv(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_DELETE_KV, getCallOptions()), request);
        }

        /**
         * <pre>
         * DeleteKvs guarantee atomic.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> deleteKvs(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvsInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_DELETE_KVS, getCallOptions()), request);
        }

        /**
         * <pre>
         * DeleteKvTree guarantee atomic.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> deleteKvTree(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvTreeInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_DELETE_KV_TREE, getCallOptions()), request);
        }

        /**
         * <pre>
         * SubscribeKv returns when k-v changes or timeout.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvOutput> subscribeKv(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_SUBSCRIBE_KV, getCallOptions()), request);
        }

        /**
         * <pre>
         * SubscribeKvPrefix returns when k-v with prefix changes or timeout.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixOutput> subscribeKvPrefix(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_SUBSCRIBE_KV_PREFIX, getCallOptions()), request);
        }

        /**
         * <pre>
         * SubscribeKvPrefixs subscribe multi k-v prefixs, returns targets changes or timeout.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixsOutput> subscribeKvPrefixs(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixsInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_SUBSCRIBE_KV_PREFIXS, getCallOptions()), request);
        }

        /**
         * <pre>
         * SubscribeKvPrefixs returns when consul nodes changes or timeout.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeNodesOutput> subscribeNodes(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeNodesInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_SUBSCRIBE_NODES, getCallOptions()), request);
        }

        /**
         * <pre>
         * GetKvTree guarantee atomic.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeOutput> getKvTree(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_GET_KV_TREE, getCallOptions()), request);
        }

        /**
         * <pre>
         * GetKvTreeRaw guarantee atomic.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeOutput> getKvTreeRaw(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_GET_KV_TREE_RAW, getCallOptions()), request);
        }

        /**
         * <pre>
         * ListKvKeys guarantee atomic.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvKeysOutput> listKvKeys(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvKeysInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_LIST_KV_KEYS, getCallOptions()), request);
        }

        /**
         * <pre>
         * PutKvs guarantee atomic.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> putKvs(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.PutKvsInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_PUT_KVS, getCallOptions()), request);
        }

        /**
         * <pre>
         * ServiceHeartbeat, ucore-guarded service should report heartbeat via `ServiceHeartbeat`.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> serviceHeartbeat(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.ServiceHeartbeatInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_SERVICE_HEARTBEAT, getCallOptions()), request);
        }

        /**
         * <pre>
         * RegisterService mark service as ucore-guarded service.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> registerService(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.RegisterServiceInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_REGISTER_SERVICE, getCallOptions()), request);
        }

        /**
         * <pre>
         * UpdateConfigStart is of 3-phase commit.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigStartOutput> updateConfigStart(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigStartInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_UPDATE_CONFIG_START, getCallOptions()), request);
        }

        /**
         * <pre>
         * UpdateConfigCommit is of 3-phase commit.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> updateConfigCommit(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigCommitInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_UPDATE_CONFIG_COMMIT, getCallOptions()), request);
        }

        /**
         * <pre>
         * UpdateConfigRollback is of 3-phase commit.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> updateConfigRollback(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigRollbackInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_UPDATE_CONFIG_ROLLBACK, getCallOptions()), request);
        }

        /**
         * <pre>
         * Alert record an alert message in consul, alertmanager should send notification after some time.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> alert(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_ALERT, getCallOptions()), request);
        }

        /**
         * <pre>
         * AlertResolve mark some alerts as resolved.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> alertResolve(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_ALERT_RESOLVE, getCallOptions()), request);
        }

        /**
         * <pre>
         * AlertResolveByFingerprint mark the alert as resolved.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> alertResolveByFingerprint(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertResolveByFingerprintInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_ALERT_RESOLVE_BY_FINGERPRINT, getCallOptions()), request);
        }

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUnresolvedAlertsNumOutput> getUnresolvedAlertsNum(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUnresolvedAlertsNumInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_GET_UNRESOLVED_ALERTS_NUM, getCallOptions()), request);
        }

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> sendAlertTestMessage(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.SendAlertTestMessageInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_SEND_ALERT_TEST_MESSAGE, getCallOptions()), request);
        }

        /**
         * <pre>
         * GetUsage returns ucore rpc call usages.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUsageOutput> getUsage(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_GET_USAGE, getCallOptions()), request);
        }

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.RequestServerCertificateOutput> requestServerCertificate(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.RequestServerCertificateInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_REQUEST_SERVER_CERTIFICATE, getCallOptions()), request);
        }

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetCaCertificateOutput> getCaCertificate(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_GET_CA_CERTIFICATE, getCallOptions()), request);
        }

        /**
         * <pre>
         * IsUcoreInit returns if consul is started.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> isUcoreInit(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_IS_UCORE_INIT, getCallOptions()), request);
        }

        /**
         * <pre>
         * Collect local nodes infos
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.CollectClusterInfosOutput> collectClusterInfos(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.CollectClusterInfosInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_COLLECT_CLUSTER_INFOS, getCallOptions()), request);
        }

        /**
         * <pre>
         * UcoreHealthCheck check health of ucore and if its leader
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> ucoreHealthCheck(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_UCORE_HEALTH_CHECK, getCallOptions()), request);
        }

        /**
         * <pre>
         * ListKvNextLevelRaw not guarantee atomic.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvNextLevelRawOutput> listKvNextLevelRaw(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvNextLevelRawInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_LIST_KV_NEXT_LEVEL_RAW, getCallOptions()), request);
        }

        /**
         * <pre>
         * ListKvByPathRaw not guarantee atomic.
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvByPathRawOutput> listKvByPathRaw(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvByPathRawInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_LIST_KV_BY_PATH_RAW, getCallOptions()), request);
        }

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> reportUguardExerciseResult(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.ReportUguardExerciseResultInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_REPORT_UGUARD_EXERCISE_RESULT, getCallOptions()), request);
        }

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.UcoreLearderOutput> ucoreLearder(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_UCORE_LEARDER, getCallOptions()), request);
        }

        /**
         * <pre>
         * GetUcoreStats get ucore cluster status
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUcoreStatsOutput> getUcoreStats(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_GET_UCORE_STATS, getCallOptions()), request);
        }

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> forceLeaveUcore(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.ForceLeaveUcoreInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_FORCE_LEAVE_UCORE, getCallOptions()), request);
        }

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeWithLimitOutput> getKvTreeWithLimit(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeWithLimitInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_GET_KV_TREE_WITH_LIMIT, getCallOptions()), request);
        }

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvsTreeOutput> getKvsTree(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvsTreeInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_GET_KVS_TREE, getCallOptions()), request);
        }

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.LockOnSessionOutput> lockOnSession(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.LockOnSessionInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_LOCK_ON_SESSION, getCallOptions()), request);
        }

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> renewSession(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.RenewSessionInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_RENEW_SESSION, getCallOptions()), request);
        }

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty> unlockOnSession(
                com.oceanbase.obsharding_d.alarm.UcoreInterface.UnlockOnSessionInput request) {
            return futureUnaryCall(
                    getChannel().newCall(METHOD_UNLOCK_ON_SESSION, getCallOptions()), request);
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
    private static final int METHODID_GET_UNRESOLVED_ALERTS_NUM = 21;
    private static final int METHODID_SEND_ALERT_TEST_MESSAGE = 22;
    private static final int METHODID_GET_USAGE = 23;
    private static final int METHODID_REQUEST_SERVER_CERTIFICATE = 24;
    private static final int METHODID_GET_CA_CERTIFICATE = 25;
    private static final int METHODID_IS_UCORE_INIT = 26;
    private static final int METHODID_COLLECT_CLUSTER_INFOS = 27;
    private static final int METHODID_UCORE_HEALTH_CHECK = 28;
    private static final int METHODID_LIST_KV_NEXT_LEVEL_RAW = 29;
    private static final int METHODID_LIST_KV_BY_PATH_RAW = 30;
    private static final int METHODID_REPORT_UGUARD_EXERCISE_RESULT = 31;
    private static final int METHODID_UCORE_LEARDER = 32;
    private static final int METHODID_GET_UCORE_STATS = 33;
    private static final int METHODID_FORCE_LEAVE_UCORE = 34;
    private static final int METHODID_GET_KV_TREE_WITH_LIMIT = 35;
    private static final int METHODID_GET_KVS_TREE = 36;
    private static final int METHODID_LOCK_ON_SESSION = 37;
    private static final int METHODID_RENEW_SESSION = 38;
    private static final int METHODID_UNLOCK_ON_SESSION = 39;

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
                    serviceImpl.putKv((com.oceanbase.obsharding_d.alarm.UcoreInterface.PutKvInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_GET_KV:
                    serviceImpl.getKv((com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvOutput>) responseObserver);
                    break;
                case METHODID_DELETE_KV:
                    serviceImpl.deleteKv((com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_DELETE_KVS:
                    serviceImpl.deleteKvs((com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvsInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_DELETE_KV_TREE:
                    serviceImpl.deleteKvTree((com.oceanbase.obsharding_d.alarm.UcoreInterface.DeleteKvTreeInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_SUBSCRIBE_KV:
                    serviceImpl.subscribeKv((com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvOutput>) responseObserver);
                    break;
                case METHODID_SUBSCRIBE_KV_PREFIX:
                    serviceImpl.subscribeKvPrefix((com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixOutput>) responseObserver);
                    break;
                case METHODID_SUBSCRIBE_KV_PREFIXS:
                    serviceImpl.subscribeKvPrefixs((com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixsInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeKvPrefixsOutput>) responseObserver);
                    break;
                case METHODID_SUBSCRIBE_NODES:
                    serviceImpl.subscribeNodes((com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeNodesInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.SubscribeNodesOutput>) responseObserver);
                    break;
                case METHODID_GET_KV_TREE:
                    serviceImpl.getKvTree((com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeOutput>) responseObserver);
                    break;
                case METHODID_GET_KV_TREE_RAW:
                    serviceImpl.getKvTreeRaw((com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeOutput>) responseObserver);
                    break;
                case METHODID_LIST_KV_KEYS:
                    serviceImpl.listKvKeys((com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvKeysInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvKeysOutput>) responseObserver);
                    break;
                case METHODID_PUT_KVS:
                    serviceImpl.putKvs((com.oceanbase.obsharding_d.alarm.UcoreInterface.PutKvsInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_SERVICE_HEARTBEAT:
                    serviceImpl.serviceHeartbeat((com.oceanbase.obsharding_d.alarm.UcoreInterface.ServiceHeartbeatInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_REGISTER_SERVICE:
                    serviceImpl.registerService((com.oceanbase.obsharding_d.alarm.UcoreInterface.RegisterServiceInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_UPDATE_CONFIG_START:
                    serviceImpl.updateConfigStart((com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigStartInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigStartOutput>) responseObserver);
                    break;
                case METHODID_UPDATE_CONFIG_COMMIT:
                    serviceImpl.updateConfigCommit((com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigCommitInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_UPDATE_CONFIG_ROLLBACK:
                    serviceImpl.updateConfigRollback((com.oceanbase.obsharding_d.alarm.UcoreInterface.UpdateConfigRollbackInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_ALERT:
                    serviceImpl.alert((com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_ALERT_RESOLVE:
                    serviceImpl.alertResolve((com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_ALERT_RESOLVE_BY_FINGERPRINT:
                    serviceImpl.alertResolveByFingerprint((com.oceanbase.obsharding_d.alarm.UcoreInterface.AlertResolveByFingerprintInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_GET_UNRESOLVED_ALERTS_NUM:
                    serviceImpl.getUnresolvedAlertsNum((com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUnresolvedAlertsNumInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUnresolvedAlertsNumOutput>) responseObserver);
                    break;
                case METHODID_SEND_ALERT_TEST_MESSAGE:
                    serviceImpl.sendAlertTestMessage((com.oceanbase.obsharding_d.alarm.UcoreInterface.SendAlertTestMessageInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_GET_USAGE:
                    serviceImpl.getUsage((com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUsageOutput>) responseObserver);
                    break;
                case METHODID_REQUEST_SERVER_CERTIFICATE:
                    serviceImpl.requestServerCertificate((com.oceanbase.obsharding_d.alarm.UcoreInterface.RequestServerCertificateInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.RequestServerCertificateOutput>) responseObserver);
                    break;
                case METHODID_GET_CA_CERTIFICATE:
                    serviceImpl.getCaCertificate((com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetCaCertificateOutput>) responseObserver);
                    break;
                case METHODID_IS_UCORE_INIT:
                    serviceImpl.isUcoreInit((com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_COLLECT_CLUSTER_INFOS:
                    serviceImpl.collectClusterInfos((com.oceanbase.obsharding_d.alarm.UcoreInterface.CollectClusterInfosInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.CollectClusterInfosOutput>) responseObserver);
                    break;
                case METHODID_UCORE_HEALTH_CHECK:
                    serviceImpl.ucoreHealthCheck((com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_LIST_KV_NEXT_LEVEL_RAW:
                    serviceImpl.listKvNextLevelRaw((com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvNextLevelRawInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvNextLevelRawOutput>) responseObserver);
                    break;
                case METHODID_LIST_KV_BY_PATH_RAW:
                    serviceImpl.listKvByPathRaw((com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvByPathRawInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.ListKvByPathRawOutput>) responseObserver);
                    break;
                case METHODID_REPORT_UGUARD_EXERCISE_RESULT:
                    serviceImpl.reportUguardExerciseResult((com.oceanbase.obsharding_d.alarm.UcoreInterface.ReportUguardExerciseResultInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_UCORE_LEARDER:
                    serviceImpl.ucoreLearder((com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.UcoreLearderOutput>) responseObserver);
                    break;
                case METHODID_GET_UCORE_STATS:
                    serviceImpl.getUcoreStats((com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetUcoreStatsOutput>) responseObserver);
                    break;
                case METHODID_FORCE_LEAVE_UCORE:
                    serviceImpl.forceLeaveUcore((com.oceanbase.obsharding_d.alarm.UcoreInterface.ForceLeaveUcoreInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_GET_KV_TREE_WITH_LIMIT:
                    serviceImpl.getKvTreeWithLimit((com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeWithLimitInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvTreeWithLimitOutput>) responseObserver);
                    break;
                case METHODID_GET_KVS_TREE:
                    serviceImpl.getKvsTree((com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvsTreeInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.GetKvsTreeOutput>) responseObserver);
                    break;
                case METHODID_LOCK_ON_SESSION:
                    serviceImpl.lockOnSession((com.oceanbase.obsharding_d.alarm.UcoreInterface.LockOnSessionInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.LockOnSessionOutput>) responseObserver);
                    break;
                case METHODID_RENEW_SESSION:
                    serviceImpl.renewSession((com.oceanbase.obsharding_d.alarm.UcoreInterface.RenewSessionInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>) responseObserver);
                    break;
                case METHODID_UNLOCK_ON_SESSION:
                    serviceImpl.unlockOnSession((com.oceanbase.obsharding_d.alarm.UcoreInterface.UnlockOnSessionInput) request,
                            (io.grpc.stub.StreamObserver<com.oceanbase.obsharding_d.alarm.UcoreInterface.Empty>) responseObserver);
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
            return com.oceanbase.obsharding_d.alarm.UcoreInterface.getDescriptor();
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
                            .addMethod(METHOD_GET_UNRESOLVED_ALERTS_NUM)
                            .addMethod(METHOD_SEND_ALERT_TEST_MESSAGE)
                            .addMethod(METHOD_GET_USAGE)
                            .addMethod(METHOD_REQUEST_SERVER_CERTIFICATE)
                            .addMethod(METHOD_GET_CA_CERTIFICATE)
                            .addMethod(METHOD_IS_UCORE_INIT)
                            .addMethod(METHOD_COLLECT_CLUSTER_INFOS)
                            .addMethod(METHOD_UCORE_HEALTH_CHECK)
                            .addMethod(METHOD_LIST_KV_NEXT_LEVEL_RAW)
                            .addMethod(METHOD_LIST_KV_BY_PATH_RAW)
                            .addMethod(METHOD_REPORT_UGUARD_EXERCISE_RESULT)
                            .addMethod(METHOD_UCORE_LEARDER)
                            .addMethod(METHOD_GET_UCORE_STATS)
                            .addMethod(METHOD_FORCE_LEAVE_UCORE)
                            .addMethod(METHOD_GET_KV_TREE_WITH_LIMIT)
                            .addMethod(METHOD_GET_KVS_TREE)
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
