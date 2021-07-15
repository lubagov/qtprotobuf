/*
 * MIT License
 *
 * Copyright (c) 2019 Giulio Girardi <giulio.girardi@protechgroup.it>
 * Copyright (c) 2021 Nikolai Lubiagov <lubagov@gmail.com>
 *
 * This file is part of QtProtobuf project https://git.semlanik.org/semlanik/qtprotobuf
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and
 * to permit persons to whom the Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies
 * or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
 * PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
 * FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

#include "qgrpcchannel.h"
#include "qgrpcchannel_p.h"

#include <QEventLoop>
#include <QThread>

#include <memory>
#include <thread>
#include <unordered_map>

#include <grpcpp/channel.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/impl/codegen/byte_buffer.h>
#include <grpcpp/impl/codegen/client_unary_call.h>
#include <grpcpp/impl/codegen/rpc_method.h>
#include <grpcpp/impl/codegen/slice.h>
#include <grpcpp/impl/codegen/status.h>
#include <grpcpp/impl/codegen/sync_stream.h>
#include <grpcpp/security/credentials.h>

#include "qabstractgrpccredentials.h"
#include "qgrpcasyncreply.h"
#include "qgrpcstatus.h"
#include "qgrpcsubscription.h"
#include "qabstractgrpcclient.h"
#include "qgrpccredentials.h"
#include "qprotobufserializerregistry_p.h"
#include "qtprotobuflogging.h"

using namespace QtProtobuf;

namespace QtProtobuf {

static inline grpc::Status parseByteBuffer(const grpc::ByteBuffer &buffer, QByteArray &data)
{
    std::vector<grpc::Slice> slices;
    auto status = buffer.Dump(&slices);

    if (!status.ok())
        return status;

    for (const auto& slice : slices) {
        data.append(QByteArray((const char *)slice.begin(), slice.size()));
    }

    return grpc::Status::OK;
}

static inline void parseQByteArray(const QByteArray &bytearray, grpc::ByteBuffer &buffer)
{
    grpc::Slice slice(bytearray.data(), bytearray.size());
    grpc::ByteBuffer tmp(&slice, 1);
    buffer.Swap(&tmp);
}

void QGrpcChannelSubscription::finish_recived(bool ok)
{
    Q_UNUSED(ok)
    qProtoDebug()<<"Finish recived!";
    //If was parsing error, then grpc_status.ok()==true and status.code()!=OK, we should not fill
    //becouse this is error on top level
    if((status.code()==QGrpcStatus::Ok)||(!grpc_status.ok())){
        this->status=QGrpcStatus((QGrpcStatus::StatusCode) grpc_status.error_code(),
                             QString::fromStdString(grpc_status.error_message()));
    }
    emit finished();
}

void QGrpcChannelSubscription::newData(bool ok)
{
    if(!ok){
        //this->status=QGrpcStatus(QGrpcStatus::Aborted,"Some error happens");
        //emit finished();
        qProtoDebug()<<"Subscription error tag recived!";
        return;
    }
    if(readerState==FIRST_CALL){
        reader->Read(&response,this);
        readerState=PROCESSING;
        return;
    }
    if(readerState==PROCESSING){
        QByteArray data;
        grpc_status = parseByteBuffer(response, data);
        if(!grpc_status.ok()){
            this->status=QGrpcStatus((QGrpcStatus::StatusCode) grpc_status.error_code(),
                                     QString::fromStdString(grpc_status.error_message()));
            readerState=ENDED;
            //emit finished();
            cancel();
            return;
        }
        emit dataReady(data);
        reader->Read(&response,this);
    }
}

void QGrpcChannelSubscription::startReader()
{
    this->status=QGrpcStatus((QGrpcStatus::StatusCode) QGrpcStatus::Ok,"");
    grpc::ByteBuffer request;
    parseQByteArray(argument, request);
    reader= grpc::internal::ClientAsyncReaderFactory<grpc::ByteBuffer>::Create(channel,queue,
            grpc::internal::RpcMethod (method.toLatin1().data(),::grpc::internal::RpcMethod::SERVER_STREAMING),
                                                                               &context, request, true, this);
    reader->Finish(&grpc_status,finish_status);
}

QGrpcChannelSubscription::QGrpcChannelSubscription(grpc::Channel *channel, grpc::CompletionQueue* queue,
                                                   const QString &method, const QByteArray &argument, QObject *parent) :
    QGrpcChannelBaseCall(channel, queue, method, argument, parent)
{
    finish_status=new FinishStatus(this,this);
}

QGrpcChannelSubscription::~QGrpcChannelSubscription()
{
    cancel();
    if (reader != nullptr) {
        delete reader;
    }
}

void QGrpcChannelSubscription::cancel()
{
    // TODO: check thread safety
    qProtoDebug() << "Subscription canceled";
    context.TryCancel();
}

void QGrpcChannelCall::finish_recived(bool ok)
{
    Q_UNUSED(ok)
    qProtoDebug()<<"Finish recived!";
    //If was parsing error, then grpc_status.ok()==true and status.code()!=OK, we should not fill
    //becouse this is error on top level
    if((status.code()==QGrpcStatus::Ok)||(!grpc_status.ok())){
        this->status=QGrpcStatus((QGrpcStatus::StatusCode) grpc_status.error_code(),
                             QString::fromStdString(grpc_status.error_message()));
    }
    emit finished();
}

void QGrpcChannelCall::newData(bool ok)
{
    if(!ok){
        //this->status=QGrpcStatus(QGrpcStatus::Aborted,"Some error happens");
        //emit finished();
        qProtoDebug()<<"Call error tag recived!";
        return;
    }
    qProtoDebug()<<"reader state1: "<<readerState;
    if(readerState==FIRST_CALL){
        //read next value
        reader->Read(&response,this);
        readerState=PROCESSING;
        return;
    }

    if(readerState==PROCESSING){
        grpc_status=parseByteBuffer(response,responseParsed);
        this->status=QGrpcStatus((QGrpcStatus::StatusCode) grpc_status.error_code(),
                                 QString::fromStdString(grpc_status.error_message()));
        readerState=ENDED;
        //cancel();
        //emit finished();
    }
}

void QGrpcChannelCall::startReader()
{
    this->status=QGrpcStatus((QGrpcStatus::StatusCode) QGrpcStatus::Ok,"");
    grpc::ByteBuffer request;
    parseQByteArray(argument, request);
    grpc::internal::RpcMethod method_(method.toLatin1().data(),::grpc::internal::RpcMethod::NORMAL_RPC);
    reader= grpc::internal::ClientAsyncReaderFactory<grpc::ByteBuffer>::Create(channel,queue,method_,&context, request, true, this);
    reader->Finish(&grpc_status,finish_status);
}

QGrpcChannelCall::QGrpcChannelCall(grpc::Channel *channel, grpc::CompletionQueue* queue, const QString &method,
                                   const QByteArray &argument, QObject *parent):
                                   QGrpcChannelBaseCall(channel, queue, method, argument, parent)
{
    finish_status=new FinishStatus(this,this);
}

QGrpcChannelCall::~QGrpcChannelCall()
{
    cancel();
    //TODO wait while Finished will called?
    if(reader != nullptr){
        delete reader;
    }
}

void QGrpcChannelCall::cancel()
{
    // TODO: check thread safety
    qProtoDebug() << "Call canceled";
    context.TryCancel();
}

QGrpcChannelPrivate::QGrpcChannelPrivate(const QUrl &url, std::shared_ptr<grpc::ChannelCredentials> credentials):QObject(nullptr)
{
    m_channel = grpc::CreateChannel(url.toString().toStdString(), credentials);
    thread = QThread::create([this](){
        void* tag=nullptr;
        bool ok=true;
        while (queue.Next(&tag,&ok)) {
            if(tag==nullptr){
                continue;
            }
            QGrpcChannelBaseCall* call_base=reinterpret_cast<QGrpcChannelBaseCall*>(tag);
            call_base->newData(ok);
        }
    });
    thread->setObjectName("QGrpcChannel_worker");
    //Channel finished;
    connect(thread, &QThread::finished, this, &QGrpcChannelPrivate::finished);
    thread->start();
}

QGrpcChannelPrivate::~QGrpcChannelPrivate()
{
    queue.Shutdown();
    thread->wait();
    thread->deleteLater();
}

void QGrpcChannelPrivate::call(const QString &method, const QString &service, const QByteArray &args, QGrpcAsyncReply *reply)
{
    QString rpcName = QString("/%1/%2").arg(service,method);

    std::shared_ptr<QGrpcChannelCall> call;
    std::shared_ptr<QMetaObject::Connection> connection(new QMetaObject::Connection);
    std::shared_ptr<QMetaObject::Connection> abortConnection(new QMetaObject::Connection);
    std::shared_ptr<QMetaObject::Connection> clientConnection(new QMetaObject::Connection);

    call.reset(
        new QGrpcChannelCall(m_channel.get(), &queue, rpcName, args, reply),
        [](QGrpcChannelCall * c) {
            c->deleteLater();
        }
    );

    *clientConnection = QObject::connect(this,&QGrpcChannelPrivate::finished,reply, [clientConnection, call, reply, connection, abortConnection](){
        qProtoDebug() << "Grpc chanel was destroyed";
        reply->setData({});
        emit reply->error(QGrpcStatus(QGrpcStatus::Aborted,"GRPC channel aborted"));
        emit reply->finished();

        QObject::disconnect(*connection);
        QObject::disconnect(*abortConnection);
        QObject::disconnect(*clientConnection);
    });

    *connection = QObject::connect(call.get(), &QGrpcChannelCall::finished, reply,
                                   [clientConnection, call, reply, connection, abortConnection](){
        if (call->status.code() == QGrpcStatus::Ok) {
            reply->setData(call->responseParsed);
            emit reply->finished();
        } else {
            reply->setData({});
            emit reply->error(call->status);
        }

        QObject::disconnect(*connection);
        QObject::disconnect(*abortConnection);
        QObject::disconnect(*clientConnection);
    });

    *abortConnection = QObject::connect(reply, &QGrpcAsyncReply::error, call.get(),
                                        [clientConnection, call, connection, abortConnection](const QGrpcStatus &status){
        if (status.code() == QGrpcStatus::Aborted) {
            QObject::disconnect(*connection);
            QObject::disconnect(*abortConnection);
            QObject::disconnect(*clientConnection);
        }
    });

    call->startReader();
}

QGrpcStatus QGrpcChannelPrivate::call(const QString &method, const QString &service, const QByteArray &args, QByteArray &ret)
{
    QEventLoop loop;

    QString rpcName = QString("/%1/%2").arg(service,method);
    QGrpcChannelCall call(m_channel.get(), &queue, rpcName, args);

    //TODO if connection aborted, then data should not filled
    QObject::connect(this, &QGrpcChannelPrivate::finished,&loop,&QEventLoop::quit);
    QObject::connect(&call, &QGrpcChannelCall::finished, &loop, &QEventLoop::quit);

    call.startReader();

    loop.exec();

    //I think not good way, it is should not success if worker thread stopped
    if(thread->isFinished()){
        call.status=QGrpcStatus(QGrpcStatus::Aborted,"Connection aborted");
    }

    ret = call.responseParsed;
    return call.status;
}

void QGrpcChannelPrivate::subscribe(QGrpcSubscription *subscription, const QString &service, QAbstractGrpcClient *client)
{
    assert(subscription != nullptr);

    QString rpcName = QString("/%1/%2").arg(service,subscription->method());

    std::shared_ptr<QGrpcChannelSubscription> sub;
    std::shared_ptr<QMetaObject::Connection> abortConnection(new QMetaObject::Connection);
    std::shared_ptr<QMetaObject::Connection> readConnection(new QMetaObject::Connection);
    std::shared_ptr<QMetaObject::Connection> clientConnection(new QMetaObject::Connection);
    std::shared_ptr<QMetaObject::Connection> connection(new QMetaObject::Connection);
    std::shared_ptr<QMetaObject::Connection> channelFinished(new QMetaObject::Connection);

    sub.reset(
        new QGrpcChannelSubscription(m_channel.get(), &queue, rpcName, subscription->arg(), subscription),
        [](QGrpcChannelSubscription * sub) {
            sub->deleteLater();
        }
    );

    *readConnection = QObject::connect(sub.get(), &QGrpcChannelSubscription::dataReady, subscription, [subscription](const QByteArray &data) {
        subscription->handler(data);
    });

    *connection = QObject::connect(sub.get(), &QGrpcChannelSubscription::finished, subscription, [channelFinished,sub, subscription, readConnection, abortConnection, service, connection, clientConnection](){
        qProtoDebug() << "Subscription ended with server closing connection";

        QObject::disconnect(*channelFinished);
        QObject::disconnect(*connection);
        QObject::disconnect(*readConnection);
        QObject::disconnect(*abortConnection);
        QObject::disconnect(*clientConnection);

        if (sub->status.code() != QGrpcStatus::Ok)
        {
            emit subscription->error(sub->status);
        }
    });

    *abortConnection = QObject::connect(subscription, &QGrpcSubscription::finished, sub.get(), [channelFinished, connection, abortConnection, readConnection, sub, clientConnection]() {
        qProtoDebug() << "Subscription client was finished";

        QObject::disconnect(*channelFinished);
        QObject::disconnect(*connection);
        QObject::disconnect(*readConnection);
        QObject::disconnect(*abortConnection);
        QObject::disconnect(*clientConnection);

        sub->cancel();
    });

    *clientConnection = QObject::connect(client, &QAbstractGrpcClient::destroyed, sub.get(), [channelFinished, readConnection, connection, abortConnection, sub, clientConnection](){
        qProtoDebug() << "Grpc client was destroyed";

        QObject::disconnect(*channelFinished);
        QObject::disconnect(*connection);
        QObject::disconnect(*readConnection);
        QObject::disconnect(*abortConnection);
        QObject::disconnect(*clientConnection);

        sub->cancel();
    });

    *channelFinished = QObject::connect(this, &QGrpcChannelPrivate::finished, sub.get(), [channelFinished, readConnection, connection, abortConnection, sub, clientConnection](){
        qProtoDebug() << "Grpc chanel was destroyed";

        QObject::disconnect(*channelFinished);
        QObject::disconnect(*connection);
        QObject::disconnect(*readConnection);
        QObject::disconnect(*abortConnection);
        QObject::disconnect(*clientConnection);

        sub->cancel();
    });

    sub->startReader();
}

QGrpcChannel::QGrpcChannel(const QUrl &url, std::shared_ptr<grpc::ChannelCredentials> credentials) : QAbstractGrpcChannel()
  , dPtr(std::make_unique<QGrpcChannelPrivate>(url, credentials))
{
}

QGrpcChannel::~QGrpcChannel()
{
}

QGrpcStatus QGrpcChannel::call(const QString &method, const QString &service, const QByteArray &args, QByteArray &ret)
{
    return dPtr->call(method, service, args, ret);
}

void QGrpcChannel::call(const QString &method, const QString &service, const QByteArray &args, QGrpcAsyncReply *reply)
{
    dPtr->call(method, service, args, reply);
}

void QGrpcChannel::subscribe(QGrpcSubscription *subscription, const QString &service, QAbstractGrpcClient *client)
{
    dPtr->subscribe(subscription, service, client);
}

std::shared_ptr<QAbstractProtobufSerializer> QGrpcChannel::serializer() const
{
    //TODO: make selection based on credentials or channel settings
    return QProtobufSerializerRegistry::instance().getSerializer("protobuf");
}

}
