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
#include "qgrpcsubscriptionbidirect.h"
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

void QGrpcChannelSubscription::finishRead(bool ok)
{
    qProtoDebug()<<"Finish QGrpcChannelSubscription recived status, ok:"<<ok
           <<"code:"<<m_grpc_status.error_code()
           <<"message:"<<QString::fromStdString(m_grpc_status.error_message())
           <<"detail:"<<QString::fromStdString(m_grpc_status.error_details());
    //If was parsing error, then grpc_status.ok()==true and status.code()!=OK, we should not fill
    //becouse this is error on top level
    if((m_status.code()==QGrpcStatus::Ok)||(!m_grpc_status.ok())){
        this->m_status=QGrpcStatus((QGrpcStatus::StatusCode) m_grpc_status.error_code(),
                             QString::fromStdString(m_grpc_status.error_message()));
    }
    emit finished();
}

void QGrpcChannelSubscription::newData(bool ok)
{
    if(!ok){
        //this->status=QGrpcStatus(QGrpcStatus::Aborted,"Some error happens");
        //emit finished();
        qProtoDebug()<<"QGrpcChannelSubscription error tag recived!";
        return;
    }
    if(m_readerState==FIRST_CALL){
        m_reader->Read(&response,&m_newData);
        m_readerState=PROCESSING;
        return;
    }
    if(m_readerState==PROCESSING){
        QByteArray data;
        m_grpc_status = parseByteBuffer(response, data);
        if(!m_grpc_status.ok()){
            this->m_status=QGrpcStatus((QGrpcStatus::StatusCode) m_grpc_status.error_code(),
                                     QString::fromStdString(m_grpc_status.error_message()));
            m_readerState=ENDED;
            //emit finished();
            cancel();
            return;
        }
        emit dataReady(data);
        m_reader->Read(&response,&m_newData);
    }
}

void QGrpcChannelSubscription::startReader()
{
    this->m_status=QGrpcStatus((QGrpcStatus::StatusCode) QGrpcStatus::Ok,"");
    grpc::ByteBuffer request;
    parseQByteArray(m_argument, request);
    m_reader= grpc::internal::ClientAsyncReaderFactory<grpc::ByteBuffer>::Create(m_channel,m_queue,
              grpc::internal::RpcMethod (m_method.toLatin1().data(),::grpc::internal::RpcMethod::SERVER_STREAMING),
                                                                               &m_context, request, true, &m_newData);
    m_reader->Finish(&m_grpc_status,&m_finishRead);
}

QGrpcChannelSubscription::QGrpcChannelSubscription(grpc::Channel *channel, grpc::CompletionQueue* queue,
                                                   const QString &method, const QByteArray &argument, QObject *parent) :
    QGrpcChannelBaseCall(channel, queue, method, parent),m_argument(argument)
{
    using std::placeholders::_1;
    m_finishRead={this,std::bind(&QGrpcChannelSubscription::finishRead,this, _1)};
    m_newData={this,std::bind(&QGrpcChannelSubscription::newData,this, _1)};
}

QGrpcChannelSubscription::~QGrpcChannelSubscription()
{
    cancel();
    if (m_reader != nullptr) {
        delete m_reader;
    }
}

void QGrpcChannelSubscription::cancel()
{
    // TODO: check thread safety
    qProtoDebug() << "Subscription canceled";
    m_context.TryCancel();
}

void QGrpcChannelCall::finishRead(bool ok){
    qProtoDebug()<<"Finish QGrpcChannelCall recived status, ok:"<<ok
           <<"code:"<<m_grpc_status.error_code()
           <<"message:"<<QString::fromStdString(m_grpc_status.error_message())
           <<"detail:"<<QString::fromStdString(m_grpc_status.error_details());

    //If was parsing error, then grpc_status.ok()==true and status.code()!=OK, we should not fill
    //becouse this is error on top level
    if((m_status.code()==QGrpcStatus::Ok)||(!m_grpc_status.ok())){
        this->m_status=QGrpcStatus((QGrpcStatus::StatusCode) m_grpc_status.error_code(),
                             QString::fromStdString(m_grpc_status.error_message()));
    }
    emit finished();
}

void QGrpcChannelCall::newData(bool ok)
{
    if(!ok){
        //this->status=QGrpcStatus(QGrpcStatus::Aborted,"Some error happens");
        //emit finished();
        qProtoDebug()<<"QGrpcChannelCall error tag recived!";
        return;
    }
    if(m_readerState==FIRST_CALL){
        //read next value
        m_reader->Read(&response,&m_newData);
        m_readerState=PROCESSING;
        return;
    }
    if(m_readerState==PROCESSING){
        m_grpc_status=parseByteBuffer(response,responseParsed);
        this->m_status=QGrpcStatus((QGrpcStatus::StatusCode) m_grpc_status.error_code(),
                                 QString::fromStdString(m_grpc_status.error_message()));
        m_readerState=ENDED;
        //cancel();
        //emit finished();
    }
}

void QGrpcChannelCall::startReader()
{
    this->m_status=QGrpcStatus((QGrpcStatus::StatusCode) QGrpcStatus::Ok,"");
    grpc::ByteBuffer request;
    parseQByteArray(m_argument, request);
    grpc::internal::RpcMethod method_(m_method.toLatin1().data(),::grpc::internal::RpcMethod::NORMAL_RPC);
    m_reader= grpc::internal::ClientAsyncReaderFactory<grpc::ByteBuffer>::Create(m_channel,m_queue,
                                                                                 method_,&m_context, request, true, &m_newData);
    m_reader->Finish(&m_grpc_status,&m_finishRead);
}

QGrpcChannelCall::QGrpcChannelCall(grpc::Channel *channel, grpc::CompletionQueue* queue, const QString &method,
                                   const QByteArray &argument, QObject *parent):
                                   QGrpcChannelBaseCall(channel, queue, method, parent),m_argument(argument)
{
    using std::placeholders::_1;
    m_finishRead={this,std::bind(&QGrpcChannelCall::finishRead,this, _1)};
    m_newData={this,std::bind(&QGrpcChannelCall::newData,this, _1)};
}

QGrpcChannelCall::~QGrpcChannelCall()
{
    cancel();
    //TODO wait while Finished will called?
    if(m_reader != nullptr){
        delete m_reader;
    }
}

void QGrpcChannelCall::cancel()
{
    // TODO: check thread safety
    qProtoDebug() << "Call canceled";
    m_context.TryCancel();
}

QGrpcChannelPrivate::QGrpcChannelPrivate(const QUrl &url, std::shared_ptr<grpc::ChannelCredentials> credentials):QObject(nullptr)
{
    m_channel = grpc::CreateChannel(url.toString().toStdString(), credentials);
    m_workThread = QThread::create([this](){
        void* tag=nullptr;
        bool ok=true;
        while (m_queue.Next(&tag,&ok)) {
            if(tag==nullptr){
                qProtoDebug()<<"GrpcChannel recive null tag!";
                continue;
            }
            FunctionCall* f=reinterpret_cast<FunctionCall*>(tag);
            f->callMethod(ok);
        }
        qProtoDebug()<<"Exit form worker thread";
    });
    m_workThread->setObjectName("QGrpcChannel_worker");
    //Channel finished;
    connect(m_workThread, &QThread::finished, this, &QGrpcChannelPrivate::finished);
    m_workThread->start();
}

QGrpcChannelPrivate::~QGrpcChannelPrivate()
{
    m_queue.Shutdown();
    m_workThread->wait();
    m_workThread->deleteLater();
}

void QGrpcChannelPrivate::call(const QString &method, const QString &service, const QByteArray &args, QGrpcAsyncReply *reply)
{
    QString rpcName = QString("/%1/%2").arg(service,method);

    std::shared_ptr<QGrpcChannelCall> call;
    std::shared_ptr<QMetaObject::Connection> connection(new QMetaObject::Connection);
    std::shared_ptr<QMetaObject::Connection> abortConnection(new QMetaObject::Connection);
    std::shared_ptr<QMetaObject::Connection> clientConnection(new QMetaObject::Connection);

    call.reset(
        new QGrpcChannelCall(m_channel.get(), &m_queue, rpcName, args, reply),
        [](QGrpcChannelCall * c) {
            c->deleteLater();
        }
    );

    *clientConnection = QObject::connect(this,&QGrpcChannelPrivate::finished,reply,
                                         [clientConnection, call, reply, connection, abortConnection](){
        qProtoDebug() << "Call gRPC chanel was destroyed";
        reply->setData({});
        emit reply->error(QGrpcStatus(QGrpcStatus::Aborted,"GRPC channel aborted"));
        emit reply->finished();

        QObject::disconnect(*connection);
        QObject::disconnect(*abortConnection);
        QObject::disconnect(*clientConnection);
    });

    *connection = QObject::connect(call.get(), &QGrpcChannelCall::finished, reply,
                                   [clientConnection, call, reply, connection, abortConnection](){
        if (call->m_status.code() == QGrpcStatus::Ok) {
            reply->setData(call->responseParsed);
            emit reply->finished();
        } else {
            reply->setData({});
            emit reply->error(call->m_status);
        }
        qProtoDebug() << "Call gRPC was Finished";
        QObject::disconnect(*connection);
        QObject::disconnect(*abortConnection);
        QObject::disconnect(*clientConnection);
    });

    *abortConnection = QObject::connect(reply, &QGrpcAsyncReply::error, call.get(),
                                        [clientConnection, call, connection, abortConnection](const QGrpcStatus &status){
        if (status.code() == QGrpcStatus::Aborted) {
            qProtoDebug() << "Call gRPC was Aborted";
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
    QGrpcChannelCall call(m_channel.get(), &m_queue, rpcName, args);

    //TODO if connection aborted, then data should not filled
    QObject::connect(this, &QGrpcChannelPrivate::finished,&loop,&QEventLoop::quit);
    QObject::connect(&call, &QGrpcChannelCall::finished, &loop, &QEventLoop::quit);

    call.startReader();

    loop.exec();

    //I think not good way, it is should not success if worker thread stopped
    if(m_workThread->isFinished()){
        call.m_status=QGrpcStatus(QGrpcStatus::Aborted,"Connection aborted");
    }

    ret = call.responseParsed;
    return call.m_status;
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
        new QGrpcChannelSubscription(m_channel.get(), &m_queue, rpcName, subscription->arg(), subscription),
        [](QGrpcChannelSubscription * sub) {
            sub->deleteLater();
        }
    );

    *readConnection = QObject::connect(sub.get(), &QGrpcChannelSubscription::dataReady, subscription,
                                       [subscription](const QByteArray &data) {subscription->handler(data);});

    *connection = QObject::connect(sub.get(), &QGrpcChannelSubscription::finished, subscription,
                                   [channelFinished,sub, subscription, readConnection, abortConnection, service, connection, clientConnection](){
        qProtoDebug() << "QGrpcSubscription ended with server closing connection";

        QObject::disconnect(*channelFinished);
        QObject::disconnect(*connection);
        QObject::disconnect(*readConnection);
        QObject::disconnect(*abortConnection);
        QObject::disconnect(*clientConnection);

        if (sub->m_status.code() != QGrpcStatus::Ok)
        {
            emit subscription->error(sub->m_status);
        }
    });

    *abortConnection = QObject::connect(subscription, &QGrpcSubscription::finished, sub.get(),
                                        [channelFinished, connection, abortConnection, readConnection, sub, clientConnection]() {
        qProtoDebug() << "Subscription client was finished";

        QObject::disconnect(*channelFinished);
        QObject::disconnect(*connection);
        QObject::disconnect(*readConnection);
        QObject::disconnect(*abortConnection);
        QObject::disconnect(*clientConnection);

        sub->cancel();
    });

    *clientConnection = QObject::connect(client, &QAbstractGrpcClient::destroyed, sub.get(),
                                         [channelFinished, readConnection, connection, abortConnection, sub, clientConnection](){
        qProtoDebug() << "QGrpcSubscription client was destroyed";

        QObject::disconnect(*channelFinished);
        QObject::disconnect(*connection);
        QObject::disconnect(*readConnection);
        QObject::disconnect(*abortConnection);
        QObject::disconnect(*clientConnection);

        sub->cancel();
    });

    *channelFinished = QObject::connect(this, &QGrpcChannelPrivate::finished, sub.get(),
                                        [channelFinished, readConnection, connection, abortConnection, sub, clientConnection](){
        qProtoDebug() << "QGrpcSubscription chanel was destroyed";

        QObject::disconnect(*channelFinished);
        QObject::disconnect(*connection);
        QObject::disconnect(*readConnection);
        QObject::disconnect(*abortConnection);
        QObject::disconnect(*clientConnection);

        sub->cancel();
    });

    sub->startReader();
}

QGrpcChannelSubscriptionBidirect::QGrpcChannelSubscriptionBidirect(grpc::Channel *channel, grpc::CompletionQueue* queue, const QString &method,
                                                                   QObject *parent):
    QGrpcChannelBaseCall(channel, queue, method, parent),m_inProcess(false){

    using std::placeholders::_1;
    m_finishRead={this,std::bind(&QGrpcChannelSubscriptionBidirect::finishRead,this, _1)};
    m_newData={this,std::bind(&QGrpcChannelSubscriptionBidirect::newData,this, _1)};
    m_finishWrite={this,std::bind(&QGrpcChannelSubscriptionBidirect::finishWrite,this, _1)};
}

QGrpcChannelSubscriptionBidirect::~QGrpcChannelSubscriptionBidirect(){
    cancel();
    if (m_reader != nullptr) {
        delete m_reader;
    }
};

void QGrpcChannelSubscriptionBidirect::newData(bool ok){
    if(!ok){
        //this->status=QGrpcStatus(QGrpcStatus::Aborted,"Some error happens");
        //emit finished();
        qProtoDebug()<<"QGrpcChannelSubscriptionBidirect error tag recived!";
        return;
    }
    if(m_readerState==FIRST_CALL){
        m_reader->Read(&response,&m_newData);
        m_readerState=PROCESSING;
        return;
    }
    if(m_readerState==PROCESSING){
        QByteArray data;
        m_grpc_status = parseByteBuffer(response, data);
        if(!m_grpc_status.ok()){
            this->m_status=QGrpcStatus((QGrpcStatus::StatusCode) m_grpc_status.error_code(),
                                     QString::fromStdString(m_grpc_status.error_message()));
            m_readerState=ENDED;
            //emit finished();
            cancel();
            return;
        }
        emit dataReady(data);
        m_reader->Read(&response,&m_newData);
    }
}

void QGrpcChannelSubscriptionBidirect::finishWrite(bool ok) {
    if(!ok){
        m_currentWriteReplay->setStatus(QGrpcWriteReplay::Failed);
        emit m_currentWriteReplay->error();
        emit m_currentWriteReplay->finished();
        //Если произошла ошибка исходящего потока, что делать? Будет ли вызва Finish? Будет ли получин статус?
        //this->status=QGrpcStatus(QGrpcStatus::Internal,"Some error happens at write data to client stream");
        //emit finished();
    }else{
        m_currentWriteReplay->setStatus(QGrpcWriteReplay::OK);
        emit m_currentWriteReplay->finished();
    }
    if(!m_sendQueue.isEmpty()){
        QGrpcChannelWiteData d=m_sendQueue.dequeue();
        if(d.done){
            m_reader->WritesDone(&m_finishWrite);
        }else{
            grpc::ByteBuffer data;
            parseQByteArray(d.data, data);
            m_currentWriteReplay=d.replay;
            m_reader->Write(data,&m_finishWrite);
        }
    }else{
        m_inProcess=false;
    }
}

void QGrpcChannelSubscriptionBidirect::finishRead(bool ok) {
    qProtoDebug()<<"Finish QGrpcChannelSubscriptionBidirect recived status, ok:"<<ok
           <<"code:"<<m_grpc_status.error_code()
           <<"message:"<<QString::fromStdString(m_grpc_status.error_message())
           <<"detail:"<<QString::fromStdString(m_grpc_status.error_details());

    //If was parsing error, then grpc_status.ok()==true and status.code()!=OK, we should not fill
    //becouse this is error on top level
    if((m_status.code()==QGrpcStatus::Ok)||(!m_grpc_status.ok())){
        this->m_status=QGrpcStatus((QGrpcStatus::StatusCode) m_grpc_status.error_code(),
                             QString::fromStdString(m_grpc_status.error_message()));
    }    
    //Finish all write replay
    if (m_inProcess){
        m_currentWriteReplay->setStatus(QGrpcWriteReplay::Failed);
        emit m_currentWriteReplay->finished();
        emit m_currentWriteReplay->error();
    }
    while(!m_sendQueue.empty()){
        QGrpcChannelWiteData d=m_sendQueue.dequeue();
        d.replay->setStatus(QGrpcWriteReplay::Failed);
        emit d.replay->finished();
        emit d.replay->error();
    }
    emit finished();
}

void QGrpcChannelSubscriptionBidirect::startReader(){
    this->m_status=QGrpcStatus((QGrpcStatus::StatusCode) QGrpcStatus::Ok,"");
    grpc::ByteBuffer request;
    m_reader= grpc::internal::ClientAsyncReaderWriterFactory<grpc::ByteBuffer,grpc::ByteBuffer>::Create(m_channel, m_queue,
              grpc::internal::RpcMethod(m_method.toLatin1().data(),::grpc::internal::RpcMethod::BIDI_STREAMING), &m_context, true, &m_newData);
    m_reader->Finish(&m_grpc_status,&m_finishRead);
}


void QGrpcChannelSubscriptionBidirect::writeDone(const QGrpcWriteReplayShared& replay){
    if(m_inProcess){
        m_sendQueue.enqueue({QByteArray(),replay,true});
        return;
    }
    m_currentWriteReplay=replay;
    m_reader->WritesDone(&m_finishWrite);
}

void QGrpcChannelSubscriptionBidirect::appendToSend(const QByteArray& data, const QGrpcWriteReplayShared& replay){
    if(m_inProcess){
        m_sendQueue.enqueue({data,replay,false});
        return;
    }
    m_inProcess=true;
    grpc::ByteBuffer dataParsed;
    parseQByteArray(data,dataParsed);
    m_currentWriteReplay=replay;
    m_reader->Write(dataParsed,&m_finishWrite);
}

void QGrpcChannelSubscriptionBidirect::cancel(){
    m_context.TryCancel();
}

void QGrpcChannelPrivate::subscribe(QGrpcSubscriptionBidirect *subscription, const QString &service, QAbstractGrpcClient *client){
    assert(subscription != nullptr);

    QString rpcName = QString("/%1/%2").arg(service,subscription->method());

    std::shared_ptr<QGrpcChannelSubscriptionBidirect> sub;
    std::shared_ptr<QMetaObject::Connection> abortConnection(new QMetaObject::Connection);
    std::shared_ptr<QMetaObject::Connection> readConnection(new QMetaObject::Connection);
    std::shared_ptr<QMetaObject::Connection> writeConnection(new QMetaObject::Connection);
    std::shared_ptr<QMetaObject::Connection> writeDoneConnection(new QMetaObject::Connection);
    std::shared_ptr<QMetaObject::Connection> clientConnection(new QMetaObject::Connection);
    std::shared_ptr<QMetaObject::Connection> connection(new QMetaObject::Connection);    
    std::shared_ptr<QMetaObject::Connection> channelFinished(new QMetaObject::Connection);

    sub.reset(
        new QGrpcChannelSubscriptionBidirect(m_channel.get(), &m_queue, rpcName, subscription),
        [](QGrpcChannelSubscriptionBidirect * sub) {
            qProtoDebug()<<"subscription deleted";
            sub->deleteLater();
        }
    );

    *readConnection = QObject::connect(sub.get(), &QGrpcChannelSubscriptionBidirect::dataReady, subscription,
                                       [subscription](const QByteArray &data) {subscription->handler(data);});

    *writeConnection=QObject::connect(subscription, &QGrpcSubscriptionBidirect::writeReady, this,
                                      [subscription, sub]() {
        QGrpcWriteReplayShared& reply=subscription->getReplay();
        reply->setStatus(QGrpcWriteReplay::InProcess);
        sub->appendToSend(subscription->getWriteData(),reply);
    }, Qt::DirectConnection);

    *writeDoneConnection=QObject::connect(subscription, &QGrpcSubscriptionBidirect::writeDoneReady, this,
                                          [subscription, sub]() {
        QGrpcWriteReplayShared& reply=subscription->getReplay();
        reply->setStatus(QGrpcWriteReplay::InProcess);
        sub->writeDone(reply);
    }, Qt::DirectConnection);

    *connection = QObject::connect(sub.get(), &QGrpcChannelSubscriptionBidirect::finished, subscription,
                                   [channelFinished,sub, subscription, readConnection, writeConnection, writeDoneConnection, abortConnection, service, connection, clientConnection](){
        qProtoDebug() << "QGrpcSubscriptionBidirect ended with server closing connection";

        QObject::disconnect(*channelFinished);
        QObject::disconnect(*connection);
        QObject::disconnect(*readConnection);
        QObject::disconnect(*abortConnection);
        QObject::disconnect(*clientConnection);
        QObject::disconnect(*writeConnection);
        QObject::disconnect(*writeDoneConnection);

        if (sub->m_status.code() != QGrpcStatus::Ok)
        {
            emit subscription->error(sub->m_status);
        }
    });

    *abortConnection = QObject::connect(subscription, &QGrpcSubscriptionBidirect::finished, sub.get(),
                                        [channelFinished, connection, abortConnection, readConnection, writeConnection, writeDoneConnection, sub, clientConnection]() {
        qProtoDebug() << "QGrpcSubscriptionBidirect client was finished";

        QObject::disconnect(*channelFinished);
        QObject::disconnect(*connection);
        QObject::disconnect(*readConnection);
        QObject::disconnect(*abortConnection);
        QObject::disconnect(*clientConnection);
        QObject::disconnect(*writeConnection);
        QObject::disconnect(*writeDoneConnection);

        sub->cancel();
    });

    *clientConnection = QObject::connect(client, &QAbstractGrpcClient::destroyed, sub.get(),
                                         [channelFinished, readConnection, writeConnection, writeDoneConnection, connection, abortConnection, sub, clientConnection](){
        qProtoDebug() << "QGrpcSubscriptionBidirect client was destroyed";

        QObject::disconnect(*channelFinished);
        QObject::disconnect(*connection);
        QObject::disconnect(*readConnection);
        QObject::disconnect(*abortConnection);
        QObject::disconnect(*clientConnection);
        QObject::disconnect(*writeConnection);
        QObject::disconnect(*writeDoneConnection);

        sub->cancel();
    });

    *channelFinished = QObject::connect(this, &QGrpcChannelPrivate::finished, sub.get(),
                                        [channelFinished, readConnection, writeConnection, writeDoneConnection, connection, abortConnection, sub, clientConnection](){
        qProtoDebug() << "QGrpcSubscriptionBidirect chanel was destroyed";

        QObject::disconnect(*channelFinished);
        QObject::disconnect(*connection);
        QObject::disconnect(*readConnection);
        QObject::disconnect(*abortConnection);
        QObject::disconnect(*clientConnection);
        QObject::disconnect(*writeConnection);
        QObject::disconnect(*writeDoneConnection);

        sub->cancel();
    });

    sub->startReader();
}


QGrpcChannel::QGrpcChannel(const QUrl &url, std::shared_ptr<grpc::ChannelCredentials> credentials) : QAbstractGrpcChannel()
  , dPtr(std::make_unique<QGrpcChannelPrivate>(url, credentials))
{}

QGrpcChannel::~QGrpcChannel() {}

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

void QGrpcChannel::subscribe(QGrpcSubscriptionBidirect *subscription, const QString &service, QAbstractGrpcClient *client)
{
    dPtr->subscribe(subscription,service,client);
}

std::shared_ptr<QAbstractProtobufSerializer> QGrpcChannel::serializer() const
{
    //TODO: make selection based on credentials or channel settings
    return QProtobufSerializerRegistry::instance().getSerializer("protobuf");
}

}
