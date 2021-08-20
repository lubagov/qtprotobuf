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

#include <QEventLoop>
#include <QThread>

#include <QQueue>
#include <grpcpp/channel.h>
#include <grpcpp/impl/codegen/byte_buffer.h>
#include <grpcpp/impl/codegen/client_context.h>
#include <grpcpp/impl/codegen/async_stream.h>

#include <grpcpp/impl/codegen/completion_queue.h>
#include <grpcpp/security/credentials.h>

#include "qabstractgrpccredentials.h"
#include "qgrpcasyncreply.h"
#include "qgrpcsubscription.h"
#include "qgrpcsubscriptionbidirect.h"
#include "qabstractgrpcclient.h"
#include "qgrpccredentials.h"
#include "qprotobufserializerregistry_p.h"
#include "qtprotobuflogging.h"

namespace QtProtobuf {

struct FunctionCall{
    QObject* m_parent;
    std::function<void(bool)> m_method;

    void callMethod(bool arg){
        QMetaObject::invokeMethod(m_parent, [this, arg](){m_method(arg);}, Qt::QueuedConnection);
    }
};

//! \private
class QGrpcChannelBaseCall : public QObject{
    Q_OBJECT
public:
    //! \private
    typedef enum ReaderState {
        FIRST_CALL=0,
        PROCESSING,
        ENDED
    } ReaderState;

    QGrpcStatus m_status;
    QGrpcChannelBaseCall(grpc::Channel *channel, grpc::CompletionQueue* queue,
                        const QString &method, QObject* parent=nullptr):
                        QObject(parent),m_readerState(FIRST_CALL),m_channel(channel),
                        m_queue(queue),m_method(method){}

protected:
    ReaderState m_readerState;
    ::grpc::Status m_grpc_status;

    grpc::ClientContext m_context;
    grpc::ByteBuffer response;

    grpc::Channel *m_channel;
    grpc::CompletionQueue* m_queue;
    QString m_method;
};

//! \private
class QGrpcChannelSubscription : public QGrpcChannelBaseCall {
    //! \private
    Q_OBJECT;
private:
    QByteArray m_argument;
    grpc::ClientAsyncReader<grpc::ByteBuffer> *m_reader;
    FunctionCall m_finishRead;
    FunctionCall m_newData;

public:
    QGrpcChannelSubscription(grpc::Channel *channel, grpc::CompletionQueue* queue, const QString &method,
                             const QByteArray &argument, QObject *parent = nullptr);
    ~QGrpcChannelSubscription();
    void startReader();
    void cancel();
    void newData(bool ok);
    void finishRead(bool ok);

signals:
    void dataReady(const QByteArray &data);
    void finished();
};

struct QGrpcChannelWiteData{
    QByteArray data;
    QGrpcWriteReplayShared replay;
    bool done;
};

class QGrpcChannelSubscriptionBidirect : public QGrpcChannelBaseCall {
    //! \private
    Q_OBJECT
private:
    grpc::ClientAsyncReaderWriter<grpc::ByteBuffer,grpc::ByteBuffer> *m_reader;
    QQueue<QGrpcChannelWiteData> m_sendQueue;
    bool m_inProcess;
    QGrpcWriteReplayShared m_currentWriteReplay;

    FunctionCall m_finishWrite;
    FunctionCall m_finishRead;
    FunctionCall m_newData;

public:
    QGrpcChannelSubscriptionBidirect(grpc::Channel *channel, grpc::CompletionQueue* queue, const QString &method,
                                     QObject *parent = nullptr);
    ~QGrpcChannelSubscriptionBidirect();
    void startReader();
    void cancel();
    void writeDone(const QGrpcWriteReplayShared& replay);
    void appendToSend(const QByteArray& data, const QGrpcWriteReplayShared& replay);
    void newData(bool ok);
    void finishRead(bool ok);
    void finishWrite(bool ok);

signals:
    void dataReady(const QByteArray &data);
    void finished();
};

//! \private
class QGrpcChannelCall : public QGrpcChannelBaseCall {
    //! \private
    Q_OBJECT
private:
    QByteArray m_argument;
    grpc::ClientAsyncReader<grpc::ByteBuffer> *m_reader;
    FunctionCall m_newData;
    FunctionCall m_finishRead;

public:
    QByteArray responseParsed;
    QGrpcChannelCall(grpc::Channel *channel, grpc::CompletionQueue* queue, const QString &method,
                     const QByteArray &data, QObject *parent = nullptr);
    ~QGrpcChannelCall();
    void startReader();
    void cancel();
    void newData(bool ok);
    void finishRead(bool ok);

signals:
    void finished();
};

//Баг в версии Qt 5.15.2 для Android в результате которого cxx11_future всегда False.
//Поэтому я не могу использовать QThread::create(), и как временное решение создаю отдельный объект
#if !QT_CONFIG(cxx11_future)
class QGrpcWorkThread:public QObject
{
    Q_OBJECT
private:
    ::grpc::CompletionQueue& m_queue;
public:
    QGrpcWorkThread(::grpc::CompletionQueue& queue):QObject(nullptr), m_queue(queue){}
public slots:
    void run();
};
#endif

//! \private
class QGrpcChannelPrivate: public QObject {
    Q_OBJECT
    //! \private
private:
    QThread* m_workThread;
#if !QT_CONFIG(cxx11_future)
    QGrpcWorkThread* m_workThread_object;
#endif
    std::shared_ptr<grpc::Channel> m_channel;
    ::grpc::CompletionQueue m_queue;
public:
    QGrpcChannelPrivate(const QUrl &url, std::shared_ptr<grpc::ChannelCredentials> credentials);
    ~QGrpcChannelPrivate();

    void call(const QString &method, const QString &service, const QByteArray &args, QGrpcAsyncReply *reply);
    QGrpcStatus call(const QString &method, const QString &service, const QByteArray &args, QByteArray &ret);
    void subscribe(QGrpcSubscription *subscription, const QString &service, QAbstractGrpcClient *client);
    void subscribe(QGrpcSubscriptionBidirect *subscription, const QString &service, QAbstractGrpcClient *client);
signals:
    void finished();
};

};
