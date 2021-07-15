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

#include <grpcpp/channel.h>
#include <grpcpp/impl/codegen/byte_buffer.h>
#include <grpcpp/impl/codegen/client_context.h>
#include <grpcpp/impl/codegen/async_stream.h>

#include <grpcpp/impl/codegen/completion_queue.h>
#include <grpcpp/security/credentials.h>

#include "qabstractgrpccredentials.h"
#include "qgrpcasyncreply.h"
#include "qgrpcsubscription.h"
#include "qabstractgrpcclient.h"
#include "qgrpccredentials.h"
#include "qprotobufserializerregistry_p.h"
#include "qtprotobuflogging.h"

namespace QtProtobuf {

class FinishStatus;

class QGrpcChannelBaseAbstract: public QObject{
    Q_OBJECT

public:
    virtual void newData(bool ok)=0;
    QGrpcChannelBaseAbstract(QObject* parent):QObject(parent){};
};

class QGrpcChannelBaseCall : public QGrpcChannelBaseAbstract{
    Q_OBJECT

public:
    typedef enum ReaderState {
        FIRST_CALL=0,
        PROCESSING,
        ENDED
    } ReaderState;

    QGrpcStatus status;
    virtual void finish_recived(bool ok)=0;
    QGrpcChannelBaseCall(grpc::Channel *channel, grpc::CompletionQueue* queue,
                        const QString &method, const QByteArray &argument, QObject* parent=nullptr):
                        QGrpcChannelBaseAbstract(parent),readerState(FIRST_CALL), reader(nullptr),channel(channel),
                        queue(queue),method(method),argument(argument){}

protected:
    FinishStatus* finish_status;
    ReaderState readerState;
    ::grpc::Status grpc_status;

    grpc::ClientContext context;
    grpc::ClientAsyncReader<grpc::ByteBuffer> *reader;
    grpc::ByteBuffer response;

    grpc::Channel *channel;
    grpc::CompletionQueue* queue;
    QString method;
    QByteArray argument;
};

class FinishStatus:public QGrpcChannelBaseAbstract{
    Q_OBJECT;

private:
    QGrpcChannelBaseCall* baseCall;

public:
    inline void newData(bool ok) override {
        baseCall->finish_recived(ok);
    };
    FinishStatus(QGrpcChannelBaseCall* baseCall, QObject* parent):QGrpcChannelBaseAbstract(parent),baseCall(baseCall)
    {
    }
};

//! \private
class QGrpcChannelSubscription : public QGrpcChannelBaseCall {
    //! \private
    Q_OBJECT;

public:
    QGrpcChannelSubscription(grpc::Channel *channel, grpc::CompletionQueue* queue, const QString &method,
                             const QByteArray &argument, QObject *parent = nullptr);
    ~QGrpcChannelSubscription();
    void startReader();
    void cancel();
    void newData(bool ok) override;
    void finish_recived(bool ok) override;

signals:
    void dataReady(const QByteArray &data);
    void finished();
};

//! \private
class QGrpcChannelCall : public QGrpcChannelBaseCall {
    //! \private
    Q_OBJECT;

public:
    QByteArray responseParsed;
    QGrpcChannelCall(grpc::Channel *channel, grpc::CompletionQueue* queue, const QString &method,
                     const QByteArray &data, QObject *parent = nullptr);
    ~QGrpcChannelCall();
    void startReader();
    void cancel();

    void newData(bool ok) override;
    void finish_recived(bool ok) override;

signals:
    void finished();
};

//! \private
class QGrpcChannelPrivate: public QObject {
    Q_OBJECT
    //! \private
private:
    QThread* thread;
    std::shared_ptr<grpc::Channel> m_channel;
    ::grpc::CompletionQueue queue;

public:
    QGrpcChannelPrivate(const QUrl &url, std::shared_ptr<grpc::ChannelCredentials> credentials);
    ~QGrpcChannelPrivate();

    void call(const QString &method, const QString &service, const QByteArray &args, QGrpcAsyncReply *reply);
    QGrpcStatus call(const QString &method, const QString &service, const QByteArray &args, QByteArray &ret);
    void subscribe(QGrpcSubscription *subscription, const QString &service, QAbstractGrpcClient *client);
signals:
    void finished();
};

};
