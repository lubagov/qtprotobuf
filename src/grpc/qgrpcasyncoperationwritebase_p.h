/*
 * MIT License
 *
 * Copyright (c) 2019 Alexey Edelev <semlanik@gmail.com>
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

#pragma once

#include <QObject>
#include <QMutex>
#include <QEventLoop>
#include <QMetaObject>
#include <QThread>

#include <functional>
#include <memory>

#include "qabstractgrpcchannel.h"
#include "qabstractgrpcclient.h"
#include "qgrpcasyncoperationbase_p.h"
#include "qgrpcwritereplay.h"

#include "qtgrpcglobal.h"


namespace QtProtobuf {
/*!
 * \ingroup QtGrpc
 * \private
 * \brief The QGrpcAsyncOperationWriteBase class implements subscription logic
 */
class Q_GRPC_EXPORT QGrpcAsyncOperationWriteBase : public QGrpcAsyncOperationBase
{
    Q_OBJECT
public:

    /*!
     * \brief Write message from value to channel.
     * \return QGrpcWriteReplayShared with the status of the operation, and signals recording status
     * \details This function writes data to the raw byte array from the value, and emmit the writeReady signal,
     *          in blocking mode, the QAbstractGrpcChannel implementation, must pick up the data by calling getWriteData()
     */
    template <typename T>
    QGrpcWriteReplayShared write(T value) {
        /*
         * Если вызвать метод из другого потока, отличного от вызова в котором был создан QAbstractGrpcClient,
         * то это приведет к ошибкам синхронизации.В очереди которая буферизирует данные внутри qgrpcchannel, т.к. данные
         * добавляются в нее из текущего потока через блокирующий слот, а удяются и отправляются на сервер
         * из потока в котором создан subscribe (т.к. он создается внутри потока в котором создан канал).
         * Кроме того, метод write самого gRPC будет вызываться из разных потоков, хотя официальная имплементация Reader'ов
         * считается потокобезопасной.
         * Таким образом защита реализована внутри абстрактного клиента:
         * /build_grpc/qtProtobuf_native/qtprotobuf1/qtprotobuf/src/grpc/qabstractgrpcclient.cpp:134
         *     if (thread() != QThread::currentThread()) {
         *             QMetaObject::invokeMethod(this, [&]()->QGrpcSubscriptionShared {
         *                                           qProtoDebug() << "Subscription: " << dPtr->service << method << " called from different thread";
         *                                           return subscribe(method, arg, handler);
         *                                       }, Qt::BlockingQueuedConnection, &subscription);
         *     }
         */

        ///m_channel->thread() ??
        if (thread() != QThread::currentThread()) {
            QGrpcWriteReplayShared writeShared;
            QMetaObject::invokeMethod(this, [&]()->QGrpcWriteReplayShared {
                                          qProtoDebug() << "Write: called from different thread";
                                          return write(value);
                                      }, Qt::BlockingQueuedConnection, &writeShared);
            return writeShared;
        }else{
            QMutexLocker locker(&m_asyncLock);
            try {
                m_writeReplay=QSharedPointer<QGrpcWriteReplay>(new QGrpcWriteReplay(),&QObject::deleteLater);
                m_writeData=value.serialize(static_cast<QAbstractGrpcClient*>(parent())->serializer());
                emit writeReady();
            } catch (std::invalid_argument &) {
                m_writeReplay->setStatus(QGrpcWriteReplay::Failed);
                static const QLatin1String invalidArgumentErrorMessage("Response deserialization failed invalid field found");
                error({QGrpcStatus::InvalidArgument, invalidArgumentErrorMessage});
            } catch (std::out_of_range &) {
                m_writeReplay->setStatus(QGrpcWriteReplay::Failed);
                static const QLatin1String outOfRangeErrorMessage("Invalid size of received buffer");
                error({QGrpcStatus::OutOfRange, outOfRangeErrorMessage});
            } catch (...) {
                m_writeReplay->setStatus(QGrpcWriteReplay::Failed);
                error({QGrpcStatus::Internal, QLatin1String("Unknown exception caught during deserialization")});
            }
            if((m_writeReplay->status()==QGrpcWriteReplay::Failed)||
               (m_writeReplay->status()==QGrpcWriteReplay::NotConnected)){
                emit m_writeReplay->error();
                emit m_writeReplay->finished();
            }
            return m_writeReplay;
        }
    }

    /*!
     * \brief Blocking write message from value to channel. Call write method and wait over QEventLoop when async operation finished
     * \return Returns the status of a write operation to a channel.
     */
    template <typename T>
    QGrpcWriteReplay::WriteStatus writeBlocked(T value) {
        QGrpcWriteReplayShared reply=write(value);
        QEventLoop loop;
        QObject::connect(reply.get(), &QGrpcWriteReplay::finished,&loop, &QEventLoop::quit, Qt::QueuedConnection);
        if(reply->status()==QGrpcWriteReplay::InProcess){
            loop.exec();
        }
        return reply->status();
    }

    /*!
     * \brief Sends a message about the completion of write to the channel. The function is called after the last write operation.
     * \return QGrpcWriteReplayShared with the status of the operation, and signals handle status.
     * \details This function emmit the writeDoneReady signal, the QAbstractGrpcChannel implementation, should process the signal
     *          blocking mode and complete the stream.
     */
    QGrpcWriteReplayShared writeDone() {
        if (thread() != QThread::currentThread()) {
            QGrpcWriteReplayShared writeShared;
            QMetaObject::invokeMethod(this, [&]()->QGrpcWriteReplayShared {
                                          qProtoDebug() << "Write: called from different thread";
                                          return writeDone();
                                      }, Qt::BlockingQueuedConnection, &writeShared);
            return writeShared;
        }else{
            QMutexLocker locker(&m_asyncLock);
            m_writeReplay=QSharedPointer<QGrpcWriteReplay>(new QGrpcWriteReplay(),&QObject::deleteLater);
            emit writeDoneReady();
            return m_writeReplay;
        }
    }

    /*!
     * \brief Blocking sends a message about the completion of write to the channel. Call writeDone method and wait over QEventLoop when
     *        async operation finished.
     * \return Returns the status of a operation to a channel.
     */
    QGrpcWriteReplay::WriteStatus writeDoneBlocked() {
        QGrpcWriteReplayShared reply=writeDone();
        QEventLoop loop;
        QObject::connect(reply.get(), &QGrpcWriteReplay::finished,&loop, &QEventLoop::quit, Qt::QueuedConnection);
        if(reply->status()==QGrpcWriteReplay::InProcess){
            loop.exec();
        }
        return reply->status();
    }

    //TODO Should be private
    /*!
     * \return Returns the returns the raw buffer to be written to the stream
     */
    const QByteArray& getWriteData() {
        return m_writeData;
    }

    //TODO Should be private
    /*!
     * \return Returns QGrpcWriteReplayShared which will be available for the next asynchronous write call
     */
    QGrpcWriteReplayShared& getReplay(){
        return m_writeReplay;
    }

signals:
    //Connect over blocking queue in QAbstractGrpcClient, to copy data in process thread
    void writeReady();
    void writeDoneReady();

protected:
    //! \private
    QGrpcAsyncOperationWriteBase(const std::shared_ptr<QAbstractGrpcChannel> &channel, QAbstractGrpcClient *parent) : QGrpcAsyncOperationBase(channel,parent)
    {}
    //! \private
    virtual ~QGrpcAsyncOperationWriteBase();
private:
    QGrpcAsyncOperationWriteBase();
    Q_DISABLE_COPY_MOVE(QGrpcAsyncOperationWriteBase)
    QByteArray m_writeData;
    QGrpcWriteReplayShared m_writeReplay;

    friend class QAbstractGrpcClient;
};

}
