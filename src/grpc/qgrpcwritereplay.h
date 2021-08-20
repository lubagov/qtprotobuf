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

#pragma once //QGrpcWriteReplay

#include <QObject>
#include "qtgrpcglobal.h"

namespace QtProtobuf {

class QGrpcAsyncOperationWriteBase;
/*!
 * \ingroup QtGrpc
 * \brief The QGrpcWriteReplay class contains data for write call of gRPC client API. It's owned by client class, that
 *        created it. QGrpcWriteReplay could be used by QAbstractGrpcChannel implementations to control call work flow and
 *        it return status of QGrpcAsyncOperationWriteBase write method.
 */
class Q_GRPC_EXPORT QGrpcWriteReplay:public QObject
{
    Q_OBJECT
public:

    /*!
     * \enum WriteStatus
     *
     *  \value OK
     *     The write operation success.
     *  \value Failed
     *     The write operation failed.
     *  \value InProcess
     *     The write operation in processing.
     *  \value NotConnected
     *     The write operation failed due to lack of subscription to this method.
     *
     * \brief List of states of messages recording, to the outgoing stream
     */
    typedef enum WriteStatus{
        OK, //
        Failed, //
        InProcess, //Connection process request
        NotConnected //If slot is not connected, we should handle it, not enter to event loop
    } WriteStatus;    

    /*!
     * \return return status of a write message
     */
    inline const WriteStatus& status() const{
        return m_status;
    }

    /*!
     * \return return true, if write successful
     */
    inline bool ok() const{
        return m_status==OK;
    }

    //TODO Should be private
    /*!
     * \brief Specifies the recording status of a message
     * \param[in] reply returned write status
     */
    void setStatus(WriteStatus status){
        this->m_status=status;
    }

    QGrpcWriteReplay(QObject* parent=nullptr):QObject(parent),m_status(NotConnected)
    {}

private:
    WriteStatus m_status;
    friend QGrpcAsyncOperationWriteBase;
signals:
    void finished();
    void error();
};

typedef QSharedPointer<QGrpcWriteReplay> QGrpcWriteReplayShared;
}
