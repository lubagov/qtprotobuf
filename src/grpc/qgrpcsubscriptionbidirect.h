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

#pragma once //QGrpcSubscriptionBidirect

#include <functional>
#include <QMutex>
#include <memory>

#include "qabstractgrpcchannel.h"
#include "qabstractgrpcclient.h"
#include "qgrpcasyncoperationwritebase_p.h"

#include "qtgrpcglobal.h"

namespace QtProtobuf {

class QAbstractGrpcClient;

/*!
 * \ingroup QtGrpc
 * \brief The QGrpcSubscriptionBidirect class
 */
class Q_GRPC_EXPORT QGrpcSubscriptionBidirect final : public QGrpcAsyncOperationWriteBase
{
    Q_OBJECT
public:
    /*!
     * \brief Cancels this subscription and try to abort call in channel
     */
    void cancel();

    /*!
     * \brief Returns method for this subscription
     */
    QString method() const {
        return m_method;
    }

    /*!
     * \brief Invokes handler method assigned to this subscription.
     * \param data updated subscription data buffer
     * \details Should be used by QAbstractGrpcChannel implementations,
     *          to update data in subscription and notify clients about subscription updates.
     */
    void handler(const QByteArray& data) {
        setData(data);
        for (auto handler : m_handlers) {
            handler(data);
        }
        updated();
    }

signals:
    /*!
     * \brief The signal is emitted when subscription received updated value from server
     */
    void updated();

protected:
    //! \private
    QGrpcSubscriptionBidirect(const std::shared_ptr<QAbstractGrpcChannel> &channel, const QString &method,
                      const SubscriptionHandler &handler, QAbstractGrpcClient *parent);
    //! \private
    virtual ~QGrpcSubscriptionBidirect() = default;

    //! \private
    void addHandler(const SubscriptionHandler &handler);

    bool operator ==(const QGrpcSubscriptionBidirect &other) const {
        return other.method() == this->method();
    }

private:
    friend class QAbstractGrpcClient;
    QString m_method;
    std::vector<SubscriptionHandler> m_handlers;
};

}
