package tgtools.mq.rabbit;

import com.rabbitmq.client.*;
import tgtools.exceptions.APPErrorException;
import tgtools.interfaces.IDispose;
import tgtools.mq.rabbit.entity.Message;
import tgtools.mq.rabbit.listen.IMessageListener;
import tgtools.util.LogHelper;
import tgtools.util.StringUtil;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author 田径
 * @Title
 * @Description
 * @date 9:38
 */
public abstract class AbstractConsumer implements IDispose,Closeable {
    private Channel mChannel;
    private String mQueueName;
    private MyDefaultConsumer mDefaultConsumer = null;
    private IMessageListener mMessageListening = null;
    private String mConsumerTag=null;

    /**
     * 获取Channel
     * @return
     */
    public Channel getChannel() {
        return mChannel;
    }

    /**
     * 设置 Channel
     * @param pChannel
     */
    public void setChannel(Channel pChannel) {
        mChannel = pChannel;
    }

    /**
     * 获取 QueueName
     * @return
     */
    public String getQueueName() {
        return mQueueName;
    }

    /**
     * 设置 QueueName
     * @param pQueueName
     */
    public void setQueueName(String pQueueName) {
        mQueueName = pQueueName;
    }



    /**
     * 获取消息监听
     * @return
     */
    public IMessageListener getMessageListening() {
        return mMessageListening;
    }

    /**
     * 设置消息监听
     * @param pMessageListening
     */
    public void setMessageListening(IMessageListener pMessageListening) {
        mMessageListening = pMessageListening;
    }

    /**
     * 获取监听标志
     * 启动监听后可以获得
     * @return
     */
    public String getConsumerTag() {
        return mConsumerTag;
    }


    /**
     *
     * @param pDeliveryTag
     * @throws APPErrorException
     */
    public void basicAck(long pDeliveryTag) throws APPErrorException {
        basicAck(pDeliveryTag, false);
    }

    /**
     *
     * @param pDeliveryTag
     * @param pMultiple
     * @throws APPErrorException
     */
    public void basicAck(long pDeliveryTag, boolean pMultiple) throws APPErrorException {
        try {
            mChannel.basicAck(pDeliveryTag, pMultiple);
        } catch (IOException e) {
            throw new APPErrorException("basicAck 出错；原因：" + e.getMessage(), e);
        }
    }

    /**
     *
     * @param pPrefetchSize
     * @throws APPErrorException
     */
    public void basicQos(int pPrefetchSize) throws APPErrorException {
        try {
            mChannel.basicQos(pPrefetchSize);
        } catch (IOException e) {
            throw new APPErrorException("设置basicQos错误；原因：" + e.getMessage(), e);
        }
    }

    /**
     * 定义Qos
     * @param pPrefetchSize
     * @param pPrefetchCount
     * @param pGlobal
     * @throws APPErrorException
     */
    public void basicQos(int pPrefetchSize, int pPrefetchCount, boolean pGlobal) throws APPErrorException {
        try {
            mChannel.basicQos(pPrefetchSize, pPrefetchCount, pGlobal);
        } catch (IOException e) {
            throw new APPErrorException("设置basicQos错误；原因：" + e.getMessage(), e);
        }
    }

    /**
     * 启动监听 （自动ack true）
     * @throws APPErrorException
     */
    public void startListen() throws APPErrorException {
        startListen(true);
    }

    /**
     * 启动监听
     * @param pAutoAck
     * @throws APPErrorException
     */
    public void startListen(boolean pAutoAck) throws APPErrorException {
        try {
            mConsumerTag= mChannel.basicConsume(mQueueName, pAutoAck, mDefaultConsumer);
        } catch (IOException e) {
            throw new APPErrorException("启动消费者监听失败！原因：" + e.getMessage(), e);
        }
    }

    /**
     * 获取队列中1条消息。
     * @return
     * @throws APPErrorException
     */
    public Message getMessage() throws APPErrorException {
        try {
            GetResponse res = mChannel.basicGet(mQueueName, true);
            return new Message(this, null, res.getEnvelope(), res.getProps(), res.getBody());

        } catch (IOException e) {
            throw new APPErrorException("获取消息错误；原因：" + e.getMessage(), e);
        }
    }

    /**
     * 初始化
     * @param pConnection
     * @throws APPErrorException
     */
    public abstract void init(Connection pConnection) throws APPErrorException;

    /**
     * 释放
     */
    @Override
    public void Dispose() {
        if(!StringUtil.isNullOrEmpty(mConsumerTag)) {
            try {
                mChannel.basicCancel(mConsumerTag);
                mChannel.abort();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (mChannel.isOpen()) {
            try {
                mChannel.close();
            } catch (Exception e) {
                LogHelper.error("", "Channel关闭错误，原因：" + e.getMessage(), "Producer.Dispose", e);
            }
        }
        mChannel = null;
    }
    /**
     * 释放
     */
    @Override
    public void close()
    {
        Dispose();
    }
    /**
     *
     * @param pChannel
     */
    protected void createDefaultConsumer(Channel pChannel) {
        mDefaultConsumer = new MyDefaultConsumer(pChannel);
    }

    /**
     * 监听对象
     */
    class MyDefaultConsumer extends DefaultConsumer {

        public MyDefaultConsumer(Channel channel) {
            super(channel);
        }

        /**
         * 消息处理事件
         * @param consumerTag
         * @param envelope
         * @param properties
         * @param body
         */
        @Override
        public void handleDelivery(String consumerTag, Envelope envelope,
                                   AMQP.BasicProperties properties, byte[] body) {
            if (null != AbstractConsumer.this.mMessageListening) {
                AbstractConsumer.this.mMessageListening.onMessage(new Message(AbstractConsumer.this, consumerTag, envelope, properties, body));
            }
        }
    }
}
