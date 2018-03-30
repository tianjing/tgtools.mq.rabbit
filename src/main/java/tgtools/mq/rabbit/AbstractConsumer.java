package tgtools.mq.rabbit;

import com.rabbitmq.client.*;
import tgtools.exceptions.APPErrorException;
import tgtools.interfaces.IDispose;
import tgtools.mq.rabbit.entity.Message;
import tgtools.mq.rabbit.listen.IMessageListener;
import tgtools.util.LogHelper;
import tgtools.util.StringUtil;

import java.io.IOException;
import java.util.Map;

/**
 * @author 田径
 * @Title
 * @Description
 * @date 9:38
 */
public abstract class AbstractConsumer implements IDispose {
    private Channel mChannel;
    private String mQueueName;
    private String mCharsetName = "UTF-8";
    private MyDefaultConsumer mDefaultConsumer = null;
    private IMessageListener mMessageListening = null;
    private String mConsumerTag=null;

    public Channel getChannel() {
        return mChannel;
    }

    public void setChannel(Channel pChannel) {
        mChannel = pChannel;
    }

    public String getQueueName() {
        return mQueueName;
    }

    public void setQueueName(String pQueueName) {
        mQueueName = pQueueName;
    }

    public String getCharsetName() {
        return mCharsetName;
    }

    public void setCharsetName(String pCharsetName) {
        mCharsetName = pCharsetName;
    }


    public IMessageListener getMessageListening() {
        return mMessageListening;
    }

    public void setMessageListening(IMessageListener pMessageListening) {
        mMessageListening = pMessageListening;
    }

    public String getConsumerTag() {
        return mConsumerTag;
    }

    public void setConsumerTag(String pConsumerTag) {
        mConsumerTag = pConsumerTag;
    }

    public void basicAck(long pDeliveryTag) throws APPErrorException {
        basicAck(pDeliveryTag, false);
    }

    public void basicAck(long pDeliveryTag, boolean pMultiple) throws APPErrorException {
        try {
            mChannel.basicAck(pDeliveryTag, pMultiple);
        } catch (IOException e) {
            throw new APPErrorException("basicAck 出错；原因：" + e.getMessage(), e);
        }
    }

    public void basicQos(int pPrefetchSize) throws APPErrorException {
        try {
            mChannel.basicQos(pPrefetchSize);
        } catch (IOException e) {
            throw new APPErrorException("设置basicQos错误；原因：" + e.getMessage(), e);
        }
    }


    public void basicQos(int pPrefetchSize, int pPrefetchCount, boolean pGlobal) throws APPErrorException {
        try {
            mChannel.basicQos(pPrefetchSize, pPrefetchCount, pGlobal);
        } catch (IOException e) {
            throw new APPErrorException("设置basicQos错误；原因：" + e.getMessage(), e);
        }
    }

    public void startListen() throws APPErrorException {
        startListen(true);
    }

    public void startListen(boolean pAutoAck) throws APPErrorException {
        try {
            mConsumerTag= mChannel.basicConsume(mQueueName, pAutoAck, mDefaultConsumer);
        } catch (IOException e) {
            throw new APPErrorException("启动消费者监听失败！原因：" + e.getMessage(), e);
        }
    }

    public Message getMessage() throws APPErrorException {
        try {
            GetResponse res = mChannel.basicGet(mQueueName, true);
            return new Message(this, null, res.getEnvelope(), res.getProps(), res.getBody());

        } catch (IOException e) {
            throw new APPErrorException("获取消息错误；原因：" + e.getMessage(), e);
        }
    }
    public abstract void init(Connection pConnection) throws APPErrorException;
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
    protected void createDefaultConsumer(Channel pChannel) {
        mDefaultConsumer = new MyDefaultConsumer(pChannel);
    }

    class MyDefaultConsumer extends DefaultConsumer {

        /**
         * Constructs a new instance and records its association to the passed-in channel.
         *
         * @param channel the channel to which this consumer is attached
         */
        public MyDefaultConsumer(Channel channel) {
            super(channel);
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope,
                                   AMQP.BasicProperties properties, byte[] body) {
            if (null != AbstractConsumer.this.mMessageListening) {
                AbstractConsumer.this.mMessageListening.onMessage(new Message(AbstractConsumer.this, consumerTag, envelope, properties, body));
            }
        }
    }
}
