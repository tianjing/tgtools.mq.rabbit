package tgtools.mq.rabbit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import tgtools.exceptions.APPErrorException;
import tgtools.interfaces.IDispose;
import tgtools.util.LogHelper;
import tgtools.util.StringUtil;

import java.io.IOException;
import java.util.Map;

/**
 * @author 田径
 * @Title
 * @Description
 * @date 9:24
 */
public abstract class AbstractProducer implements IDispose {

    private Channel mChannel;
    private String mQueueName;
    private String mCharsetName = "UTF-8";


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
    protected void queueDeclare(boolean pDurable, boolean pExclusive, boolean pAutoDelete,
                                Map<String, Object> pArguments) throws APPErrorException {
        try {
            getChannel().queueDeclare(getQueueName(), pDurable, pExclusive, pAutoDelete, pArguments);
        } catch (IOException e) {
            throw new APPErrorException("定义队列出错！");
        }
    }
    public void send(byte[] pMessage, AMQP.BasicProperties pBasicProperties) throws APPErrorException {
        try {
            getChannel().basicPublish(StringUtil.EMPTY_STRING, getQueueName(), pBasicProperties, pMessage);
        } catch (IOException e) {
            throw new APPErrorException("发送队列失败", e);
        }
    }

    public void send(String pMessage, AMQP.BasicProperties pBasicProperties) throws APPErrorException {
        try {
            send(pMessage.getBytes(getCharsetName()), pBasicProperties);
        } catch (IOException e) {
            throw new APPErrorException("字符串转换失败", e);
        }
    }

    public void send(String pMessage) throws APPErrorException {
        send(pMessage, null);
    }

    public abstract void init(Connection pConnection) throws APPErrorException;
    @Override
    public void Dispose() {
        if (mChannel.isOpen()) {
            try {
                mChannel.close();
            } catch (Exception e) {
                LogHelper.error("", "Channel关闭错误，原因：" + e.getMessage(), "Producer.Dispose", e);
            }
        }
        mChannel=null;
    }
}
