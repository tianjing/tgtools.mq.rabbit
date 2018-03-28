package tgtools.mq.rabbit.exchange.fanout;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Connection;
import tgtools.exceptions.APPErrorException;
import tgtools.mq.rabbit.AbstractConsumer;
import tgtools.mq.rabbit.listen.IMessageListener;

/**
 * @author 田径
 * @Title
 * @Description
 * @date 10:58
 */
public class Consumer extends AbstractConsumer {

    private String mExchangeName;

    public Consumer() {
    }

    public String getExchangeName() {
        return mExchangeName;
    }

    public void setExchangeName(String pExchangeName) {
        mExchangeName = pExchangeName;
    }


    protected void queueDeclare() throws APPErrorException {
        try {
            getChannel().exchangeDeclare(mExchangeName, BuiltinExchangeType.FANOUT);
            getChannel().queueDeclare(getQueueName(), true, false, false, null);
            getChannel().queueBind(getQueueName(), mExchangeName, "");
        } catch (Exception e) {
            throw new APPErrorException("exchange：" + mExchangeName + "绑定 queue:" + getQueueName() + "失败；原因：" + e.getMessage(), e);
        }
    }

    @Override
    public void init(Connection pConnection) throws APPErrorException {
        init(pConnection,getQueueName(),getExchangeName(),getMessageListening());
    }

    public void init(Connection pConnection, String pQueueName, String pExchangeName, IMessageListener pMessage) throws APPErrorException {
        try {
            setQueueName(pQueueName);
            setExchangeName(pExchangeName);
            setMessageListening(pMessage);
            setChannel(pConnection.createChannel());
            createDefaultConsumer(getChannel());
            queueDeclare();
        } catch (Exception e) {
            throw new APPErrorException("创建Channel出错；原因：" + e.getMessage(), e);
        }
    }


}
