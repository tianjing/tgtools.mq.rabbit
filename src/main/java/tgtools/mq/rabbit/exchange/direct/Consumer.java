package tgtools.mq.rabbit.exchange.direct;

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

    public Consumer() {
    }
    private String mExchangeName;
    private String mRoutingKey;
    public String getExchangeName() {
        return mExchangeName;
    }

    public void setExchangeName(String pExchangeName) {
        mExchangeName = pExchangeName;
    }


    public String getRoutingKey() {
        return mRoutingKey;
    }

    public void setRoutingKey(String pRoutingKey) {
        mRoutingKey = pRoutingKey;
    }

    protected void queueDeclare() throws APPErrorException {
        try {
            getChannel().exchangeDeclare(getExchangeName(),  BuiltinExchangeType.DIRECT);
            getChannel().queueDeclare(getQueueName(), true, false, false, null);
            getChannel().queueBind(getQueueName(), getExchangeName(), getRoutingKey());
        }catch (Exception e)
        {
            throw new APPErrorException("exchange："+mExchangeName+"绑定 queue:"+getQueueName()+"失败；原因："+e.getMessage(),e);
        }
    }


    public void init(Connection pConnection, String pQueueName, String pExchangeName, String pRouteKey, IMessageListener pMessage) throws APPErrorException {
        try {
           // Consumer consumer = new Consumer();
            setQueueName(pQueueName);
            setExchangeName(pExchangeName);
            setRoutingKey(pRouteKey);
            setMessageListening(pMessage);
            setChannel(pConnection.createChannel());
            createDefaultConsumer(getChannel());
            queueDeclare();
        } catch (Exception e) {
            throw new APPErrorException("创建Channel出错；原因：" + e.getMessage(), e);
        }
    }


    @Override
    public void init(Connection pConnection) throws APPErrorException {
        init(pConnection,getQueueName(),getExchangeName(),getRoutingKey(),getMessageListening());
    }
}
