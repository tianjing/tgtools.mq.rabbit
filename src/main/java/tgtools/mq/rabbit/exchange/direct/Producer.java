package tgtools.mq.rabbit.exchange.direct;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Connection;
import tgtools.exceptions.APPErrorException;
import tgtools.mq.rabbit.AbstractProducer;

import java.io.IOException;
import java.util.Map;

/**
 * @author 田径
 * @Title
 * @Description
 * @date 9:55
 */
public class Producer extends AbstractProducer {

    private String mExchangeName;
    private String mRoutingKey;

    public Producer() {
    }

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

    public void init(Connection pConnection, String pExchangeName, String pRoutingKey,
                     boolean pDurable) throws APPErrorException {
        init(pConnection,pExchangeName,pRoutingKey,pDurable,false,false,null);
    }

    public void init(Connection pConnection, String pExchangeName, String pRoutingKey,
                     boolean pDurable, boolean pExclusive, boolean pAutoDelete,
                     Map<String, Object> pArguments) throws APPErrorException {

        try {
            setChannel(pConnection.createChannel());
            setExchangeName(pExchangeName);
            setRoutingKey(pRoutingKey);
            queueDeclare(pDurable, pExclusive, pAutoDelete, pArguments);
        } catch (Exception e) {
            throw new APPErrorException("创建出错！原因：" + e.getMessage(), e);
        }
    }

    @Override
    public void init(Connection pConnection) throws APPErrorException {
        init(pConnection, getExchangeName(), getRoutingKey(), true);
    }


    @Override
    protected void queueDeclare(boolean pDurable, boolean pExclusive, boolean pAutoDelete,
                                Map<String, Object> pArguments) throws APPErrorException {
        try {
            getChannel().exchangeDeclare(mExchangeName, BuiltinExchangeType.DIRECT, pDurable, pExclusive, pAutoDelete, pArguments);
        } catch (IOException e) {
            throw new APPErrorException("定义队列出错！");
        }
    }

    @Override
    public void send(byte[] pMessage, AMQP.BasicProperties pBasicProperties) throws APPErrorException {
        try {
            getChannel().basicPublish(getExchangeName(), getRoutingKey(), pBasicProperties, pMessage);
        } catch (IOException e) {
            throw new APPErrorException("发送队列失败", e);
        }
    }


}
