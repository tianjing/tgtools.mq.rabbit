package tgtools.mq.rabbit.exchange.topic;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import tgtools.exceptions.APPErrorException;

import java.io.IOException;
import java.util.Map;

/**
 * @author 田径
 * @Title
 * @Description
 * @date 9:55
 */
public class Producer extends tgtools.mq.rabbit.exchange.direct.Producer {
    private String mExchangeName;
    private String mRouteKey;

    @Override
    public String getExchangeName() {
        return mExchangeName;
    }

    @Override
    public void setExchangeName(String pExchangeName) {
        mExchangeName = pExchangeName;
    }

    public String getRouteKey() {
        return mRouteKey;
    }

    public void setRouteKey(String pRouteKey) {
        mRouteKey = pRouteKey;
    }

    @Override
    protected void queueDeclare() throws APPErrorException {
        try {
            getChannel().exchangeDeclare(getExchangeName(), BuiltinExchangeType.TOPIC);
        } catch (IOException e) {
            throw new APPErrorException("定义队列出错！");
        }
    }

    @Override
    protected void queueDeclare(boolean pDurable, boolean pExclusive, boolean pAutoDelete,
                                Map<String, Object> pArguments) throws APPErrorException {
        try {
            getChannel().exchangeDeclare(getExchangeName(), BuiltinExchangeType.TOPIC, pDurable, pExclusive, pAutoDelete, pArguments);
        } catch (IOException e) {
            throw new APPErrorException("定义队列出错！");
        }
    }
    @Override
    public void send(byte[] pMessage, AMQP.BasicProperties pBasicProperties) throws APPErrorException {
        try {
            getChannel().basicPublish(getExchangeName(), getRouteKey(), pBasicProperties, pMessage);
        } catch (IOException e) {
            throw new APPErrorException("发送队列失败", e);
        }
    }
}
