package tgtools.mq.rabbit.exchange.topic;

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
}
