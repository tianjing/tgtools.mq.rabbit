package tgtools.mq.rabbit.exchange.topic;

import com.rabbitmq.client.BuiltinExchangeType;
import tgtools.exceptions.APPErrorException;

/**
 * @author 田径
 * @Title
 * @Description
 * @date 10:58
 */
public class Consumer extends tgtools.mq.rabbit.exchange.direct.Consumer {
    @Override
    protected void queueDeclare() throws APPErrorException {
        try {
            getChannel().exchangeDeclare(getExchangeName(),  BuiltinExchangeType.TOPIC);
            getChannel().queueDeclare(getQueueName(), true, false, false, null);
            getChannel().queueBind(getQueueName(), getExchangeName(), getRoutingKey());
        }catch (Exception e)
        {
            throw new APPErrorException("exchange："+getExchangeName()+"绑定 queue:"+getQueueName()+"失败；原因："+e.getMessage(),e);
        }
    }
}
