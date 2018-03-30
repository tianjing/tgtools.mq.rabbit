package tgtools.mq.rabbit.queue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Connection;
import tgtools.exceptions.APPErrorException;
import tgtools.mq.rabbit.AbstractProducer;
import tgtools.util.StringUtil;

import java.io.IOException;
import java.util.Map;

/**
 * @author 田径
 * @Title
 * @Description
 * @date 9:55
 */
public class Producer extends AbstractProducer {
    public Producer() {
    }



    public void init(Connection pConnection, String pQueueName, boolean pDurable, boolean pExclusive, boolean pAutoDelete,
                     Map<String, Object> pArguments) throws APPErrorException {
        try {
            setChannel(pConnection.createChannel());
            setQueueName(pQueueName);
            queueDeclare(pDurable, pExclusive, pAutoDelete, pArguments);
        } catch (Exception e) {
            throw new APPErrorException("初始化出错！原因：" + e.getMessage(), e);
        }
    }


    public void init(Connection pConnection, String pQueueName) throws APPErrorException {
        init(pConnection, pQueueName, true, false, false, null);
    }

    public void init(Connection pConnection, String pQueueName, boolean pDurable) throws APPErrorException {
        init(pConnection, pQueueName, pDurable, false, false, null);
    }
    @Override
    public void init(Connection pConnection) throws APPErrorException {
        init(pConnection, getQueueName(), true, false, false, null);
    }
    @Override
    public void send(byte[] pMessage, AMQP.BasicProperties pBasicProperties) throws APPErrorException {
        try {
            getChannel().basicPublish(getQueueName(), StringUtil.EMPTY_STRING, pBasicProperties, pMessage);
        } catch (IOException e) {
            throw new APPErrorException("发送队列失败", e);
        }
    }
}
