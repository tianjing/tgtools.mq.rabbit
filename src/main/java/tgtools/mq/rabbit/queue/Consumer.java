package tgtools.mq.rabbit.queue;

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

    public void init(Connection pConnection) throws APPErrorException {
        init(pConnection,getQueueName(),getMessageListening());
    }

    public void init(Connection pConnection, String pQueueName, IMessageListener pMessage) throws APPErrorException {
        try {
            setChannel(pConnection.createChannel());
            setQueueName(pQueueName);
            setMessageListening(pMessage);
            createDefaultConsumer(getChannel());
        } catch (Exception e) {
            throw new APPErrorException("初始化Channel出错；原因：" + e.getMessage(), e);
        }
    }


}