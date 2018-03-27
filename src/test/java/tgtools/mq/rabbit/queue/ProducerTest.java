package tgtools.mq.rabbit.queue;

import com.rabbitmq.client.ConnectionFactory;
import org.junit.Test;
import tgtools.exceptions.APPErrorException;
import tgtools.mq.rabbit.SingleConnectionFactory;
import tgtools.tasks.Task;
import tgtools.tasks.TaskContext;
import tgtools.tasks.TaskRunner;

import static org.junit.Assert.*;

/**
 * @author 田径
 * @Title
 * @Description
 * @date 20:16
 */
public class ProducerTest {

    @Test
    public void startSendMessage()
    {
        String name = "tg1";
        String queueName = "rabbitMQ.test";
        //创建连接工厂
        ConnectionFactory factory = new ConnectionFactory();
        //设置RabbitMQ相关信息
        factory.setHost("192.168.88.134");
        factory.setUsername("admin");
        factory.setPassword("admin");
        SingleConnectionFactory.add(name, factory);

        ProducerTask task1 = new ProducerTask(name, queueName);
        //ConsumerTask task2 =new ConsumerTask(name,queueName);
        TaskRunner<Task> ss = new TaskRunner<Task>();
        ss.add(task1);
        //ss.add(task2);
        ss.runThreadTillEnd();
        System.out.println("all end");
        SingleConnectionFactory.clear();

    }
    private static class ProducerTask extends Task {
        private String mName;
        private String mQueueName;

        public ProducerTask(String pName, String pQueueName) {
            mName = pName;
            mQueueName = pQueueName;
        }

        @Override
        protected boolean canCancel() {
            return true;
        }

        @Override
        public void run(TaskContext taskContext) {
            String text = "tianjing message";

            Producer producer = new Producer();
            try {
                producer.init(SingleConnectionFactory.get(mName), mQueueName);
            } catch (APPErrorException e) {
                e.printStackTrace();
            }


            for (int i = 0; i < 10; i++) {
                if (isCancel()) {
                    break;
                }
                try {
                    producer.send(text);
                    System.out.println("send messaged");
                    Thread.sleep(5000);
                } catch (Exception e) {
                    System.out.println("send messaged error");
                }

            }
            if (null != producer) {
                producer.Dispose();
            }
        }
    }
}