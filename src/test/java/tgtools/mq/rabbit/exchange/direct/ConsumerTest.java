package tgtools.mq.rabbit.exchange.direct;

import com.rabbitmq.client.ConnectionFactory;
import org.junit.Test;
import tgtools.mq.rabbit.SingleConnectionFactory;
import tgtools.mq.rabbit.entity.Message;
import tgtools.mq.rabbit.listen.IMessageListener;
import tgtools.tasks.Task;
import tgtools.tasks.TaskContext;
import tgtools.tasks.TaskRunner;


/**
 * @author 田径
 * @Title
 * @Description
 * @date 13:45
 */
public class ConsumerTest {
    @Test
    public void startListen() {
        String name = "tg1";
        String queueName = "rabbitMQ.test";
        //创建连接工厂
        ConnectionFactory factory = new ConnectionFactory();
        //设置RabbitMQ相关信息
        factory.setHost("192.168.88.134");
        factory.setUsername("user");
        factory.setPassword("user123");
        factory.setVirtualHost("/user");

        SingleConnectionFactory.add(name, factory);

        //ProducerTask task1 =new ProducerTask(name,queueName);
        ConsumerTask task2 = new ConsumerTask(name, queueName);
        TaskRunner<Task> ss = new TaskRunner<Task>();
        //ss.add(task1);
        ss.add(task2);
        ss.runThreadTillEnd();
        System.out.println("all end");
        SingleConnectionFactory.clear();
    }

    private static class ConsumerTask extends Task {
        private String mName;
        private String mQueueName;

        public ConsumerTask(String pName, String pQueueName) {
            mName = pName;
            mQueueName = pQueueName;
        }

        @Override
        protected boolean canCancel() {
            return true;
        }
        boolean canrun=true;
        @Override
        public void run(TaskContext taskContext) {
            String text = "tianjing message";
            Consumer consumer = new Consumer();
            try {
                consumer.init(SingleConnectionFactory.get(mName), mQueueName,mQueueName,mQueueName, new IMessageListener() {
                    @Override
                    public void onMessage(Message messages) {
                        try {
                            System.out.println("onMessage:" + new String(messages.getBody(), "UTF-8"));
                            Consumer con= (Consumer)messages.getSender();
                            //con.basicAck(messages.getEnvelope().getDeliveryTag());
                            Thread.sleep(5000);
                            ConsumerTask.this.canrun= false;
                        } catch (Exception e) {
                            System.out.println("onMessage Error");
                            e.printStackTrace();
                        }
                    }
                });
               // consumer.basicQos(0,1,false);
               // consumer.startListen();
            } catch (Exception e) {
                e.printStackTrace();
            }
            while(this.canrun)
            {
                try {
                    Thread.sleep(15000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            consumer.Dispose();
            System.out.println("this end");
        }
    }
}