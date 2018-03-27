package tgtools.mq.rabbit;

import com.rabbitmq.client.ConnectionFactory;
import tgtools.exceptions.APPErrorException;
import tgtools.mq.rabbit.entity.Message;
import tgtools.mq.rabbit.listen.IMessageListener;
import tgtools.mq.rabbit.queue.Consumer;
import tgtools.mq.rabbit.queue.Producer;
import tgtools.tasks.Task;
import tgtools.tasks.TaskContext;
import tgtools.tasks.TaskRunner;


/**
 * @author 田径
 * @Title
 * @Description
 * @date 13:05
 */
public class Test {
    public static void main1(String[] args) {
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

    public static void main(String[] args) {
        String name = "tg1";
        String queueName = "rabbitMQ.test";
        //创建连接工厂
        ConnectionFactory factory = new ConnectionFactory();
        //设置RabbitMQ相关信息
        factory.setHost("192.168.88.134");
        factory.setUsername("admin");
        factory.setPassword("admin");
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
                consumer.init(SingleConnectionFactory.get(mName), mQueueName, new IMessageListener() {
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
                consumer.basicQos(0,1,false);
                consumer.startListen();
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
