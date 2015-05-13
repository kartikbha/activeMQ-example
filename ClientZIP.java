package test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.Message;

public class ClientZIP {
	private static int ackMode;
	private boolean transacted = false;
	static {
		ackMode = Session.AUTO_ACKNOWLEDGE;
	}

	public ClientZIP(String clientQueueName, long msg_count) {

		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
				"tcp://localhost:61616");
		Connection connection;
		try {



			connectionFactory.setAlwaysSessionAsync(true);
			connectionFactory.setOptimizeAcknowledge(true);

			ActiveMQPrefetchPolicy preFetchPolicy = new ActiveMQPrefetchPolicy();
			preFetchPolicy.setQueuePrefetch(5000);
			preFetchPolicy.setMaximumPendingMessageLimit(10000);
			preFetchPolicy.setQueueBrowserPrefetch(2000);

			// preFetchPolicy
			connectionFactory.setPrefetchPolicy(preFetchPolicy);
			connection = connectionFactory.createConnection();
			connection.start();

			Session session = connection.createSession(transacted, ackMode);
			Destination queue = session.createQueue(clientQueueName);
			MessageConsumer consumer = session.createConsumer(queue);

			/*
			 * String filterSelector = "myfilter='" + clientQueueName + "'";
			 * MessageConsumer consumer = session.createConsumer(queue,
			 * filterSelector, false);
			 */

			// Synchronous message consumer
			int i = 1;
			long startmilli = System.currentTimeMillis();
			while (true) {
				Message message = (Message) consumer.receive(5000);
				if (message != null) {
					if (message instanceof TextMessage) {
						String text = ((TextMessage) message).getText();
						System.out.println(" TXT msg RECEIVED " + i + " time "
								+ new Date());

					} else if (message instanceof BytesMessage) {

						BytesMessage bm = (BytesMessage) message;
						byte data[] = new byte[(int) bm.getBodyLength()];
						bm.readBytes(data);

						try {
							dobytesUnZipAndRead(data);
						} catch (Exception e) {

						}
						
						if(i%100==0){
							System.out.println(" BYTE msg RECEIVED " + i + " time "
									+ new Date());
						}

					}
				}

				if (i == msg_count) {
					long endMilli = System.currentTimeMillis();
					long diff = (endMilli - startmilli) / 1000;
					System.out.print("time to received " + msg_count + " is "
							+ diff);
					consumer.close();
					session.close();
					connection.close();
					break;
				}
				i++;
			}

		} catch (JMSException e) {
			// Handle the exception appropriately
			System.out.println("  error in client   " + e);
		}
	}

	private String dobytesUnZipAndRead(byte[] bytes)
			throws FileNotFoundException, IOException {

		ZipInputStream zipStream = new ZipInputStream(new ByteArrayInputStream(
				bytes));

		ByteArrayOutputStream streamBuilder = new ByteArrayOutputStream();
		int bytesRead;
		byte[] tempBuffer = new byte[8192 * 2];
		ZipEntry entry = (ZipEntry) zipStream.getNextEntry();
		try {
			while ((bytesRead = zipStream.read(tempBuffer)) != -1) {
				streamBuilder.write(tempBuffer, 0, bytesRead);
			}
		} catch (IOException e) {

		}
		
		System.out.println(" streamBuilder.toString()---> " +streamBuilder.toString());
		return streamBuilder.toString();
	}

	public static void main(String[] args) {

		System.out.println("################ RUNNING CLIENT  #############");

		System.out.println(" queue name  " + args[0]);
		System.out.println(" msg count   " + args[1]);

		String clientQueueName = args[0];
		long msg_count = Long.parseLong(args[1]);
		new ClientZIP(clientQueueName, msg_count);
	}

}
