package test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;

public class ServerZIP {
	private static int ackMode;

	private static String messageBrokerUrl;
	private Session session;
	private boolean transacted = false;

	static {

		// on 423
		// messageBrokerUrl = "tcp://localhost:61616?socketBufferSize=131072";

		// compression on 424
		// off
		// messageBrokerUrl =
		// "tcp://localhost:61616?ioBufferSize=16384&socketBufferSize=131072";

		// 435 - compression off
		// 413/412 compression on
		messageBrokerUrl = "tcp://localhost:61616";

		// 416/444 compression on
		// 439 compression off
		// messageBrokerUrl = "tcp://localhost:61616?ioBufferSize=16384";

		ackMode = Session.AUTO_ACKNOWLEDGE;
	}

	public ServerZIP(String clientQueueName, long msg_count) {
		this.setupMessageQueueProducer(clientQueueName, msg_count);
	}

	private void setupMessageQueueProducer(String clientQueueName,
			long msg_count) {
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
				messageBrokerUrl);
		Connection connection;
		try {

			connectionFactory.setUseCompression(true);

			connectionFactory.setAlwaysSessionAsync(true);
			connectionFactory.setOptimizeAcknowledge(true);
			connectionFactory.setAlwaysSyncSend(false);
			connectionFactory.setDispatchAsync(false);
			connectionFactory.setSendAcksAsync(true);
			connectionFactory.setUseAsyncSend(true);

			connection = connectionFactory.createQueueConnection();
			connection.start();
			this.session = connection.createSession(this.transacted, ackMode);

			Destination destination = this.session.createQueue(clientQueueName);

			MessageProducer producer = session.createProducer(destination);
			producer.setTimeToLive(500000000);

			long startmilli = System.currentTimeMillis();

			for (int i = 1; i <= msg_count; i++) {
				producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

				BytesMessage message = session.createBytesMessage();
				message.writeBytes(zipMethod(readFile("./src/test/input.txt")));
				// message.setStringProperty("myfilter",clientQueueName);
				producer.send(message);
				if (i % 100 == 0) {
					System.out.println(" SEND " + i + " time " + new Date());
				}
			}
			
			
			long endMilli = System.currentTimeMillis();
			long diff = (endMilli - startmilli) / 1000;
			System.out.print("time to send " + msg_count + " is " + diff);

			producer.close();
			session.close();
			connection.close();

		} catch (JMSException e) {
			System.out.println("   error in server  " + e);
		}
	}

	public static void main(String[] args) {

		System.out.println("################ RUNNING SERVER  #############");
		System.out.println(" queue name  " + args[0]);
		System.out.println(" msg count   " + args[1]);

		String clientQueueName = args[0];
		long msg_count = Long.parseLong(args[1]);
		new ServerZIP(clientQueueName, msg_count);
	}

	private byte[] zipMethod(String str) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (ZipOutputStream zos = new ZipOutputStream(baos)) {
			ZipEntry entry = new ZipEntry("test.txt");
			zos.putNextEntry(entry);
			zos.write(str.getBytes());
			zos.closeEntry();
		} catch (IOException ioe) {

		}
		return baos.toByteArray();
	}


	public String readFile(String filename) {
		String content = null;
		File file = new File(filename); // for ex foo.txt
		try {
			FileReader reader = new FileReader(file);
			char[] chars = new char[(int) file.length()];
			reader.read(chars);
			content = new String(chars);
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return content;
	}
}