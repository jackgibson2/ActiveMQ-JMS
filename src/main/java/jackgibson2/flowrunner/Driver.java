package jackgibson2.flowrunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.qpid.jms.JmsConnectionFactory;

class Driver {
	private String host;
	List<String> addresses;
	private String user;
	private String password;
	private int port;
	private int runtime;

	public Driver(String xmlFile) {
		Configurations configs = new Configurations();
		try {
			XMLConfiguration config = configs.xml("driver.xml");
			host = config.getString("host");
			user = config.getString("user");
			password = config.getString("password");
			port = config.getInt("port");
			runtime = config.getInt("runtime");
			addresses = config.getList(String.class, "addresses.address");
			for (int i = 0; i < addresses.size(); i++) {
				System.out.println(addresses.get(i));
			}
		} catch (ConfigurationException cex) {
			System.out.println(cex.getMessage());
			cex.printStackTrace();
		}
	}

	private int getRandomIndex() {
		Random random = new Random();
		return random.nextInt(this.addresses.size());
	}

	public void run() throws Exception {

		String connectionURI = "amqp://" + this.host + ":" + this.port;

		int size = 256;

		String DATA = "abcdefghijklmnopqrstuvwxyz";
		String body = "";
		for (int i = 0; i < size; i++) {
			body += DATA.charAt(i % DATA.length());
		}

		JmsConnectionFactory factory = new JmsConnectionFactory(connectionURI);

		Connection connection = factory.createConnection(user, password);
		connection.start();

		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		Destination destination = null;
		MessageProducer producer = null;
		ArrayList<MessageProducer> producers = new ArrayList<MessageProducer>();
		for (int i = 0; i < this.addresses.size(); i++) {
			destination = session.createQueue(addresses.get(i));
			producer = session.createProducer(destination);
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			producers.add(producer);
		}
		long endTime = System.currentTimeMillis() + this.runtime;
		int i = 1;
		while (System.currentTimeMillis() < endTime) {
			TextMessage msg = session.createTextMessage("#:" + i);
			msg.setIntProperty("id", i);
			producers.get(this.getRandomIndex()).send(msg);

			if ((i % 1000) == 0) {
				System.out.println(String.format("Sent %d messages", i));
			}
			i++;
		}

		Thread.sleep(1000 * 3);
		connection.close();
		System.exit(0);
	}

	public static void main(String[] args) {
		String xmlFile = "driver.xml";

		Driver publisher = new Driver(xmlFile);
		try {
			publisher.run();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}