import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.PulsarClient;

import java.io.IOException;

public class PulsarSubscriptionIssue {

    public static void main(String[] args) throws PulsarClientException {

        final Runtime JVMRuntime = Runtime.getRuntime();
        String serviceUrlOfPulsarServer = "pulsar+ssl://api-peamouth-0b57f3c7.paas.macrometa.io:6651";
        String gdnAPIToken = "Tu_TZ0W2cR92-sr1j-l7ACA.newone.9pej9tihskpx2vYZaxubGW3sFCJLzxe55NRh7T0uk1JMYiRmHdiQsWh5JhRXXT6c418385";
        String topicOfStream = "Tu_TZ0W2cR92-sr1j-l7ACA/c8local._system/c8locals.OutputStream";
        final int NUM_OF_READERS = 13;
        Reader<byte[]> reader = null;

        // create pulsar client
        PulsarClient pulsarClient = PulsarClient
                .builder()
                .serviceUrl(serviceUrlOfPulsarServer)
                .authentication(AuthenticationFactory.token(gdnAPIToken))
                .build();

        for (int i = 0; i < NUM_OF_READERS; i++) {
            System.out.printf("creating reader %d \n",i);

            try {
                // Create a reader on a topic and for a specific message (and onward)
                reader = pulsarClient
                        .newReader()
                        .topic(topicOfStream)
                        .startMessageId(MessageId.latest)
                        .create();

            } catch (PulsarClientException e) {
                e.printStackTrace();
                return;
            }
        }

        Reader<byte[]> finalReader = reader;
        JVMRuntime.addShutdownHook(new Thread(){
            @Override
            public void run() {
                try {
                    finalReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
