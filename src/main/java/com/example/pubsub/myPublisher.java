package com.example.pubsub;



import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.ServiceOptions;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.cloud.pubsub.v1.Publisher;



import java.util.ArrayList;
import java.util.List;

public class myPublisher {

    //using the default project ID
    private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();

    public static void main(String... args) throws Exception {
        String topicID = args[0];
        int msgCount = Integer.parseInt(args[1]);
        ProjectTopicName topicName = ProjectTopicName.of(PROJECT_ID, topicID);

        Publisher publisher = null;
        List<ApiFuture<String>> futures = new ArrayList<>();

        try {
            publisher = Publisher.newBuilder(topicName).build();

            for(int i = 0; i < msgCount; ++i){
                String msg = "message num - " + i;

                //Convert this String named "msg" into bytes and build a new pubsub message
                ByteString msgdata = ByteString.copyFromUtf8(msg);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(msgdata).build();
                ApiFuture<String> future = publisher.publish(pubsubMessage);
                futures.add(future);
            }
        } finally {
            //Waiting on requests pending
            List<String> msgIDs = ApiFutures.allAsList(futures).get();

            for(String msgID: msgIDs){
                System.out.println(msgID);
            }

            if(publisher!= null){
                publisher.shutdown();
            }


        }
    }

}
