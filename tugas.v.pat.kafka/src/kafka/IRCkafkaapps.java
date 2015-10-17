package kafka;

import java.io.IOException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import scala.collection.Seq;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author mhusainj
 */
public class IRCkafkaapps {
    private static String TOPIC = "lounge";
    private static List<String> channellist = new ArrayList<>();
    private static String user;
    private static String[] usernamelist = {"lalala", "randomize", "fafafa", "anehkuz", "kuzma", "borma", "zip", "jomblo", "ceudih", "ceumungudh"};
    private String queueName;
    private static Hashtable<String, ChannelListener> source = new Hashtable<String,ChannelListener>(); 
    private static HashMap<String, ChannelListener> map = new HashMap(source);
    
    public static void main(String[] argv)
        throws java.io.IOException, TimeoutException {

            Properties properties = new Properties();
            properties.put("metadata.broker.list","localhost:9092");
            properties.put("serializer.class","kafka.serializer.StringEncoder");
            ProducerConfig producerConfig = new ProducerConfig(properties);
        
            Random rnd = new Random();
            int indeks = rnd.nextInt(usernamelist.length);
            String Randusername = usernamelist[indeks] + rnd.nextInt(10000);
            
            
            user = Randusername;
            channellist.add("lounge");            
            map.put("lounge", new ChannelListener(user, "lounge"));
            map.get("lounge").start();
            
            Scanner sc = new Scanner(System.in);
            
            System.out.println(" Welcome "+user+" to Channel lounge");
            System.out.println(" Queries: ");
            System.out.println(" 1. /NICK <Username>            : Change username ");
            System.out.println(" 2. /JOIN <Channel Name>        : Join Channel");
            System.out.println(" 3. @<Channel Name> <Message>   : Send message to Spesific Channel");
            System.out.println(" 4. /LEAVE <Channel Name>       : Leave Channel");
            System.out.println(" 5. <Random text>               : BroadCast");
            System.out.println(" 6. /EXIT                       : Exit App");
            while (true){
                
                String input = sc.nextLine();
                String[] query;
                if ((query = CommandRegexes.NICK.match(input)) != null) {
                    String Nickname = query[0];
                    user = Nickname;
                    resetallchannel(user);
                    System.out.println(" [x] Your Nickname '" + Nickname + "'");
                    
                } else if ((query = CommandRegexes.JOIN.match(input)) != null) {
                    String channel_name = query[0];
                    if(!map.containsKey(channel_name)){
                        map.put(channel_name, new ChannelListener(user, channel_name));
                        map.get(channel_name).start();
                        channellist.add(channel_name);
                    }
                    System.out.println(" [x] you had Join '" + channel_name + "'");
                    
                } else if ((query = CommandRegexes.LEAVE.match(input)) != null) {
                    String channel_name = query[0];
                    if(map.containsKey(channel_name)){
                        map.get(channel_name).shutdown();
                        map.remove(channel_name);
                        channellist.remove(channellist.indexOf(channel_name));
                        System.out.println(" [x] you had leave '" + channel_name +"'");
                    }else{
                        System.out.println(" [x] you're not in the channel "+channel_name);
                    }
                    
                } else if (CommandRegexes.EXIT.match(input) != null) {
                    System.out.println(" Please Wait While Exiting . . . . .");
                    deleteallchannel();
                    System.out.println(" bye . . . ");
                    break;
                    
                } else if ((query = CommandRegexes.MESSAGE_CHANNEL.match(input)) != null) {
                    String channel_name = query[0];
                    if (channellist.contains(channel_name)){
                        String messagestring = "["+channel_name+"] ("+user+") "+ query[1];
                        kafka.javaapi.producer.Producer<String,String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
                        SimpleDateFormat sdf = new SimpleDateFormat();
                        KeyedMessage<String, String> message =new KeyedMessage<String, String>(channel_name,messagestring);
                        producer.send(message);
                        producer.close();
                        System.out.println(" [x] Sent '" + messagestring + "'");
                        System.out.println(" : to '" + channel_name + "'");
                    }else{
                        System.out.println(" [x] No Channel "+channel_name+" on your list");
                    }
                } else {
                    System.out.println(" [x] Broadcasting '" + input + "'");
                    for (String channellist1 : channellist) {
                        String messagestring = "["+channellist1+"] ("+user+") "+input;
                        kafka.javaapi.producer.Producer<String,String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
                        KeyedMessage<String, String> message =new KeyedMessage<String, String>(channellist1,messagestring);
                        producer.send(message);
                        producer.close();
                    }
                    System.out.println(" [x] OK ;D");
                }
            }
            
                
    }
    
    private static void deleteallchannel(){
        for (String channellist1 : channellist) {
            map.get(channellist1).shutdown();
        }
        map.clear();
    }
    private static void resetallchannel(String usera){
        for (String channellist1 : channellist) {
            map.get(channellist1).shutdown();
            map.remove(channellist1);
            map.put(channellist1, new ChannelListener(usera, channellist1));
            map.get(channellist1).start();
        }
    }
}