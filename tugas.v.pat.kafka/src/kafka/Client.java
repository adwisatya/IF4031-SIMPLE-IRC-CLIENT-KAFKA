/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package kafka;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author adwisatya
 */
public class Client {
    
    public static void main(String args[]) {
        Consumer example = new Consumer();
        long maxReads = Long.parseLong(args[0]);
        String topic = args[1];
        int partition = Integer.parseInt(args[2]);
        List<String> seeds = new ArrayList<String>();
        seeds.add(args[3]);
        int port = Integer.parseInt(args[4]);
        try {
            example.run(maxReads, topic, partition, seeds, port);
        } catch (Exception e) {
            System.out.println("Oops:" + e);
             e.printStackTrace();
        }
    }
}
