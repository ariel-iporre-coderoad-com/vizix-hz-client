package com.vizix.hazelcast;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;

/**
 * Created by ariel on 6/23/16.
 */
public class Client {
    private static HazelcastInstance hz;

    public static void main(String args[]){
        String ip = "localhost";
        String groupName = "dev";
        String pass = "dev-pass";
        if (args.length > 0) {
            ip = args[0];
        }
        if (args.length > 2) {
            groupName = args[1];
            pass = args[2];
        }

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setGroupConfig(new GroupConfig(groupName, pass));
        System.out.println("Connecting to " + ip);
        String[] ips = null;
        if (ip.indexOf(':') == -1) {
            ips = new String[]{ip + ":5701", ip + ":5702", ip + ":5703"};
        } else {
            ips = new String[]{ip};
        }
        clientConfig.addAddress(ips);
        hz = HazelcastClient.newHazelcastClient(clientConfig);
        System.out.println(hz.getCluster().getMembers());


    }
}
