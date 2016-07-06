package com.vizix.hazelcast;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ariel on 6/23/16.
 */
public class Client {
    private static HazelcastInstance hz;
    private static final ILogger logger = Logger.getLogger( Client.class );

    public static void main(String args[]) {
        String ip = "10.100.1.155";
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


        // Near cache configuration
        nearCacheConfigFactory(clientConfig,"Cities");
        nearCacheConfigFactory(clientConfig,"com.tierconnect.riot.iot,entities.*");


        hz = HazelcastClient.newHazelcastClient(clientConfig);

        System.out.println(hz.getCluster().getMembers());

        IMap map0 = hz.getMap("Cities");


        showMapContent(map0);

        System.out.println(map0.size());
        for(int i = 0 ; i < 100; i++){
            map0.get(1);
        }

        System.out.println("Hits local map near cache: " + map0.getLocalMapStats().getHits());
        System.out.println("Hits local map near cache: " + map0.getLocalMapStats().getNearCacheStats().getHits());
        IMap map01 = hz.getMap("CitiesLocal");
        System.out.println("near cache local map size: " + map01.size());


        for (int i = 0; i < 60000; i++) {
            logger.info("Iteration " + i + " (CLIENT MAP MONITOR)");
            IMap thingTypesMap = hz.getMap("com.tierconnect.riot.iot.entities.ThingType");
            System.out.println("Map name:   " + thingTypesMap.getName());
            System.out.println("Map size:   " + thingTypesMap.size());
            System.out.println("Local Map Stats:   " + thingTypesMap.getLocalMapStats());
            System.out.println("Local Near Cache stats:   " + thingTypesMap.getLocalMapStats());
            try {
                Thread.sleep(1000);                 //1000 milliseconds is one second.
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }


    }

    private static void showMapContent(IMap map) {
        System.out.println(map.size());
        for (Object k: map.keySet()) {
            Object value = map.get(k);
        }

        System.out.println("Local map stats: " + map.getLocalMapStats());
        System.out.println("Near Cache map stats: " + map.getLocalMapStats().getNearCacheStats());
    }

    private static void nearCacheConfigFactory(ClientConfig clientConfig,String mapName) {

        NearCacheConfig ncc = clientConfig.getNearCacheConfig(mapName);
        if(ncc == null){
            ncc = new NearCacheConfig();
        }
        ncc.setName(mapName);
        ncc.setCacheLocalEntries(true);
        ncc.setEvictionPolicy("NONE");
        ncc.setMaxSize(500000000);
        ncc.setInvalidateOnChange(false);
        //Map<String, NearCacheConfig> nearCache =  new HashMap<String, NearCacheConfig>();
        //nearCache.put(mapName+"Local", ncc);
        clientConfig.addNearCacheConfig(ncc);
    }


}
