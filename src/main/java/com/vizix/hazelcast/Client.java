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

        for (int i = 0; i < 60000; i++) {
            logger.info("Iteration " + i + " query for the map things is:");
            IMap map1 = hz.getMap("com.tierconnect.riot.iot.entities.Zone");
            System.out.println("zones map in the node :   " + map1.getName());
            System.out.println("zones map in the node:     " + map1.size());
            try {
                Thread.sleep(1000);                 //1000 milliseconds is one second.
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            IMap thingTypesMap = hz.getMap("com.tierconnect.riot.iot.entities.ThingType");
            System.out.println("thingtypemap in the near cache:   " + thingTypesMap.getName());
            System.out.println("thingtypemap in the near cache:   " + thingTypesMap.size());
            System.out.println("thingtypemap in the near cache:   " + thingTypesMap.getLocalMapStats().getNearCacheStats().getHits());
        }


    }

    private static void showMapContent(IMap map) {
        System.out.println(map.size());
        for (Object k: map.keySet()) {
            Object value = map.get(k);
        }

        System.out.println(map.getLocalMapStats().getNearCacheStats().getHits());
    }

    private static void nearCacheConfigFactory(ClientConfig clientConfig,String mapName) {

        NearCacheConfig ncc = clientConfig.getNearCacheConfig(mapName);
        if(ncc == null){
            ncc = new NearCacheConfig();
        }
        ncc.setName(mapName);
        ncc.setCacheLocalEntries(true);
        ncc.setEvictionPolicy("LRU");
        ncc.setMaxSize(500000);
        ncc.setInvalidateOnChange(true);
        Map<String, NearCacheConfig> nearCache =  new HashMap<String, NearCacheConfig>();
        nearCache.put("ThingTypeLocal", ncc);
        clientConfig.addNearCacheConfig(ncc);
    }


}
