/*
 * Copyright 2009-2011 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.runtime.util;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.asterix.common.api.AsterixAppContextInfoImpl;

public class AsterixRuntimeUtil {

    public static Set<String> getNodeControllersOnIP(String ipAddress) throws Exception {
        return getNodeControllerMap().get(ipAddress);
    }

    public static Set<String> getNodeControllersOnHostName(String hostName) throws Exception {
        Map<String, Set<String>> nodeControllerInfo = getNodeControllerMap();
        String address;
        address = InetAddress.getByName(hostName).getHostAddress();
        if (address.equals("127.0.1.1")) {
            address = "127.0.0.1";
        }
        Set<String> nodeControllersAtLocation = nodeControllerInfo.get(address);
        return nodeControllersAtLocation;
    }

    public static List<String> getAllNodeControllers() throws Exception {
        Collection<Set<String>> nodeControllersCollection = getNodeControllerMap().values();
        List<String> nodeControllers = new ArrayList<String>();
        for (Set<String> ncCollection : nodeControllersCollection) {
            nodeControllers.addAll(ncCollection);
        }
        return nodeControllers;
    }

    private static Map<String, Set<String>> getNodeControllerMap() throws Exception {
        Map<String, Set<String>> nodeControllerMap = new HashMap<String, Set<String>>();
        AsterixAppContextInfoImpl.getInstance().getCCApplicationContext().getCCContext()
                .getIPAddressNodeMap(nodeControllerMap);
        return nodeControllerMap;

    }
}
