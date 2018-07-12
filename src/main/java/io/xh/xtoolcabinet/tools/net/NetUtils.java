package io.xh.xtoolcabinet.tools.net;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class NetUtils {

    private static final Logger logger = LoggerFactory.getLogger(NetUtils.class);


    public static String localIp() {
        String localIp = null;
        try {
            InetAddress address = getIpAddress();

            return address.getHostAddress();

        } catch (UnknownHostException e) {
            logger.error("get local ip exception", e);
            return null;
        }
    }




    private static InetAddress getIpAddress() throws UnknownHostException {
        try {

            List<InetAddress> siteLocalAddressList = null;

            // 遍历所有的网络接口
            for (Enumeration ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements(); ) {
                NetworkInterface iface = (NetworkInterface) ifaces.nextElement();
                // 在所有的接口下再遍历IP
                for (Enumeration inetAddrs = iface.getInetAddresses(); inetAddrs.hasMoreElements(); ) {
                    InetAddress inetAddr = (InetAddress) inetAddrs.nextElement();

                    // 排除loopback类型地址
                    if (inetAddr.isLoopbackAddress() || inetAddr.getHostAddress().indexOf(":") != -1) {
                        continue;
                    }

                    // 外网地址
                    if (!inetAddr.isSiteLocalAddress()) {
                        return inetAddr;
                    }

                    if (siteLocalAddressList == null) {
                        siteLocalAddressList = new ArrayList<InetAddress>();
                    }

                    siteLocalAddressList.add(inetAddr);
                }
            }

            // 最大子网中的ip
            InetAddress candidateAddress = findOuterSiteLocalAddress(siteLocalAddressList);

            if (candidateAddress != null) {
                return candidateAddress;
            }

            // 如果没有发现 non-loopback地址.只能用最次选的方案
            InetAddress jdkSuppliedAddress = InetAddress.getLocalHost();
            if (jdkSuppliedAddress == null) {
                throw new UnknownHostException("The JDK InetAddress.getLocalHost() method unexpectedly returned null.");
            }
            return jdkSuppliedAddress;
        } catch (Exception e) {
            UnknownHostException unknownHostException = new UnknownHostException(
                    "Failed to determine LAN address: " + e);
            unknownHostException.initCause(e);
            throw unknownHostException;
        }
    }



    private static IpMatcher[] IP_MATCHERS = {
            // "10/8"
            new IpMatcher() {
                @Override
                public boolean match(InetAddress addr) {
                    byte[] bytes = addr.getAddress();
                    return (bytes[0] & 0xFF) == 10;
                }
            },
            // 172.16/12,
            new IpMatcher() {
                @Override
                public boolean match(InetAddress addr) {
                    byte[] bytes = addr.getAddress();
                    return ((bytes[0] & 0xFF) == 172) && ((bytes[1] & 0xF0) == 16);
                }
            },

            // 192.168/16
            new IpMatcher() {
                @Override
                public boolean match(InetAddress addr) {
                    byte[] bytes = addr.getAddress();
                    return ((bytes[0] & 0xFF) == 192) && ((bytes[1] & 0xFF) == 168);
                }
            }

    };

    private static InetAddress findOuterSiteLocalAddress(List<InetAddress> addressList) {

        if (addressList == null) {
            return null;
        }

        for (IpMatcher ipMatcher : IP_MATCHERS) {
            for (InetAddress address : addressList) {
                if (ipMatcher.match(address)) {
                    return address;
                }
            }
        }

        return null;
    }

    private interface IpMatcher {
        boolean match(InetAddress addr);
    }
}
