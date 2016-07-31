package eco.windrain.kafka.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

public class HostUtil {
	
	private static volatile String hostIpCached;
	
	private static volatile String hostNameCached;

	public static String getHostIp() {
		if (hostIpCached == null) {
			hostIpCached = getHostRealIp(); // NOT getHostAddr(true);
		}
		return hostIpCached;
	}

	public static String getHostName() {
		if (hostNameCached == null) {
			hostNameCached = getHostAddr(false); 
		}
		return hostNameCached;
	}

	private static String getHostAddr(boolean isDigit) {
		try {
			String hostName = InetAddress.getLocalHost().getHostName();
			if (!isDigit) {
				return hostName;
			}
			InetAddress addr = InetAddress.getByName(hostName);
			return addr.getHostAddress();

		} catch (UnknownHostException uhe) {
			return "UnknownHost";
		}
	}

	private static String getHostRealIp() {
		/*
		 * REFER: http://stackoverflow.com/questions/9128019/inetaddress-getlocalhost-gethostaddress-returns-127-0-0-1-in-android
		 * */
		try {
			for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {
				NetworkInterface intf = en.nextElement();
				for (Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses(); enumIpAddr.hasMoreElements();) {
					InetAddress inetAddress = enumIpAddr.nextElement();
					if (!inetAddress.isLoopbackAddress() && !inetAddress.isLinkLocalAddress()
							&& inetAddress.isSiteLocalAddress()) {
						return inetAddress.getHostAddress().toString(); // return first when multi
					}

				}
			}
		} catch (SocketException ex) {
			throw new IllegalStateException("ifconfig error", ex);
		}
		return "UnknownHost";
	}
	
}
