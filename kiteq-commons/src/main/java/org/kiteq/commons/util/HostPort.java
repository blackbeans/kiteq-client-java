package org.kiteq.commons.util;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

/**
 * @author gaofeihang
 * @since Feb 11, 2015
 */
public class HostPort implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private String host;
    private int port;

    public HostPort(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Ping host:port to check its alive.
     */
    public void ping() throws Exception {
        Socket s = null;
        try {
            s = new Socket();
            SocketAddress endpoint = new InetSocketAddress(host, port);
            s.connect(endpoint, 1000); // timeout 1000ms
        } catch (Exception e) {
            throw e;
        } finally {
            if (s != null)
                s.close();
        }
    }

    public boolean isAvailable() {
        try {
            ping();
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }

    /**
     * @param hostport
     *            192.168.1.1:6359
     */
    public static HostPort parse(String hostport) {
        String[] parts = hostport.split(":");
        return new HostPort(parts[0], Integer.valueOf(parts[1]));
    }
    
    /**
     * Do not modify it!
     */
    @Override
    public String toString() {
        return host + ":" + port;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + port;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        HostPort other = (HostPort) obj;
        if (host == null) {
            if (other.host != null)
                return false;
        } else if (!host.equals(other.host))
            return false;
        if (port != other.port)
            return false;
        return true;
    }

}
