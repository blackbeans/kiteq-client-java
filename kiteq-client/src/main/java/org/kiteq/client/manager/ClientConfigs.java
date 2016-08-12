package org.kiteq.client.manager;

/**
 * luofucong at 2015-03-10.
 */
public final class ClientConfigs {

    public String groupId;

    public String secretKey;


    //预热时间
    private int warmingupSeconds = 10;

    public void setWarmingupSeconds(int warmingupSeconds) {

        this.warmingupSeconds = warmingupSeconds;
    }

    public int getWarmingupSeconds() {
        return warmingupSeconds;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClientConfigs that = (ClientConfigs) o;

        if (warmingupSeconds != that.warmingupSeconds) return false;
        if (groupId != null ? !groupId.equals(that.groupId) : that.groupId != null) return false;
        return secretKey != null ? secretKey.equals(that.secretKey) : that.secretKey == null;

    }

    @Override
    public int hashCode() {
        int result = groupId != null ? groupId.hashCode() : 0;
        result = 31 * result + (secretKey != null ? secretKey.hashCode() : 0);
        result = 31 * result + warmingupSeconds;
        return result;
    }

    @Override
    public String toString() {
        return "ClientConfigs{" +
                "groupId='" + groupId + '\'' +
                ", secretKey='" + secretKey + '\'' +
                ", warmingupSeconds=" + warmingupSeconds +
                '}';
    }
}
