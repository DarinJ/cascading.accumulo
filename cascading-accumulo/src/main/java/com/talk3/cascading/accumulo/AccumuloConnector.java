/**
 * Class designed to hold all connection information for the Accumulo Tap/Scheme
 * follows a simple builder pattern for easy use.
 */
package com.talk3.cascading.accumulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AccumuloConnector extends Object{
    private static final Logger LOG = LoggerFactory
            .getLogger(AccumuloTap.class);

    public static final String MAXAUTHS="";
    private final String instanceName;
    private final String zookeepers;
    private final String table;
    private final String user;
    private final boolean createTable;
    private final String password;
    private final String auths;
    private final int numWriteThreads;
    private final int maxLatency;
    private final long maxBufferSize;
    private final boolean offline;
    private final String outputDirectory;
    private Instance instance=null;
    private Connector connector=null;
    private Authorizations authorizations=null;

    public String getInstanceName() {
        return instanceName;
    }

    public String getZookeepers() {
        return zookeepers;
    }

    public String getTable() {
        return table;
    }

    public boolean isCreateTable(){return createTable;}

    public String getOutputDirectory(){
        return outputDirectory;
        
    }
    
    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public AuthenticationToken getAuthenticationToken(){
        return new PasswordToken(password);
    }
    
    public String getAuths() {
        return auths;
    }

    public int getNumWriteThreads() {
        return numWriteThreads;
    }

    public boolean isOffline() {
        return offline;
    }

    public Instance getInstance(){
        if(instance==null)
                instance=new ZooKeeperInstance(instanceName,zookeepers);
        return instance;
    }

    public Connector getConnector(){
        if(instance==null)
            instance=getInstance();
        if(connector==null){
            try{
                instance.getConnector(getUser(),new PasswordToken(getPassword()));
            } catch (AccumuloException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            } catch (AccumuloSecurityException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            }
        }
        return connector;
    }

    public Authorizations getAuthorizations(){
        if(connector==null)
            connector=getConnector();
        if(authorizations==null) {
            if (auths.equals(MAXAUTHS)) {
                try {
                    authorizations = connector.securityOperations().getUserAuthorizations(user);
                } catch (AccumuloException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (AccumuloSecurityException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            else
                authorizations=new Authorizations(auths.split(","));
        }
        return authorizations;
    }
    public static AccumuloConnector fromString(String accumuloConnectionString) throws IOException{
        AccumuloConnector.AccumuloConnectorBuilder acBuilder=new AccumuloConnector.AccumuloConnectorBuilder();
        acBuilder.addAuths(AccumuloConnector.MAXAUTHS);
        try {
            if (!accumuloConnectionString.startsWith("accumulo://")) {
                try {
                    LOG.error("Bad connection string!  Expected format=accumulo://table1?instance=myinstance&user=root&password=secret&zookeepers=127.0.0.1:2181&auths=PRIVATE,PUBLIC");
                    throw new Exception("Bad connection string.");
                } catch (Exception e) {
                    LOG.error(
                            "Error in AccumuloTap.setResourceFromConnectionString",
                            e);
                    e.printStackTrace();
                }
            }

            String[] urlParts = accumuloConnectionString.split("\\?");
            if (urlParts.length > 1) {
                for (String param : urlParts[1].split("&")) {
                    String[] pair = param.split("=");
                    if (pair[0].equals("instance"))
                        acBuilder.addInstance(pair[1]);
                    else if (pair[0].equals("user"))
                        acBuilder.addUserName(pair[1]);
                    else if (pair[0].equals("password"))
                        acBuilder.addPassword(pair[1]);
                    else if (pair[0].equals("zookeepers"))
                        acBuilder.addZookeepers(pair[1]);
                    else if (pair[0].equals("write_buffer_size_bytes"))
                        acBuilder.setMaxBufferSize(Long.parseLong(pair[1]));
                    else if (pair[0].equals("write_threads"))
                        acBuilder.setNumWriteTheads(Integer.parseInt(pair[1]));
                    else if (pair[0].equals("write_latency_ms"))
                        acBuilder.setMaxLatency(Integer.parseInt(pair[1]));
                    else if (pair[0].equals("auths"))
                        acBuilder.addAuths(pair[1]);
                    else if (pair[0].equals("offline")&&pair[1].equals("false"))
                        acBuilder.setOffline(false);
                    else if (pair[0].equals("create_table")&&pair[1].equals("true"))
                        acBuilder.setCreateTable(true);
                }
            }
            String[] parts = urlParts[0].split("/+");
            if (parts.length > 0) {
                acBuilder.addTable(parts[1]);
            }
        }catch (Exception e) {
            throw new IOException("Bad parameter; Format expected is: accumulo://table1?instance=myinstance&user=root&password=secret&zookeepers=CSVListofZooserver:portNum&auths=PRIVATE,PUBLIC&write_threads=3");
        }
        return acBuilder.build();
    }
    /**
     * Normally would make private to enforce builder but legacy code says no, best to use AccumuloConnectorBuilder
     * @param instance
     * @param zookeepers
     * @param table
     * @param user
     * @param password
     * @param auths
     * @param numWriteThreads
     * @param offline
     */
    public AccumuloConnector(String instance, String zookeepers, String table, boolean createTable, String user,
                             String password, String auths, int numWriteThreads, int maxLatency,
                             long maxBufferSize, boolean offline,String outputDirectory) {
        this.instanceName = instance;
        this.zookeepers = zookeepers;
        this.table = table;
        this.createTable=createTable;
        this.user = user;
        this.password = password;
        this.auths = auths;
        this.numWriteThreads = numWriteThreads;
        this.maxBufferSize=maxBufferSize;
        this.maxLatency=maxLatency;
        this.offline = offline;
        this.outputDirectory=outputDirectory;
    }

    /**
     * Normally would make private to enforce builder but legacy code says no
     * @param instance
     * @param zookeepers
     * @param table
     * @param user
     * @param password
     * @param auths
     * @param numWriteThreads
     * @param offline
     */
    public AccumuloConnector(String instance, String zookeepers, String table, String user, String password,
                             String auths, int numWriteThreads, boolean offline) {
        this.instanceName = instance;
        this.zookeepers = zookeepers;
        this.table = table;
        this.createTable=true;
        this.user = user;
        this.password = password;
        this.auths = auths;
        this.numWriteThreads = numWriteThreads;
        this.offline = offline;
        this.maxBufferSize=1000*1000*3;
        this.maxLatency=1000;
        this.outputDirectory="output";
    }


    /**
     * Legacy code says no
     * @param instance
     * @param zookeepers
     * @param table
     * @param user
     * @param password
     * @param auths
     * @param offline
     */
    @Deprecated
    public AccumuloConnector(String instance, String zookeepers, String table, String user, String password,
                             String auths, boolean offline) {
        this.instanceName = instance;
        this.zookeepers = zookeepers;
        this.table = table;
        this.createTable=true;
        this.user = user;
        this.password = password;
        this.auths = auths;
        this.numWriteThreads = 10;
        this.maxBufferSize=1000*1000*10;
        this.maxLatency=1000;
        this.offline = offline;
        this.outputDirectory="output";
    }

    @Override
    public String toString() {
        return "accumulo://" + table +
                "?instance=" + instanceName +
                "&user=" + user +
                "&password=" + password +
                "&zookeepers=" + zookeepers +
                "&auths=" + auths +
                "&write_threads=" + Integer.toString(numWriteThreads) +
                "&max_latency="+Integer.toString(maxLatency)+
                "&write_buffer_size="+Long.toString(maxBufferSize)+
                "&offline=" + Boolean.toString(offline);
    }

    /**
     * Your basic builder, default values are assign to username, password, auths, numWriteThreads, and offline
     */
    public static class AccumuloConnectorBuilder {
        private String bldrInstance;
        private String bldrZookeepers;
        private String bldrTable;
        private String bldrUser="root";
        private String bldrPassword="secret";
        private String bldrAuths="PUBLIC";
        private int bldrNumWriteThreads=10;
        private int bldrMaxLatency=1000;
        private long bldrMaxBufferSize=1000;
        private boolean bldrOffline=false;
        private boolean bldrCreateTable=false;
        private String bldrOutputDirectory="output";
        /**
         * Most basic constructor
         * defaults to zookeeper=127.0.0.1
         *             instance=accumulo
         *             table=table
         */
        public AccumuloConnectorBuilder(){
            this.bldrInstance="accumulo";
            this.bldrZookeepers="127.0.0.1:2181";
            this.bldrTable="table";
        }

        /**
         * Constructor where you pass the instance, zookeepers, and tablename
         * @param instance
         * @param zookeepers
         * @param table
         */
        public AccumuloConnectorBuilder(String instance, String zookeepers, String table){
            this.bldrInstance=instance;
            this.bldrZookeepers=zookeepers;
            this.bldrTable=table;
        }

        /**
         * Add username, otherwise root
         * @param user
         */
        public AccumuloConnectorBuilder addUserName(String user){
            bldrUser=user;
            return this;
        }

        /**
         * Add password, otherwise secret
         * @param password
         * @return
         */
        public AccumuloConnectorBuilder addPassword(String password){
            bldrPassword=password;
            return this;
        }

        /**
         * Set the zookeepr if not done so by constructor
         * @param zookeepers
         * @return
         */
        public AccumuloConnectorBuilder addZookeepers(String zookeepers){
            bldrZookeepers=zookeepers;
            return this;
        }

        /**
         * Set the instance if not done so by constructor
         * @param instance
         * @return
         */
        public AccumuloConnectorBuilder addInstance(String instance){
            bldrInstance=instance;
            return this;
        }
        public AccumuloConnectorBuilder addTable(String table){
            bldrTable=table;
            return this;
        }
        public AccumuloConnectorBuilder addTable(String table, Boolean createTable){
            bldrCreateTable=createTable;
            bldrTable=table;
            return this;
        }

        /**
         * add auths. otherwise PUBLIC
         * use AccumuloConnector.MAXAUTHS for maximum authorizations for a user
         * @param auths
         * @return
         */
        public AccumuloConnectorBuilder addAuths(String auths){
            bldrAuths=auths;
            return this;
        }
        /**Set numWriteThreads otherwise 3
         * @param numWriteThreads
         */
        public AccumuloConnectorBuilder setNumWriteTheads(int numWriteThreads){
            bldrNumWriteThreads=numWriteThreads;
            return this;
        }

        /**
         * Sets Maximum Write Buffer size
         * @param bufferSize
         * @return
         */
        public AccumuloConnectorBuilder setMaxBufferSize(long bufferSize){
            bldrMaxBufferSize=bufferSize;
            return this;
        }
        public AccumuloConnectorBuilder setMaxLatency(int maxLatency){
            bldrMaxLatency=maxLatency;
            return this;
        }
        /**
         * Sets offine to true, which is by default false
         */
        public AccumuloConnectorBuilder setOffline(){
            bldrOffline=true;
            return this;
        }

        /**
         * Sets offine/online nice for checking commandline options.
         * default false
         */
        public AccumuloConnectorBuilder setOffline(boolean bool){
            bldrOffline=bool;
            return this;
        }
        public AccumuloConnectorBuilder setCreateTable(boolean createTable){
            bldrCreateTable=createTable;
            return this;
        }

        /**
         * Sets output Directory for AccumuloFileOutputFormat.
         * Only used if offline is set to true, default "output" 
         * @param outputDirectory
         * @return
         */
        public AccumuloConnectorBuilder setBldrOutputDirectory(String outputDirectory){
            bldrOutputDirectory = outputDirectory;
            return this;
        }
        /**
         *
         * @return AccumuloConnector object
         */
        public AccumuloConnector build(){
            return new AccumuloConnector(bldrInstance,bldrZookeepers, bldrTable, bldrCreateTable,bldrUser, bldrPassword, bldrAuths, bldrNumWriteThreads, bldrMaxLatency, bldrMaxBufferSize,bldrOffline,bldrOutputDirectory);
        }
    }

}
