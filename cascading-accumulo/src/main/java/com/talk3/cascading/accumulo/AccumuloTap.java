package com.talk3.cascading.accumulo;

import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;
import java.util.UUID;

public class AccumuloTap extends Tap<JobConf, RecordReader, OutputCollector>
        implements java.io.Serializable {

    private static final Logger LOG = LoggerFactory
            .getLogger(AccumuloTap.class);

    protected String accumuloTableName;
    private String resourceID;

    protected String accumuloInstanceName;
    protected transient String accumuloZookeeperQuorum;
    protected String accumuloUserName;
    private String accumuloPassword;
    protected transient AuthenticationToken accumuloAuthToken;
    private Connector accumuloConnector;
    protected Authorizations accumuloAuthorizations;
    protected int maxWriteThreads = 10;
    protected long maxMutationBufferSize = 10 * 1000 * 1000;
    protected int maxLatency = 10 * 1000;
    protected boolean offline;

    private String tapUUID;

    List<Pair<Text, Text>> columnFamilyColumnQualifierPairs = new LinkedList<Pair<Text, Text>>();
    protected String outputDirectory;

    /**
     * Constructor
     * @param accumuloConnectionString URI String
     * @param accumuloScheme AccumuloScheme
     * @throws Exception
     */
    public AccumuloTap(String accumuloConnectionString,
            AccumuloScheme accumuloScheme) throws Exception {
        this(AccumuloConnector.fromString(accumuloConnectionString),accumuloScheme);

    }

    /**
     * Constructor
     * @param accumuloConnectionString URI String
     * @param accumuloScheme AccumuloScheme
     * @param sinkMode SinkMode
     * @throws Exception
     */
    public AccumuloTap(String accumuloConnectionString,
            AccumuloScheme accumuloScheme, SinkMode sinkMode) throws Exception {
        this(AccumuloConnector.fromString(accumuloConnectionString), accumuloScheme,sinkMode);
    }

    // Constructor
    public AccumuloTap(AccumuloConnector conn,
                       AccumuloScheme accumuloScheme) throws Exception {
        super(accumuloScheme, SinkMode.UPDATE);
        setResourcesFromConnector(conn);
    }

    public AccumuloTap(AccumuloConnector conn,AccumuloScheme accumuloScheme, SinkMode sinkMode){
        super(accumuloScheme,sinkMode);
        setResourcesFromConnector(conn);
        
    }

    private void setResourcesFromConnector(AccumuloConnector conn){
        accumuloUserName=conn.getUser();
        accumuloPassword=conn.getPassword();
        accumuloAuthToken=conn.getAuthenticationToken();
        //accumuloInstance=conn.getInstance();
        accumuloConnector=conn.getConnector();
        accumuloInstanceName=conn.getInstanceName();
        accumuloAuthorizations=conn.getAuthorizations();
        accumuloTableName =conn.getTable();
        accumuloZookeeperQuorum=conn.getZookeepers();
        offline=conn.isOffline();
        maxMutationBufferSize=conn.getMaxBufferSize();
        maxWriteThreads=conn.getNumWriteThreads();
        tapUUID = UUID.randomUUID().toString();
        resourceID=conn.toString();
        outputDirectory=conn.getOutputDirectory();
    }

    Instance getInstance(){
        return new ZooKeeperInstance(accumuloInstanceName, accumuloZookeeperQuorum);
    }
	// Method initializeAccumuloConnector
    // Used only for table operations
    private void initializeAccumuloConnector() {

        if (accumuloConnector == null) {
            Instance accumuloInstance = new ZooKeeperInstance(accumuloInstanceName,
                    accumuloZookeeperQuorum);
            try {
                accumuloConnector = accumuloInstance.getConnector(
                        accumuloUserName, accumuloAuthToken);
            } catch (AccumuloException ae) {
                LOG.error("Error in AccumuloTap.initializeAccumuloConnector",
                        ae);
                ae.printStackTrace();
            } catch (AccumuloSecurityException ase) {
                LOG.error("Error in AccumuloTap.initializeAccumuloConnector",
                        ase);
                ase.printStackTrace();
            }

        }
    }
    // Method deleteTable deletes table if it exists
    private boolean deleteTable() {

        TableOperations ops = accumuloConnector.tableOperations();

        if (ops.exists(this.accumuloTableName)) {
            try {
                ops.delete(this.accumuloTableName);
            } catch (AccumuloException e) {
                LOG.error("Error in AccumuloTap.deleteTable", e);
                e.printStackTrace();
                return false;
            } catch (AccumuloSecurityException ase) {
                LOG.error("Error in AccumuloTap.deleteTable", ase);
                ase.printStackTrace();
                return false;
            } catch (TableNotFoundException tnfe) {
                LOG.error("Error in AccumuloTap.deleteTable", tnfe);
                tnfe.printStackTrace();
                return false;
            }
            return true;
        }
        return false;
    }


    // Method tableExists returns true if table exists
    private boolean tableExists() {
        initializeAccumuloConnector();
        TableOperations ops = accumuloConnector.tableOperations();
        if (ops.exists(this.accumuloTableName)) return true;
        else return false;
    }

    // Method createTable creates table and returns true if it succeeds
    private boolean createTable(boolean checkIfExists) {
        initializeAccumuloConnector();
        TableOperations ops = accumuloConnector.tableOperations();

        if (checkIfExists && ops.exists(this.accumuloTableName)) {
                return false;
        }
        else {
            try {
                ops.create(accumuloTableName);
            } catch (AccumuloException e) {
                LOG.error("Error in AccumuloTap.createTable", e);
                e.printStackTrace();
            } catch (AccumuloSecurityException e) {
                LOG.error("Error in AccumuloTap.createTable", e);
                e.printStackTrace();
            } catch (TableExistsException e) {
                LOG.error("Error in AccumuloTap.createTable", e);
                e.printStackTrace();
            }
            return true;
        }
    }

	// Method createTable creates table with splits and returns true if it
    // succeeds
    private boolean createTable(String splits) {

        initializeAccumuloConnector();
        TableOperations ops = accumuloConnector.tableOperations();
        String[] splitArray = splits.split(",");

        if (!ops.exists(this.accumuloTableName)) {
            createTable(false);
        }

        // Add splits
        TreeSet<Text> intialPartitions = new TreeSet<Text>();
        for (String split : splitArray) {
            intialPartitions.add(new Text(split.trim()));
        }

        try {
            accumuloConnector.tableOperations().addSplits(this.accumuloTableName,
                    intialPartitions);
        } catch (TableNotFoundException tnfe) {
            LOG.error("Error in AccumuloTap.createTable", tnfe);
            tnfe.printStackTrace();
        } catch (AccumuloException ae) {
            LOG.error("Error in AccumuloTap.createTable", ae);
            ae.printStackTrace();
        } catch (AccumuloSecurityException ase) {
            LOG.error("Error in AccumuloTap.createTable", ase);
            ase.printStackTrace();
        }

        return true;
    }

    // @Override method createResource creates the underlying resource.
    @Override
    public boolean createResource(JobConf conf) {
        String tableSplits;
        if (conf.get("TableSplits") == null) {
            tableSplits = "";
        } else {
            tableSplits = conf.get("TableSplits");
        }

        if (tableSplits.length() == 0) {
            return createTable(true);
        } else {
            return createTable(tableSplits);
        }
    }

	// @Override method getIdentifier returns a String representing the resource
    // this Tap instance represents.
    @Override
    public String getIdentifier() {
        return this.resourceID;
    }

	// @Override method getModifiedTime returns the date this resource was last
    // modified.
    @Override
    public long getModifiedTime(JobConf arg0) throws IOException {
        // TODO: This is a low priority item
        return 0;
    }

    // @Override public method equals
    @Override
    public boolean equals(Object otherObject) {
        if (this == otherObject) {
            return true;
        }
        if (!(otherObject instanceof AccumuloTap)) {
            return false;
        }
        if (!super.equals(otherObject)) {
            return false;
        }
        AccumuloTap otherTap = (AccumuloTap) otherObject;
        EqualsBuilder eb = new EqualsBuilder();
        eb.append(this.getUUID(), otherTap.getUUID());

        return eb.isEquals();
    }

    // @Override public method hashCode
    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder();
        hcb.append(getUUID());
        return hcb.toHashCode();
    }

	// @Override public method openForRead opens the resource represented by
    @Override
    public TupleEntryIterator openForRead(FlowProcess<JobConf> flowProcess,
            RecordReader recordReader) throws IOException {
        return new HadoopTupleEntrySchemeIterator(flowProcess, this, recordReader);
    }

	// @Override public method openForWrite opens the resource represented by
    // this tap instance for writing.
    @Override
    public TupleEntryCollector openForWrite(FlowProcess<JobConf> flowProcess,
            OutputCollector outputCollector) throws IOException {
        AccumuloCollector accumuloCollector = new AccumuloCollector(
                flowProcess, this);
        accumuloCollector.prepare();
        return accumuloCollector;
    }

    // @Override public method deleteResource deletes table if it exists
    @Override
    public boolean deleteResource(JobConf arg0) throws IOException {
        return deleteTable();
    }

    // @Override public method resourceExists checks if the table exists
    @Override
    public boolean resourceExists(JobConf arg0) throws IOException {
        return tableExists();
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> process, JobConf conf) {
        super.sinkConfInit(process, conf);
    }

    public String getUUID() {
        return tapUUID;
    }

}
