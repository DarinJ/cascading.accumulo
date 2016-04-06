package com.talk3.cascading.accumulo;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapred.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapred.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapred.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.util.ConfiguratorBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("rawtypes")
public class AccumuloScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {

    private static final Logger LOG = LoggerFactory
            .getLogger(AccumuloScheme.class);

    private String rowKeyStart = "*";
    private String rowKeyEnd = "*/0";
    private transient List<Pair<Text, Text>> columnFamilyColumnQualifierPairs = new LinkedList<Pair<Text, Text>>();
    private String accumuloSchemeID = "";
    private String rowRegex = null;
    private String columnFamilyRegex = null;
    private String columnQualifierRegex = null;
    private String valueRegex = null;
    private Long maxAge = null;
    private Long minAge = null;
    private Fields accumuloSinkFields = new Fields("rowId","columnFamily","columnQualifier","columnVisibility",
            "timeStamp","value");
    private Fields accumuloSourceFields = new Fields("key","value");
    private transient List<Range> ranges= new LinkedList<Range>();
    private List<IteratorSetting> iteratorSettings= new LinkedList<IteratorSetting>();

    private boolean addRegexFilter;

    enum TapType {
        SOURCE, SINK
    }

    protected String schemeUUID;

    /**
     * Add a single range to the ranges
     * @param accumuloRange
     */
    public void addRange(Range accumuloRange) {
        if(accumuloRange!=null)
            ranges.add(accumuloRange);
    }
    /**
     * Add a single range to the ranges
     * @param accumuloRanges collection of Ranges
     */
    public void addRanges(Collection<Range> accumuloRanges) {
        if(accumuloRanges!=null)
            ranges.addAll(accumuloRanges);
    }
    /**
     * adds a collection of Iterator Settings
     * @param accumuloIteratorSetting
     */
    public void addIteratorSettings(Collection<IteratorSetting> accumuloIteratorSetting) {
        if(accumuloIteratorSetting!=null)
            this.iteratorSettings.addAll(accumuloIteratorSetting);
    }

    /**
     * adds an Iterator Setting
     * @param iteratorSettings
     */
    public void addIteratorSetting(IteratorSetting iteratorSettings){
        this.iteratorSettings.add(iteratorSettings);
    }

    /**
     * adds a column family/qualifier pair
     * @param p
     */
    public void addFetchColumn(Pair<Text,Text> p){
        columnFamilyColumnQualifierPairs.add(p);
    }

    /**
     * Adds a collection of columns
     * @param pairs
     */
    public void addFetchColumns(Collection<Pair<Text,Text>> pairs){
        columnFamilyColumnQualifierPairs.addAll(pairs);
    }

    /**
     * Basic constructor
     */
     public AccumuloScheme() {
        setSourceFields(accumuloSourceFields);
        setSinkFields(accumuloSinkFields);
        initializeUUID();

    }

    /**
     *
     * @param queryCriteria URI String
     * @throws IOException
     */
    public AccumuloScheme(String queryCriteria) throws IOException {
        setSourceFields(accumuloSourceFields);
        setSinkFields(accumuloSinkFields);
        initializeUUID();
        addQueryString(queryCriteria);
    }

    /**
     *
     * @param queryString URI String
     * @param accumuloIteratorSettings List of Iterators to be applied
     */
    public AccumuloScheme(String queryString, List<IteratorSetting> accumuloIteratorSettings) {
        addIteratorSettings(accumuloIteratorSettings);
        setSourceFields(accumuloSourceFields);
        setSinkFields(accumuloSinkFields);
        initializeUUID();
    }

    /**
     *
     * @param accumuloRanges Collection of Ranges
     * @param accumuloIteratorSettings Collection of Iterators
     */
    public AccumuloScheme(List<Range> accumuloRanges, List<IteratorSetting> accumuloIteratorSettings) {
        addRanges(accumuloRanges);
        addIteratorSettings(accumuloIteratorSettings);
        setSourceFields(accumuloSourceFields);
        setSinkFields(accumuloSinkFields);
        initializeUUID();
    }

    /**
     *
     * @param queryCriteria
     * @throws IOException
     */
    public void addQueryString(String queryCriteria) throws IOException{
        String rowKeyStart = "*";
        String rowKeyEnd = "*/0";
        try {
            initializeUUID();
            accumuloSchemeID = queryCriteria;

            String columns = "";

            if (queryCriteria.length() > 0) {

                String[] queryCriteriaParts = queryCriteria.split("\\&");

                if (queryCriteriaParts.length >= 1) {
                    for (String param : queryCriteria.split("&")) {
                        String[] pair = param.split("=");
                        if (pair[0].equals("columns")) {
                            columns = pair[1];
                        } else if (pair[0].equals("rowKeyRangeStart")) {
                            rowKeyStart = pair[1];
                        } else if (pair[0].equals("rowKeyRangeEnd")) {
                            rowKeyEnd = pair[1];
                        } else if (pair[0].equals("rowRegex")) {
                            addRegexFilter = true;
                            rowRegex = pair[1];
                        } else if (pair[0].equals("columnFamilyRegex")) {
                            addRegexFilter = true;
                            columnFamilyRegex = pair[1];
                        } else if (pair[0].equals("columnQualifierRegex")) {
                            addRegexFilter = true;
                            columnQualifierRegex = pair[1];
                        } else if (pair[0].equals("valueRegex")) {
                            addRegexFilter = true;
                            valueRegex = pair[1];
                        }
                    }
                    if (!rowKeyStart.equals("*") && !rowKeyEnd.equals("*/0") && ranges.size()==0) {
                        ranges.add(new Range(rowKeyStart,rowKeyEnd));
                    }
                    if (!columns.equals("")) {
                        for (String cfCq : columns.split(",")) {
                            if (cfCq.contains("|")) {
                                String[] cfCqParts = cfCq.split("\\|");
                                columnFamilyColumnQualifierPairs
                                        .add(new Pair<Text, Text>(new Text(cfCqParts[0]),
                                                new Text(cfCqParts[1])));
                            } else {
                                columnFamilyColumnQualifierPairs
                                        .add(new Pair<Text, Text>(new Text(cfCq),
                                                null));
                            }
                        }

                    }
                }
            }

        } catch (Exception e) {
            throw new IOException("Bad parameter; Format expected is: " +
                    "columns=colFam1|cq1,colFam1|cq2&rowKeyRangeStart="+
                    "X0001&rowKeyRangeEnd=X0005&rowRegex=*&columnFamilyRegex=&columnQualifierRegex=*&valueRegex=*");
        }

    }

    private void initializeUUID() {
        this.schemeUUID = UUID.randomUUID().toString();
    }

    @Override
    public Fields getSourceFields() {
        return accumuloSourceFields;
    }

    // @Override method sourcePrepare is used to initialize resources needed
    // during each call of source(cascading.flow.FlowProcess, SourceCall).
    // Place any initialized objects in the SourceContext so each instance will
    // remain threadsafe.
    @Override
    public void sourcePrepare(FlowProcess<JobConf> flowProcess,
            SourceCall<Object[], RecordReader> sourceCall) {
        Object[] pair = new Object[]{sourceCall.getInput().createKey(), sourceCall.getInput().createValue()};
        sourceCall.setContext(pair);
    }

    // @Override method sourceCleanup is used to destroy resources created by
    // sourcePrepare(cascading.flow.FlowProcess, SourceCall).
    @Override
    public void sourceCleanup(FlowProcess<JobConf> flowProcess,
            SourceCall<Object[], RecordReader> sourceCall) {
        sourceCall.setContext(null);
    }

    // @Override method source-
    // Method source will read a new "record" or value from
    // SourceCall.getInput() and populate
    // the available Tuple via SourceCall.getIncomingEntry() and return true on
    // success or false
    // if no more values available.
    @Override
    public boolean source(FlowProcess<JobConf> flowProcess,
            SourceCall<Object[], RecordReader> sourceCall) {
        try {
            Key key = (Key) sourceCall.getContext()[0];
            Value value = (Value) sourceCall.getContext()[1];

            boolean hasNext = sourceCall.getInput().next(key, value);

            if (!hasNext) {
                return false;
            }

            Tuple resultTuple = new Tuple();
            resultTuple.add(key);
            resultTuple.add(value); //really bad idea to make a string, screws things up serialized objects
            sourceCall.getIncomingEntry().setTuple(resultTuple);
            return true;
        } catch (Exception e) {
            LOG.error("Error in AccumuloScheme.source", e);
            e.printStackTrace();
        }

        return false;
    }


    // @Override method sourceInit initializes this instance as a source.
    // This method is executed client side as a means to provide necessary
    // configuration
    // parameters used by the underlying platform.
    @Override
    public void sourceConfInit(FlowProcess<JobConf> flowProcess,
            Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {

        conf.setInputFormat(AccumuloInputFormat.class);
        setSourceFields(accumuloSourceFields);
        AccumuloTap accumuloTap = (AccumuloTap) tap;

        if (!ConfiguratorBase.isConnectorInfoSet(
                AccumuloInputFormat.class, conf)) {
            try {
                AccumuloInputFormat.setConnectorInfo(conf,
                        accumuloTap.accumuloUserName,
                        accumuloTap.accumuloAuthToken);
                AccumuloInputFormat.setScanAuthorizations(conf,
                        accumuloTap.accumuloAuthorizations);
                AccumuloInputFormat.setInputTableName(conf,
                        accumuloTap.accumuloTableName);
                AccumuloInputFormat.setZooKeeperInstance(conf,
                        accumuloTap.accumuloInstanceName,
                        accumuloTap.accumuloZookeeperQuorum);
            } catch (AccumuloSecurityException ex) {
                LOG.error("Error in AccumuloScheme.sourceConfInit", ex);
            }
            if (columnFamilyColumnQualifierPairs.size() > 0) {
                AccumuloInputFormat.fetchColumns(conf, columnFamilyColumnQualifierPairs);
            }

            if (iteratorSettings.size() > 0) {
                for (IteratorSetting is : iteratorSettings)
                AccumuloInputFormat.addIterator(conf,is);
            }

            if (!rowKeyStart.equals("*") && !rowKeyEnd.equals("*/0")) {
                AccumuloInputFormat.setRanges(conf, Collections.singleton(new Range(rowKeyStart, rowKeyEnd)));
            }
            if (ranges.size()>0) {
                AccumuloInputFormat.setRanges(conf, ranges);
            }
            if (addRegexFilter) {
                IteratorSetting regex = new IteratorSetting(50, "regex", RegExFilter.class);
                RegExFilter.setRegexs(regex, this.rowRegex, this.columnFamilyRegex, this.columnQualifierRegex,
                        this.valueRegex, false);
                AccumuloInputFormat.addIterator(conf, regex);
            }

            if (maxAge != null) {
              IteratorSetting maxAgeFilter = new IteratorSetting(48, "maxAgeFilter",
                  AgeOffFilter.class);
              AgeOffFilter.setTTL(maxAgeFilter, maxAge);
              AccumuloInputFormat.addIterator(conf, maxAgeFilter);
            }

            if (minAge != null) {
              IteratorSetting minAgeFilter = new IteratorSetting(49, "minAgeFilter",
                  AgeOffFilter.class);
              AgeOffFilter.setTTL(minAgeFilter, minAge + 1);
              AgeOffFilter.setNegate(minAgeFilter, true);
              AccumuloInputFormat.addIterator(conf, minAgeFilter);
            }
            AccumuloInputFormat.setOfflineTableScan(conf, accumuloTap.offline);
        }
    }

    @Override
    public Fields getSinkFields() {
        return accumuloSinkFields;
    }

    // @Override method Method sinkInit initializes this instance as a sink.
    // This method is executed client side as a means to provide necessary
    // configuration
    // parameters used by the underlying platform.
    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess,
            Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {

        AccumuloTap accumuloTap = (AccumuloTap) tap;
        if (false == accumuloTap.offline) {

            conf.setOutputFormat(AccumuloOutputFormat.class);
            conf.setOutputKeyClass(NullWritable.class);
            conf.setOutputValueClass(Mutation.class);
            setSinkFields(accumuloSinkFields);

            if (false == ConfiguratorBase.isConnectorInfoSet(
                    AccumuloOutputFormat.class, conf)) {
                try {
                    BatchWriterConfig batchWriterConfig = new BatchWriterConfig();
                    batchWriterConfig.setMaxLatency(accumuloTap.maxLatency,
                            TimeUnit.MINUTES);
                    batchWriterConfig
                            .setMaxMemory(accumuloTap.maxMutationBufferSize);
                    batchWriterConfig
                            .setMaxWriteThreads(accumuloTap.maxWriteThreads);
                    batchWriterConfig.setTimeout(5, TimeUnit.MINUTES);

                    AccumuloOutputFormat.setConnectorInfo(conf,
                            accumuloTap.accumuloUserName,
                            accumuloTap.accumuloAuthToken);
                    AccumuloOutputFormat.setDefaultTableName(conf,
                            accumuloTap.accumuloTableName);
                    AccumuloOutputFormat.setZooKeeperInstance(conf,
                            accumuloTap.accumuloInstanceName,
                            accumuloTap.accumuloZookeeperQuorum);

                    AccumuloOutputFormat.setBatchWriterOptions(conf,
                            batchWriterConfig);
                } catch (AccumuloSecurityException ex) {
                    LOG.error("Error in AccumuloScheme.sinkConfInit", ex);
                }
            }
        }else{
            //We make RFiles
            conf.setOutputFormat(AccumuloFileOutputFormat.class);
            conf.setOutputKeyClass(Key.class);
            conf.setOutputValueClass(Value.class);
            AccumuloFileOutputFormat.setOutputPath(conf,new Path(accumuloTap.outputDirectory));
        }
    }
    // @Override method Method sink writes out the given Tuple found on
    // SinkCall.getOutgoingEntry()
    // to the SinkCall.getOutput().
    @Override
    public void sink(FlowProcess<JobConf> flowProcess,
            SinkCall<Object[], OutputCollector> sinkCall) throws IOException {

        TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
        OutputCollector outputCollector = sinkCall.getOutput();

        Tuple tuple = tupleEntry.selectTuple(this.accumuloSinkFields);
        if(!((AccumuloCollector) outputCollector).isOffline())
            outputCollector.collect(null, getMutations(tuple));
        else ((AccumuloCollector) outputCollector).collect(getKey(tuple),getValue(tuple));
    }

    public Key getKey(Tuple tuple){
        String rowId = tuple.getString(0);
        String columnFamily = tuple.getString(1);
        String columnQualifier = tuple.getString(2);
        ColumnVisibility columnVisibility = (ColumnVisibility) tuple.getObject(3);
        Long timeStamp = tuple.getLong(4);
        if(timeStamp!=null && timeStamp > 0)
            return new Key(rowId,columnFamily,columnQualifier,columnVisibility,timeStamp);
        else
            return new Key(rowId,columnFamily,columnQualifier,columnVisibility.toString());
    }

    private Value getValue(Tuple tuple){
        return (Value) tuple.getObject(5);
    }

    private Mutation getMutations(Tuple tuple) throws IOException {
        Mutation mut = new Mutation(tuple.getString(0));
        String columnFamily = tuple.getString(1);
        String columnQualifier = tuple.getString(2);
        ColumnVisibility columnVisibility= (ColumnVisibility) tuple.getObject(3);
        Long timeStamp = tuple.getLong(4);
        Value val = (Value) tuple.getObject(5);
        if (columnFamily == null)
            columnFamily = "";
        if (columnQualifier == null)
            columnQualifier = "";

        if (columnVisibility != null && timeStamp != null)
            mut.put(columnFamily, columnQualifier, columnVisibility, timeStamp.longValue(),  val);
        else if (timeStamp!=null)
            mut.put(columnFamily, columnQualifier, timeStamp.longValue(), val);
        else if (columnVisibility!=null)
            mut.put(columnFamily, columnQualifier, columnVisibility, val);
        else
            mut.put(columnFamily, columnQualifier, val);
        return mut;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof AccumuloScheme)) {
            return false;
        }
        if (!super.equals(other)) {
            return false;
        }

        AccumuloScheme otherScheme = (AccumuloScheme) other;
        EqualsBuilder eb = new EqualsBuilder();
        eb.append(this.getUUID(), otherScheme.getUUID());
        return eb.isEquals();

    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder();
        hcb.append(getUUID());
        return hcb.toHashCode();
    }

    public String getUUID() {
        return schemeUUID;
    }
}
