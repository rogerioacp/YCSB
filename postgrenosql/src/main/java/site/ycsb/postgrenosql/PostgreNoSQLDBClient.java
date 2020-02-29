/*
 * Copyright 2017 YCSB Contributors. All Rights Reserved.
 *
 * CODE IS BASED ON the jdbc-binding JdbcDBClient class.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.postgrenosql;

import site.ycsb.*;
import org.json.simple.JSONObject;
import org.postgresql.Driver;
import org.postgresql.util.PGobject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;



import java.util.*;

import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY;
import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY_DEFAULT;

/**
 * PostgreNoSQL client for YCSB framework.
 */
public class PostgreNoSQLDBClient extends DB {
  private static final Logger LOG = LoggerFactory.getLogger(PostgreNoSQLDBClient.class);



  enum PostgresMode{
    BASELINE(0), OIS(1), PLAINTEXT(2);

    private final int internalType;

    PostgresMode(int type) {
      internalType = type;
    }

    public int getInternalType() {
      return internalType;
    }

    int getHashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + internalType;
      return result;
    }
  }

  /** Count the number of times initialized to teardown on the last. */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /** Cache for already prepared statements. */
  private static ConcurrentMap<StatementType, PreparedStatement> cachedStatements;

  /** The driver to get the connection to postgresql. */
  private static Driver postgrenosqlDriver;

  /** The connection to the database. */
  private static Connection connection;

  /** The class to use as the jdbc driver. */
  public static final String DRIVER_CLASS = "db.driver";

  /** The URL to connect to the database. */
  public static final String CONNECTION_URL = "postgrenosql.url";

  /** The user name to use to connect to the database. */
  public static final String CONNECTION_USER = "postgrenosql.user";

  /** The password to use for establishing the connection. */
  public static final String CONNECTION_PASSWD = "postgrenosql.passwd";

  /** The JDBC connection auto-commit property for the driver. */
  public static final String JDBC_AUTO_COMMIT = "postgrenosql.autocommit";

  /** The primary key in the user table. */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  /** The field name prefix in the table. */
  public static final String COLUMN_NAME = "YCSB_VALUE";

  private static final String DEFAULT_PROP = "";

  private static final String ZERO_PADDING_PROPERTY = "zeropadding";

  /** Postgres execution mode. */
  public static final String EXECUTION_MODE = "postgrenosql.execution";

  private PostgresMode pgmode;

  private int zeropadding;

  /** Returns parsed boolean value from the properties if set, otherwise returns defaultVal. */
  private static boolean getBoolProperty(Properties props, String key, boolean defaultVal) {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      return Boolean.parseBoolean(valueStr);
    }
    return defaultVal;
  }

  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();
    synchronized (PostgreNoSQLDBClient.class) {
      if (postgrenosqlDriver != null) {
        return;
      }

      Properties props = getProperties();
      String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
      String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
      String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
      String pgexec = props.getProperty(EXECUTION_MODE, PostgresMode.PLAINTEXT.name());
      boolean autoCommit = getBoolProperty(props, JDBC_AUTO_COMMIT, true);
      zeropadding =
          Integer.parseInt(props.getProperty(ZERO_PADDING_PROPERTY, "1"));

      try {
        Properties tmpProps = new Properties();
        tmpProps.setProperty("user", user);
        tmpProps.setProperty("password", passwd);

        cachedStatements = new ConcurrentHashMap<>();

        postgrenosqlDriver = new Driver();
        connection = postgrenosqlDriver.connect(urls, tmpProps);
        connection.setAutoCommit(autoCommit);

      } catch (Exception e) {
        LOG.error("Error during initialization: " + e);
      }
      pgmode = PostgresMode.valueOf(pgexec);

      if(pgmode != PostgresMode.PLAINTEXT){
        executeStatement("BEGIN");
        executeStatement(createOpenEnclaveStatement());
        String initSoe = createInitSoeStatement(pgmode.getInternalType());
        System.err.println("Init soe " + initSoe);
        executeStatement(initSoe);
        System.err.println("Loading oblivious tables.");
        executeStatement(createLoadBlocksStatement());
        System.err.println("Load complete");
        String tableName = props.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);
        executeStatement(createDeclareCursorStatement(tableName));
      }

    }
  }

  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      try {
        //Close transaction

        cachedStatements.clear();
        if(pgmode != PostgresMode.PLAINTEXT){
          executeStatement("CLOSE tcursor");
          executeStatement("select close_enclave()");
        }
        if (!connection.getAutoCommit() || pgmode != PostgresMode.PLAINTEXT){
          connection.commit();
        }
        connection.close();
      } catch (SQLException e) {
        System.err.println("Error in cleanup execution. " + e);
      }
      postgrenosqlDriver = null;
    }
  }


 // @Override
  /*public Status read(String tableName, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.READ, tableName, fields);
      PreparedStatement readStatement = cachedStatements.get(type);
      if (readStatement == null) {
        readStatement = createAndCacheReadStatement(type);
      }
      readStatement.setString(1, key);
      ResultSet resultSet = readStatement.executeQuery();
      if (!resultSet.next()) {
        resultSet.close();
        return  Status.NOT_FOUND;
      }

      if (result != null) {
        if (fields == null){
          while (resultSet.next()){
            String field = resultSet.getString(2);
            String value = resultSet.getString(3);
            result.put(field, new StringByteIterator(value));
          }
        } else {
          for (String field : fields) {
            String value = resultSet.getString(field);
            result.put(field, new StringByteIterator(value));
          }
        }
      }
      resultSet.close();
      return Status.OK;

    } catch (SQLException e) {
      LOG.error("Error in processing read of table " + tableName + ": " + e);
      return Status.ERROR;
    }
  }*/

  public Status read(String tableName, String key, Set<String> fields, Map<String, ByteIterator> result){
    String fetchNextStatment = "FETCH NEXT IN tcursor";
    try{
      ResultSet resultSet;
      Statement query = connection.createStatement();

      if(this.pgmode != PostgresMode.PLAINTEXT) {
        executeStatement(createSetNextTermStatement(trimKey(key)));
        resultSet = query.executeQuery(fetchNextStatment);
      }else{
        resultSet = query.executeQuery(createPlaintextReadStatement(trimKey(key)));
      }

      if (!resultSet.next()) {
        resultSet.close();
        //return  Status.NOT_FOUND;
        return Status.OK;
      }
      if (result != null) {
        if (fields == null){
          String field;
          String value;
          do{
            field = resultSet.getString(1);
            value = resultSet.getString(2);
            result.put(field, new StringByteIterator(value));
          }while (resultSet.next());

        } else {
          for (String field : fields) {
            String value = resultSet.getString(field);
            result.put(field, new StringByteIterator(value));
          }
        }
      }

      resultSet.close();
      return Status.OK;


    }catch (SQLException e) {
      LOG.error("Error in processing read of table " + tableName + ": " + e);
      return Status.ERROR;
    }
  }
  protected String buildKeyName(int zpad, long keynum) {

    String value = Long.toString(keynum);
    int fill = zpad - value.length();
    String prekey = "";
    for (int i = 0; i < fill; i++) {
      prekey += '0';
    }
    return prekey + value;
  }


  private HashMap<String, ByteIterator> oblivScan(String tableName, String key, int recordcount,
                                                  Set<String> fields) throws SQLException {
    ResultSet resultSet;
    String fetchNextStatment = "FETCH NEXT IN tcursor";
    String field;
    String tkey;
    int nkey = 0;
    boolean hasMore = false;
    HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
    Statement query = connection.createStatement();
    tkey = trimKey(key);
    nkey = Integer.parseInt(tkey);
    //System.out.println("--- requested nkey " + nkey + " with recordcount " + recordcount);
    //key = "user000000000";

    for(int i = 0; i < recordcount; i++){
      tkey = buildKeyName(zeropadding, nkey);
      //System.out.println("request key " + tkey + " with zeropadding "+ zeropadding);
      executeStatement(createSetNextTermStatement(tkey));
      resultSet = query.executeQuery(fetchNextStatment);
      if(resultSet.next()){
        //field = resultSet.getString(1).trim();
        //System.out.println("Returned field is "+field);
        resultSet.close();
      }else{
        resultSet.close();
        System.out.println("Error in oblivScan");
        System.exit(-1);
      }
      nkey +=1;
      //System.out.println("Incremented key "+nkey);

    }

    return values;
  }

  @Override
  public Status scan(String tableName, String startKey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {

    try {
      if(this.pgmode != PostgresMode.PLAINTEXT){

        HashMap<String, ByteIterator> res = oblivScan(tableName, startKey, recordcount, fields);
        result.add(res);
        return Status.OK;
      }else{
        //System.out.println("Scan key is "+startKey);
        StatementType type = new StatementType(StatementType.Type.SCAN, tableName, fields);
        PreparedStatement scanStatement = cachedStatements.get(type);
        if (scanStatement == null) {
          scanStatement = createAndCacheScanStatement(type);
          //System.out.println("Scan statement is "+ scanStatement.toString());
        }
        scanStatement.setString(1, trimKey(startKey));
        //scanStatement.setInt(2, recordcount);
        ResultSet resultSet = scanStatement.executeQuery();
        //System.out.println("recordcount " + recordcount);
        //System.out.println("Result set size " + resultSet);
        for (int i = 0; i < recordcount && resultSet.next(); i++) {
        //while(resultSet.next()){
          //System.out.println("record  " + resultSet.getString(1));
          //System.out.println("fields is "+fields);
          if (result != null && fields != null) {
            HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
            for (String field : fields) {
              String value = resultSet.getString(field);
             // System.out.println("Plaintext scan field is" + field + " and value is " + value);
              values.put(field, new StringByteIterator(value));
            }

            result.add(values);
          }
        }

        resultSet.close();
        return Status.OK;
      }
    } catch (SQLException e) {
      LOG.error("Error in processing scan of table: " + tableName + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String tableName, String key, Map<String, ByteIterator> values) {
    try{
      StatementType type = new StatementType(StatementType.Type.UPDATE, tableName, null);
      PreparedStatement updateStatement = cachedStatements.get(type);
      if (updateStatement == null) {
        updateStatement = createAndCacheUpdateStatement(type);
      }

      JSONObject jsonObject = new JSONObject();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        jsonObject.put(entry.getKey(), entry.getValue().toString());
      }

      PGobject object = new PGobject();
      object.setType("jsonb");
      object.setValue(jsonObject.toJSONString());

      updateStatement.setObject(1, object);
      updateStatement.setString(2, key);

      int result = updateStatement.executeUpdate();
      if (result == 1) {
        return Status.OK;
      }
      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      LOG.error("Error in processing update to table: " + tableName + e);
      return Status.ERROR;
    }
  }


  @Override
  public Status insert(String tableName, String key, Map<String, ByteIterator> values) {
    try{
      StatementType type = new StatementType(StatementType.Type.INSERT, tableName, null);
      PreparedStatement insertStatement = cachedStatements.get(type);
      if (insertStatement == null) {
        insertStatement = createAndCacheInsertStatement(type);
      }

      JSONObject jsonObject = new JSONObject();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        jsonObject.put(entry.getKey(), entry.getValue().toString());
      }

      /*PGobject object = new PGobject();
      object.setType("jsonb");
      object.setValue(jsonObject.toJSONString());*/
      //String logmsg = "Trimmed key is " + trimKey(key) + " and json object has size ";

      System.out.println("message of size " +jsonObject.toJSONString().length());
      String res = jsonObject.toJSONString();
      while(res.length() < 10000){
        res += res;
      }
      /*if(jsonObject.toJSONString().length() > 1300){
        insertStatement.setObject(2, jsonObject.toJSONString().substring(0, 1300));
      }else{
        insertStatement.setObject(2, jsonObject.toJSONString());
      }*/
      System.out.println("Message Size is "+res.length());
      insertStatement.setObject(2, res.substring(0, 8000));

      //System.out.println("Key is " + key);
      insertStatement.setString(1, trimKey(key));
      int result = insertStatement.executeUpdate();
      if (result == 1) {
        return Status.OK;
      }

      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      LOG.error("Error in processing insert to table: " + tableName + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String tableName, String key) {
    try{
      StatementType type = new StatementType(StatementType.Type.DELETE, tableName, null);
      PreparedStatement deleteStatement = cachedStatements.get(type);
      if (deleteStatement == null) {
        deleteStatement = createAndCacheDeleteStatement(type);
      }
      deleteStatement.setString(1, key);

      int result = deleteStatement.executeUpdate();
      if (result == 1){
        return Status.OK;
      }

      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      LOG.error("Error in processing delete to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  private PreparedStatement createAndCacheReadStatement(StatementType readType)
      throws SQLException{
    PreparedStatement readStatement = connection.prepareStatement(createReadStatement(readType));
    PreparedStatement statement = cachedStatements.putIfAbsent(readType, readStatement);
    if (statement == null) {
      return readStatement;
    }
    return statement;
  }

  private String createReadStatement(StatementType readType){
    StringBuilder read = new StringBuilder("SELECT " + PRIMARY_KEY + " AS " + PRIMARY_KEY);

    if (readType.getFields() == null) {
      read.append(", (jsonb_each_text(" + COLUMN_NAME + ")).*");
    } else {
      for (String field:readType.getFields()){
        read.append(", " + COLUMN_NAME + "->>'" + field + "' AS " + field);
      }
    }

    read.append(" FROM " + readType.getTableName());
    read.append(" WHERE ");
    read.append(PRIMARY_KEY);
    read.append(" = ");
    read.append("?");
    return read.toString();
  }

  private PreparedStatement createAndCacheScanStatement(StatementType scanType)
      throws SQLException{
    PreparedStatement scanStatement = connection.prepareStatement(createScanStatement(scanType));
    PreparedStatement statement = cachedStatements.putIfAbsent(scanType, scanStatement);
    if (statement == null) {
      return scanStatement;
    }
    return statement;
  }

  private String createScanStatement(StatementType scanType){
    StringBuilder scan = new StringBuilder("SELECT " + PRIMARY_KEY + " AS " + PRIMARY_KEY);
    if (scanType.getFields() != null){
      for (String field:scanType.getFields()){
        scan.append(", " + COLUMN_NAME + "->>'" + field + "' AS " + field);
      }
    }
    scan.append(" FROM " + scanType.getTableName());
    scan.append(" WHERE ");
    scan.append(PRIMARY_KEY);
    scan.append(" >= ?");
    /*scan.append(" ORDER BY ");
    scan.append(PRIMARY_KEY);
    scan.append(" LIMIT ?");*/

    return scan.toString();
  }

  public PreparedStatement createAndCacheUpdateStatement(StatementType updateType)
      throws SQLException{
    PreparedStatement updateStatement = connection.prepareStatement(createUpdateStatement(updateType));
    PreparedStatement statement = cachedStatements.putIfAbsent(updateType, updateStatement);
    if (statement == null) {
      return updateStatement;
    }
    return statement;
  }

  private String createUpdateStatement(StatementType updateType){
    StringBuilder update = new StringBuilder("UPDATE ");
    update.append(updateType.getTableName());
    update.append(" SET ");
    update.append(COLUMN_NAME + " = " + COLUMN_NAME);
    update.append(" || ? ");
    update.append(" WHERE ");
    update.append(PRIMARY_KEY);
    update.append(" = ?");
    return update.toString();
  }

  private PreparedStatement createAndCacheInsertStatement(StatementType insertType)
      throws SQLException{
    PreparedStatement insertStatement = connection.prepareStatement(createInsertStatement(insertType));
    PreparedStatement statement = cachedStatements.putIfAbsent(insertType, insertStatement);
    if (statement == null) {
      return insertStatement;
    }
    return statement;
  }

  private String createInsertStatement(StatementType insertType){
    StringBuilder insert = new StringBuilder("INSERT INTO ");
    insert.append(insertType.getTableName());
    insert.append(" (" + PRIMARY_KEY + "," + COLUMN_NAME + ")");
    insert.append(" VALUES(?,?)");
    return insert.toString();
  }

  private PreparedStatement createAndCacheDeleteStatement(StatementType deleteType)
      throws SQLException{
    PreparedStatement deleteStatement = connection.prepareStatement(createDeleteStatement(deleteType));
    PreparedStatement statement = cachedStatements.putIfAbsent(deleteType, deleteStatement);
    if (statement == null) {
      return deleteStatement;
    }
    return statement;
  }

  private String createDeleteStatement(StatementType deleteType){
    StringBuilder delete = new StringBuilder("DELETE FROM ");
    delete.append(deleteType.getTableName());
    delete.append(" WHERE ");
    delete.append(PRIMARY_KEY);
    delete.append(" = ?");
    return delete.toString();
  }


  /* Oblivpg_fdw staments*/

  private String createOpenEnclaveStatement(){
    return "select open_enclave()";
  }

  private String createInitSoeStatement(int mode){
    StringBuilder query = new StringBuilder();
    query.append("select init_soe(");
    query.append(mode);
    query.append(", CAST( get_ftw_oid() as INTEGER), 1, CAST (get_original_index_oid() as INTEGER))");
    return query.toString();
  }

  private String createLoadBlocksStatement(){
    return "select load_blocks(CAST (get_original_index_oid() as INTEGER), CAST (get_original_heap_oid() as INTEGER))";
  }

  private String createCloseEnclaveStatement(){
    return "select close_enclave()";
  }

  private String createDeclareCursorStatement(String tableName){
    return "declare tcursor CURSOR FOR select YCSB_KEY," + COLUMN_NAME + " from ftw_" + tableName;
  }
  private String createDeclareCursorStatementBase(String tableName, String key){
    return "declare tcursor CURSOR FOR select YCSB_KEY," + COLUMN_NAME + " from " +
            tableName + " where YCSB_KEY='"+key+"'";

  }

  private String createPlaintextReadStatement(String key){
    return "select YCSB_KEY,YCSB_VALUE from usertable where YCSB_KEY='"+key+"'";
  }
  private String createSetNextTermStatement(String key){
    return "select set_nextterm('"+key +"')";
  }


  private Status executeStatement(String statement){
    try{

      boolean result;
      Statement query = connection.createStatement();
      result = query.execute(statement);

      if (result){
        return Status.OK;
      }

      return Status.UNEXPECTED_STATE;
    }catch(SQLException ex){
      LOG.error(ex.getMessage());
      return Status.ERROR;

    }
  }

  private String trimKey(String key){
    return key.substring(4, 13);
  }
}
