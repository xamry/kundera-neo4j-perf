/**
 * 
 */
package com.impetus.benchmark;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import com.impetus.ycsb.entity.Neo4JUser;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import common.Logger;

/**
 * Kundera Neo4J Client for YCSB Framework
 * @author Amresh Singh
 * 
 */
public class KunderaNeo4JClient extends DB
{

    private static Logger logger = Logger.getLogger(KunderaNeo4JClient.class);

    static Random random = new Random();

    private static final int Ok = 0;

    private static final int Error = -1;

    static Map<String, String> config = new HashMap<String, String>(); 
    
    static
    {
        config.put("node_auto_indexing", "true");
        config.put("node_keys_indexable", "USER_ID");
    }           
    
    private static EntityManagerFactory emf = Persistence.createEntityManagerFactory("kundera_neo4j_pu", config);
    private EntityManager em = emf.createEntityManager();
    

    private int j;

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    public void init() throws DBException
    {       
        em.getTransaction().begin();
        j = 1;   
        
    }

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one
     * DB instance per client thread.
     */
    public void cleanup() throws DBException
    {
        //em.close();
        //emf.close();
        em.getTransaction().commit();
    }
    
    
    
    /**
     * Insert a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to insert.
     * @param values
     *            A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int insert(String table, String key, HashMap<String, ByteIterator> values)
    {
        try
        {
            final Neo4JUser u = new Neo4JUser(key, getString(key, "Amresh"), getString(key, "31"),
                    getString(key, "New Street"));
            
            em.persist(u);
            
            j++;
            if (j % 5000 == 0)
            {
                em.getTransaction().commit();
                em.clear();
                em.getTransaction().begin();
            }
            return Ok;
        }
        catch (Exception e)
        {
            logger.error(e);
            em.getTransaction().rollback();
            return Error;
        }
    }

    /**
     * Read a record from the database. Each field/value pair from the result
     * will be stored in a HashMap.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to read.
     * @param fields
     *            The list of fields to read, or null for all of them
     * @param result
     *            A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result)
    {
        try
        {
            final Object o = em.find(Neo4JUser.class, key);
            assert o != null;
            j++;
            if (j % 5000 == 0)
            {
                em.clear();
            }
            return Ok;
        }
        catch (Exception e)
        {
            logger.error(e);
            return Error;
        }
    }

    /**
     * Perform a range scan for a set of records in the database. Each
     * field/value pair from the result will be stored in a HashMap.
     * 
     * @param table
     *            The name of the table
     * @param startkey
     *            The record key of the first record to read.
     * @param recordcount
     *            The number of records to read
     * @param fields
     *            The list of fields to read, or null for all of them
     * @param result
     *            A Vector of HashMaps, where each HashMap is a set field/value
     *            pairs for one record
     * @return Zero on success, a non-zero error code on error
     */
    public int scan(String table, String startkey, int recordcount, Set<String> fields,
            Vector<HashMap<String, ByteIterator>> result)
    {
        return Ok;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key, overwriting any existing values with the same field name.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to write.
     * @param values
     *            A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int update(String table, String key, HashMap<String, ByteIterator> values)
    {
        return Error;
    }

    

    /**
     * Delete a record from the database.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error
     */
    public int delete(String table, String key)
    {
        try
        {
            em.remove(em.find(Neo4JUser.class, key));
            return Ok;
        }
        catch (Exception e)
        {
            logger.error(e);
            return Error;
        }
    }

    private String getString(String key, String value)
    {
        StringBuilder builder = new StringBuilder(key);
        builder.append(value);
        return builder.toString();
    }
    
    
    public static void main(String[] args)
    {
        try
        {
            new KunderaNeo4JClient().init();
        }
        catch (DBException e)
        {
            e.printStackTrace();
        }
        
        new KunderaNeo4JClient().insert("USER", "1", new HashMap());
        
    }
}
