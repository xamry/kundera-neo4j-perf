/**
 * 
 */
package com.impetus.benchmark.runner;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.impetus.benchmark.KunderaNeo4JClient;
import com.impetus.benchmark.utils.HibernateCRUDUtils;
import com.impetus.benchmark.utils.MailUtils;
import com.impetus.ycsb.entity.PerformanceNoInfo;
import com.yahoo.ycsb.db.Neo4JNativeClient;
import common.Logger;

/**
 * @author Amresh Singh
 * 
 */
public class Neo4JNumberGeneration
{
    private static HibernateCRUDUtils crudUtils;

    private static String[] clients = { "kundera-neo4j", "native-neo4j" };

    private static Map<String, String> clientClassMap = new HashMap<String, String>();

    private static Configuration config;

    private static String clientjarlocation;

    private static String ycsbJarLocation;

    private static Map<String, String> workloads = new HashMap<String, String>();

    private static String dbPropertyFileName;

    private static int noOfThreads;

    private static String runType;

    private static String workloadType;

    private static double releaseNo;

    private static Map<String, Double> timeTakenByClient = new HashMap<String, Double>();

    private static Logger logger = Logger.getLogger(Neo4JNumberGeneration.class);

    /**
     * @throws java.lang.Exception
     */

    public static void init(String propertyFileName) throws Exception
    {
        config = new PropertiesConfiguration(propertyFileName);

        // read property file to run command.
        workloadType = config.getString("workload.type", "workloada");
        runType = config.getString("run.type", "load");
        noOfThreads = config.getInt("threads", 1);
        ycsbJarLocation = config.getString("ycsbjar.location");
        clientjarlocation = config.getString("clientjar.location");
        dbPropertyFileName = propertyFileName;
        releaseNo = config.getDouble("release.no");
        // clients.
        clientClassMap.put(clients[0], KunderaNeo4JClient.class.getName());
        clientClassMap.put(clients[1], Neo4JNativeClient.class.getName());

        // workloads.
        workloads.put("workloada",
                "/home/impadmin/development/kundera-neo4j-perf/src/main/resources/workloads/workloada");
        workloads.put("workloadb",
                "/home/impadmin/development/kundera-neo4j-perf/src/main/resources/workloads/workloadb");
        workloads.put("workloadc",
                "/home/impadmin/development/kundera-neo4j-perf/src/main/resources/workloads/workloadc");
        workloads.put("workloadd",
                "/home/impadmin/development/kundera-neo4j-perf/src/main/resources/workloads/workloadd");
        workloads.put("workloade",
                "/home/impadmin/development/kundera-neo4j-perf/src/main/resources/workloads/workloade");
        workloads.put("workloadf",
                "/home/impadmin/development/kundera-neo4j-perf/src/main/resources/workloads/workloadf");

        crudUtils = new HibernateCRUDUtils();
    }

    public static void main(String[] args) throws Exception
    {
        init(args[0]);

        int runCounter = crudUtils.getMaxRunSequence(new Date(), runType);
        runCounter = runCounter + 1;

        // id column of performanceNoInfo table
        Date id = new Date();

        try
        {
            // run for all clients
            for (String client : clients)
            {
                if (clientjarlocation != null && ycsbJarLocation != null && client != null && runType != null)
                {
                    logger.info("running for client " + client);
                    Runtime runtime = Runtime.getRuntime();
                    String runCommand = getCommandString(client);

                    double totalTime = 0.0;
                    long noOfOperations = 0;

                    Process process = runtime.exec(runCommand);
                    InputStream is = process.getInputStream();
                    InputStreamReader isr = new InputStreamReader(is);
                    BufferedReader br = new BufferedReader(isr);
                    String line = null;
                    while ((line = br.readLine()) != null)
                    {
                        if (line.contains("RunTime"))
                        {
                            totalTime = Double.parseDouble(line.substring(line.lastIndexOf(", ") + 2));
                            logger.info("Total time taken " + totalTime);
                        }
                        if (line.contains("Operations") && noOfOperations == 0)
                        {
                            noOfOperations = Long.parseLong(line.substring(line.lastIndexOf(", ") + 2));
                            logger.info("Total no of oprations " + noOfOperations);
                        }
                        logger.info(line);
                    }

                    timeTakenByClient.put(client, totalTime);

                    PerformanceNoInfo info = new PerformanceNoInfo(id, releaseNo, client, runType, noOfThreads,
                            noOfOperations, totalTime, runCounter);
                    crudUtils.persistInfo(info);

                    // reset values
                    totalTime = 0.0;
                    noOfOperations = 0;
                }
            }
        }
        catch (Exception e)
        {
            logger.error(e);
        }
        Map<String, Double> delta = new HashMap<String, Double>();

        double kunderaMongoToNativeDelta = ((timeTakenByClient.get(clients[0]) - timeTakenByClient.get(clients[1]))
                / timeTakenByClient.get(clients[0]) * 100);
        delta.put("KunderaNeo4JToNativeDelta", kunderaMongoToNativeDelta);

        if (kunderaMongoToNativeDelta > 8.00)
        {
            MailUtils.sendMail(delta, runType, "neo4j");
        }
    }

    private static String getCommandString(String client)
    {
        StringBuilder command = new StringBuilder("java -cp ");
        command.append(clientjarlocation);
        command.append(":");
        command.append(ycsbJarLocation);
        command.append("* com.yahoo.ycsb.Client -db ");
        command.append(clientClassMap.get(client));
        command.append(" -s -P ");
        command.append(workloads.get(workloadType));
        command.append(" -P ");
        command.append(dbPropertyFileName);
        if (noOfThreads > 1)
        {
            command.append(" -threads ");
            command.append(noOfThreads);
        }
        command.append(" -");
        command.append(runType);

        return command.toString();
    }
}
