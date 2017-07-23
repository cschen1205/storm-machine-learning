package com.github.cschen1205.storm.regressions;

import java.util.List;

import com.github.pmerienne.trident.ml.regression.PARegressor;
import com.github.pmerienne.trident.ml.regression.RegressionQuery;
import com.github.pmerienne.trident.ml.regression.RegressionUpdater;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws AlreadyAliveException, InvalidTopologyException
    {
        LocalDRPC drpc=new LocalDRPC();
        
        LocalCluster cluster=new LocalCluster();
        Config config=new Config();
        
        cluster.submitTopology("RegressionDemo", config, buildTopology(drpc));
        
        try{
        	Thread.sleep(10000);
        }catch(InterruptedException ex)
        {
        	ex.printStackTrace();
        }
        
        List<String> drpc_args_list=BirthDataSpout.getDRPCArgsList();
        for(String drpc_args : drpc_args_list)
        {
        	System.out.println(drpc.execute("predict", drpc_args));
        }
     
        cluster.killTopology("RegressionDemo");
        cluster.shutdown();
        
        drpc.shutdown();
    }
    
    private static StormTopology buildTopology(LocalDRPC drpc)
    {
    	TridentTopology topology=new TridentTopology();
    	
    	BirthDataSpout spout=new BirthDataSpout();
    	
    	TridentState regressionModel = topology.newStream("training", spout).partitionPersist(new MemoryMapState.Factory(), new Fields("instance"), new RegressionUpdater("regression", new PARegressor()));
    	
    	topology.newDRPCStream("predict", drpc).each(new Fields("args"), new DRPCArgsToInstance(), new Fields("instance")).stateQuery(regressionModel, new Fields("instance"), new RegressionQuery("regression"), new Fields("prediction")).project(new Fields("args", "prediction"));
    	
    	return topology.build();
    }
}
