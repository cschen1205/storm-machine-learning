package com.github.cschen1205.storm.regressions;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.github.pmerienne.trident.ml.core.Instance;
import com.github.pmerienne.trident.ml.testing.data.Datasets;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

public class BirthDataSpout implements IBatchSpout {

	private static final long serialVersionUID = 1L;

	private int batchSize=10;
	private int batchIndex=0;
	
	private static List<Instance<Double>> sample_data=new ArrayList<Instance<Double>>();
	private static List<Instance<Double>> testing_data=new ArrayList<Instance<Double>>();
	
	public static List<String> getDRPCArgsList()
	{
		List<String> drpc_args_list =new ArrayList<String>();
		for(Instance<Double> instance : testing_data)
		{
			double[] features = instance.getFeatures();
			String drpc_args="";
			for(int i=0; i < features.length; ++i)
			{
				if(i==0)
				{
					drpc_args+=features[i];
				}
				else
				{
					drpc_args+=(","+features[i]);
				}
			}
			drpc_args+=(","+instance.label);
			drpc_args_list.add(drpc_args);
		}
		
		return drpc_args_list;
	}
	
	static{
		FileInputStream is=null;
		BufferedReader br=null;
		try{
			String filePath="src/test/resources/births.csv";
			is=new FileInputStream(filePath);
			br=new BufferedReader(new InputStreamReader(is));
			
			List<Instance<Double>> temp=new ArrayList<Instance<Double>>();
			String line=null;
			while((line=br.readLine())!=null)
			{
				String[] values = line.split(";");
				double label= Double.parseDouble(values[values.length-1]);
				double[] features=new double[values.length-1];
				for(int i=0; i < values.length-1; ++i)
				{
					features[i]=Double.parseDouble(values[i]);
				}
				
				Instance<Double> instance=new Instance<Double>(label, features);
				temp.add(instance);
			}
			
			Collections.shuffle(temp);
			
			for(Instance<Double> instance : temp)
			{
				if(testing_data.size() < 10)
				{
					testing_data.add(instance);
				}
				else
				{
					sample_data.add(instance);
				}
			}
			
		}catch(FileNotFoundException ex)
		{
			ex.printStackTrace();
		}catch(IOException ex)
		{
			ex.printStackTrace();
		}finally
		{
			try {
				if(is!=null) is.close();
				if(br !=null) br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public BirthDataSpout()
	{
		
	}
	
	public void open(Map conf, TopologyContext context) {
		// TODO Auto-generated method stub
		
	}

	public void emitBatch(long batchId, TridentCollector collector) {
		// TODO Auto-generated method stub
		int maxBatchCount=sample_data.size() / batchSize;
		if(maxBatchCount > 0 && batchIndex < maxBatchCount)
		{
			for(int i=batchIndex * batchSize; i < sample_data.size() && i < (batchIndex+1) * batchSize; ++i)
			{
				Instance<Double> instance = sample_data.get(i);
				collector.emit(new Values(instance));
			}
			batchIndex=(batchIndex+1) % maxBatchCount;
		}
	}

	public void ack(long batchId) {
		// TODO Auto-generated method stub
		
	}

	public void close() {
		// TODO Auto-generated method stub
		
	}

	public Map getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public Fields getOutputFields() {
		// TODO Auto-generated method stub
		return new Fields("instance");
	}

}
