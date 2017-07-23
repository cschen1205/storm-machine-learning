package com.github.cschen1205.storm.regressions;

import backtype.storm.tuple.Values;

import com.github.pmerienne.trident.ml.core.Instance;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class DRPCArgsToInstance extends BaseFunction {

	private static final long serialVersionUID = 1L;

	public void execute(TridentTuple tuple, TridentCollector collector) {
		String drpc_args = tuple.getString(0);
		String[] args=drpc_args.split(",");
		
		Double label=Double.parseDouble(args[args.length-1]);
		double[] features=new double[args.length-1];
		
		for(int i=0; i < args.length-1; ++i)
		{
			features[i]=Double.parseDouble(args[i]);
		}
		
		Instance<Double> instance=new Instance<Double>(label, features);
		
		collector.emit(new Values(instance));
	}

}
