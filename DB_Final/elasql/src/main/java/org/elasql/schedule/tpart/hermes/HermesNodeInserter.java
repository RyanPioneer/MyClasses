package org.elasql.schedule.tpart.hermes;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Iterator;

import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.BatchNodeInserter;
import org.elasql.schedule.tpart.graph.Edge;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.schedule.tpart.graph.TxNode;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.tx.recovery.DdRecoveryMgr;
import org.elasql.util.ElasqlProperties;

public class HermesNodeInserter implements BatchNodeInserter {
	
	private static final double IMBALANCED_TOLERANCE;
	private TPartStoredProcedureFactory factory;

	static {
		IMBALANCED_TOLERANCE = ElasqlProperties.getLoader()
				.getPropertyAsDouble(HermesNodeInserter.class.getName() + ".IMBALANCED_TOLERANCE", 0.25);
	}
	
	private PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();
	private double[] loadPerPart = new double[PartitionMetaMgr.NUM_PARTITIONS];
	private Set<Integer> overloadedParts = new HashSet<Integer>();
	private Set<Integer> saturatedParts = new HashSet<Integer>();
	private int overloadedThreshold;
	private HashSet<PrimaryKey> hotKeys = new HashSet<PrimaryKey>();
	private HashMap<PrimaryKey, Integer> RWCount = new HashMap<PrimaryKey, Integer>();
	private int totalNumberOfTxs = 0;

	@Override
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks, TPartStoredProcedureTask replicaTask, TPartStoredProcedureFactory factory) {
		this.factory = factory;
		// Step 0: Reset statistics
		Arrays.fill(loadPerPart, 0);
		overloadedParts.clear();
		saturatedParts.clear();
		
		// Step 1: calculate read write count, update replicaTask if needed
		// Murphy: HotRecords 代表很常被 read/write 的 record
		for (TPartStoredProcedureTask task : tasks) {
			totalNumberOfTxs++;
			for (PrimaryKey key : task.getReadSet()) {
				if(RWCount.containsKey(key)) {
					int count = RWCount.get(key)+1;
					RWCount.put(key, count);
				}else {
					RWCount.put(key, 1);
				}
			}
			for (PrimaryKey key : task.getWriteSet()) {
				if(RWCount.containsKey(key)) {
					int count = RWCount.get(key)+1;
					RWCount.put(key, count);
				}else {
					RWCount.put(key, 1);
				}
			}
			// QMurphy: recalcalateHotRecord 的時機設定
		}
		// Murphy: Test
		hotKeys.clear();
		double maxRatio = 0.0;
		for (PrimaryKey key : RWCount.keySet()) {
			double ratio = RWCount.get(key) * 1.0 / totalNumberOfTxs;
			if(ratio > maxRatio) {
				maxRatio = ratio;
			}
			if ((totalNumberOfTxs > 1000) && (RWCount.containsKey(key)) && (RWCount.get(key) * 1.0 / totalNumberOfTxs) >= 0.25) {
				hotKeys.add(key);
			}
		}
		
		// Step 2
		ArrayList<TPartStoredProcedureTask> restOfTasks = new ArrayList<TPartStoredProcedureTask>();
		
		 for (TPartStoredProcedureTask task : tasks) {
		 	boolean containWriteHotRecord = false;
		 	for (PrimaryKey key : task.getWriteSet()) {
		 		if (hotKeys.contains(key)) {
//		 			System.out.println("----Enter----");
		 			containWriteHotRecord = true;
		 			break;
		 		}
		 	}
		 	// Murphy: 把找到有包含 write hot records 的 Txn 先 insert 到 node 裡面
		 	if (containWriteHotRecord) insertAccordingRemoteEdges(graph, task);
		 	else restOfTasks.add(task);
		 }

		 // Step 3: 插入 replication Txn 到每個 node 上面
		 if(hotKeys.size() > 0){
			for (int partId = 0; partId < partMgr.getCurrentNumOfParts(); partId++) {
				StoredProcedureCall call = new StoredProcedureCall(-1, -1, -1234, new Object[] {});
				TPartStoredProcedureTask task_ = createStoredProcedureTask(call);
				graph.insertReplication(task_, hotKeys, partId);
			}
		 }
		 // Step 4: 把剩下的 node 插到 replication Txn 上面
		 for (TPartStoredProcedureTask task : restOfTasks) {
		 	insertAccordingRemoteEdges(graph, task);
		 }

		// -------  Murphy: Original END --------
		
		// Step 5: Find overloaded machines
		overloadedThreshold = (int) Math.ceil(
				((double) tasks.size() / partMgr.getCurrentNumOfParts()) * (IMBALANCED_TOLERANCE + 1));
		if (overloadedThreshold < 1) {
			overloadedThreshold = 1;
		}
		List<TxNode> candidateTxNodes = findTxNodesOnOverloadedParts(graph, tasks.size());
		
//		System.out.println(String.format("Overloaded threshold is %d (batch size: %d)", overloadedThreshold, tasks.size()));
//		System.out.println(String.format("Overloaded machines: %s, loads: %s", overloadedParts.toString(), Arrays.toString(loadPerPart)));
		
		// Step 6: Move tx nodes from overloaded machines to underloaded machines
		int increaseTolerence = 1;
		while (!overloadedParts.isEmpty()) {
//			System.out.println(String.format("Overloaded machines: %s, loads: %s, increaseTolerence: %d", overloadedParts.toString(), Arrays.toString(loadPerPart), increaseTolerence));
			candidateTxNodes = rerouteTxNodesToUnderloadedParts(candidateTxNodes, increaseTolerence);
			increaseTolerence++;
			
			if (increaseTolerence > 100)
				throw new RuntimeException("Something wrong");
		}
		

//		System.out.println(String.format("Final loads: %s", Arrays.toString(loadPerPart)));
	}
	
	private TPartStoredProcedureTask createStoredProcedureTask(StoredProcedureCall call) {
		if (call.isNoOpStoredProcCall()) {
			return new TPartStoredProcedureTask(call.getClientId(), call.getConnectionId(), call.getTxNum(), null);
		} else {
			TPartStoredProcedure<?> sp = factory.getStoredProcedure(call.getPid(), call.getTxNum());
			if (!sp.isDoingReplication()) {
				sp.prepare(call.getPars());
			}

			if (!sp.isReadOnly())
				DdRecoveryMgr.logRequest(call);

			return new TPartStoredProcedureTask(call.getClientId(), call.getConnectionId(), call.getTxNum(), sp);
		}
	}
	
	private void insertAccordingRemoteEdges(TGraph graph, TPartStoredProcedureTask task) {
		int bestPartId = 0;
		int minRemoteEdgeCount = task.getReadSet().size();
		
		for (int partId = 0; partId < partMgr.getCurrentNumOfParts(); partId++) {
			
			// Count the number of remote edge
			int remoteEdgeCount = countRemoteReadEdge(graph, task, partId);
			
			// Find the node in which the tx has fewest remote edges.
			if (remoteEdgeCount < minRemoteEdgeCount) {
				minRemoteEdgeCount = remoteEdgeCount;
				bestPartId = partId;
			}
		}
		// ----- Murphy: Origing ---------
		 graph.insertTxNode(task, bestPartId, true);
		// ----- Murphy: Origin END ------
		
		
		loadPerPart[bestPartId]++;
	}
	
	private int countRemoteReadEdge(TGraph graph, TPartStoredProcedureTask task, int partId) {
		int remoteEdgeCount = 0;
		
		for (PrimaryKey key : task.getReadSet()) {
			// Skip replicated records
			// ----- Murphy: Origin -----
			if (hotKeys.contains(key))
				continue;
			// ----- Murphy: Origin END -----

			if (graph.getResourcePosition(key).getPartId() != partId) {
				remoteEdgeCount++;
			}
		}
		
		return remoteEdgeCount;
	}
	
	private List<TxNode> findTxNodesOnOverloadedParts(TGraph graph, int batchSize) {
		
		// Find the overloaded parts
		for (int partId = 0; partId < loadPerPart.length; partId++) {
			if (loadPerPart[partId] > overloadedThreshold)
				overloadedParts.add(partId);
			else if (loadPerPart[partId] == overloadedThreshold)
				saturatedParts.add(partId);
		}
		
		List<TxNode> nodesOnOverloadedParts = new ArrayList<TxNode>();
		for (TxNode node : graph.getTxNodes()) { 
			if (node.getAllowReroute()) {
				int homePartId = node.getPartId();
				if (overloadedParts.contains(homePartId)) {
					nodesOnOverloadedParts.add(node);
				}
			}
		}
		
		// Reverse the list, which makes the tx node ordered by tx number from large to small
		Collections.reverse(nodesOnOverloadedParts);
		
		return nodesOnOverloadedParts;
	}
	
	private List<TxNode> rerouteTxNodesToUnderloadedParts(List<TxNode> candidateTxNodes, int increaseTolerence) {
		List<TxNode> nextCandidates = new ArrayList<TxNode>();
		
		for (TxNode node : candidateTxNodes) {
			// Count remote edges (including write edges)
			int currentPartId = node.getPartId();
			
			// If the home partition is no longer a overloaded part, skip it
			if (!overloadedParts.contains(currentPartId))
				continue;
			
			int currentRemoteEdges = countRemoteReadWriteEdges(node, currentPartId);
			int bestDelta = increaseTolerence + 1;
			int bestPartId = currentPartId;
			
			// Find a better partition
			for (int partId = 0; partId < partMgr.getCurrentNumOfParts(); partId++) {
				// Skip home partition
				if (partId == currentPartId)
					continue;
				
				// Skip overloaded partitions
				if (overloadedParts.contains(partId))
					continue;
				
				// Skip saturated partitions
				if (saturatedParts.contains(partId))
					continue;
				
				// Count remote edges
				int remoteEdgeCount = countRemoteReadWriteEdges(node, partId);
				
				// Calculate the difference
				int delta = remoteEdgeCount - currentRemoteEdges;
				if (delta <= increaseTolerence) {
					// Prefer the machine with lower loadn
					if ((delta < bestDelta) ||
							(delta == bestDelta && loadPerPart[partId] < loadPerPart[bestPartId])) {
						bestDelta = delta;
						bestPartId = partId;
					}
				}
			}
			
			// If there is no match, try next tx node
			if (bestPartId == currentPartId) {
				nextCandidates.add(node);
				continue;
			}
//			System.out.println(String.format("Find a better partition %d for tx.%d", bestPartId, node.getTxNum()));
			node.setPartId(bestPartId);
			
			// Update loads
			loadPerPart[currentPartId]--;
			if (loadPerPart[currentPartId] == overloadedThreshold) {
				overloadedParts.remove(currentPartId);
				saturatedParts.add(currentPartId);
			}	
			loadPerPart[bestPartId]++;
			if (loadPerPart[bestPartId] == overloadedThreshold) {
				saturatedParts.add(bestPartId);
			}
			
			// Check if there are still overloaded machines
			if (overloadedParts.isEmpty())
				return null;
		}
		
		return nextCandidates;
	}
	
	private int countRemoteReadWriteEdges(TxNode node, int homePartId) {
		int count = 0;
		
		for (Edge readEdge : node.getReadEdges()) {
			// Skip replicated records
			// ----- Murphy: Origin------
			if (hotKeys.contains(readEdge.getResourceKey()))
				continue;
			// ----- Murphy: Origin END------

			if (readEdge.getTarget().getPartId() != homePartId)
				count++;
		}
		
		for (Edge writeEdge : node.getWriteEdges()) {
			if (writeEdge.getTarget().getPartId() != homePartId)
				count++;
		}
		
		// Note: We do not consider write back edges because Hermes will make it local
		
		return count;
	}
}
