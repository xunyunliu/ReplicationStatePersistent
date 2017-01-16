package org.apache.storm.grouping;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.ReplicationUtils;
import org.apache.storm.utils.TupleUtils;

public class ReplicationAwareFieldsGrouping implements CustomStreamGrouping {

	private Fields outFields;
	private Fields fields;
	private List<Integer> targetTasks;
	private int numTasks;
	private int numReplications;
	private int realNumTasks;

	public ReplicationAwareFieldsGrouping(Fields fields) {
		this.fields = fields;
	}

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		this.targetTasks = targetTasks;
		this.numReplications = ReplicationUtils.loadNumReplications(context);
		numTasks = targetTasks.size();
		if (numTasks % numReplications != 0) {
			throw new IllegalArgumentException("The number of tasks should be devisible by the number of replications");
		}
		realNumTasks = numTasks / numReplications;
		if (this.fields != null) {
			this.outFields = context.getComponentOutputFields(stream);
		}
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		List<Integer> selectedTaskIDs = new ArrayList<Integer>();
		if (fields != null) {
			int targetTaskIndex = Math.abs(TupleUtils.listHashCode(outFields.select(fields, values))) % realNumTasks;
			for (int i = 0; i < numReplications; i++) {
				selectedTaskIDs.add(targetTasks.get(targetTaskIndex * numReplications + i));
			}
		} else {
			throw new RuntimeException("Error! Fields have not been set for ReplicationAwareFieldsGrouping.");
		}
		return selectedTaskIDs;
	}

}
