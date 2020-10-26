package io.github.anttikaikkonen.blockchainanalyticsflink.statefun.sink;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.CassandraSessionBuilder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;

public class ScyllaCommitter extends CheckpointCommitter {

    private static final long serialVersionUID = 1L;

	private final CassandraSessionBuilder sessionBuilder;
	private transient Session session;

	private String table = "checkpoints_";

	/**
	 * A cache of the last committed checkpoint ids per subtask index. This is used to
	 * avoid redundant round-trips to Cassandra (see {@link #isCheckpointCommitted(int, long)}.
	 */
	private final Map<Integer, Long> lastCommittedCheckpoints = new HashMap<>();

	public ScyllaCommitter(CassandraSessionBuilder sessionBuilder) {
		this.sessionBuilder = sessionBuilder;
	}


	/**
	 * Internally used to set the job ID after instantiation.
	 */
        @Override
	public void setJobId(String id) throws Exception {
		super.setJobId(id);
		//table += id;
	}

	/**
	 * Generates the necessary tables to store information.
	 *
	 * @throws Exception
	 */
	@Override
	public void createResource() throws Exception {
                session = sessionBuilder.build();

		session.execute(String.format("CREATE TABLE IF NOT EXISTS %s (sink_id text, sub_id int, checkpoint_id bigint, PRIMARY KEY (sink_id, sub_id));", table));

		try {
			session.close();
		} catch (Exception e) {
			LOG.error("Error while closing session.", e);
		}
		try {
			session.getCluster().close();
		} catch (Exception e) {
			LOG.error("Error while closing cluster.", e);
		}
	}

	@Override
	public void open() throws Exception {
		if (sessionBuilder == null) {
			throw new RuntimeException("No ClusterBuilder was set.");
		}
		session = sessionBuilder.build();
	}

	@Override
	public void close() throws Exception {
		this.lastCommittedCheckpoints.clear();
		try {
			session.close();
		} catch (Exception e) {
			LOG.error("Error while closing session.", e);
		}
		try {
			session.getCluster().close();
		} catch (Exception e) {
			LOG.error("Error while closing cluster.", e);
		}
	}

	@Override
	public void commitCheckpoint(int subtaskIdx, long checkpointId) {
		String statement = String.format(
			"UPDATE %s set checkpoint_id=%d where sink_id='%s' and sub_id=%d;", table, checkpointId, operatorId, subtaskIdx);

		session.execute(statement);
		lastCommittedCheckpoints.put(subtaskIdx, checkpointId);
	}

	@Override
	public boolean isCheckpointCommitted(int subtaskIdx, long checkpointId) {
		// Pending checkpointed buffers are committed in ascending order of their
		// checkpoint id. This way we can tell if a checkpointed buffer was committed
		// just by asking the third-party storage system for the last checkpoint id
		// committed by the specified subtask.

		Long lastCommittedCheckpoint = lastCommittedCheckpoints.get(subtaskIdx);
		if (lastCommittedCheckpoint == null) {
			String statement = String.format(
				"SELECT checkpoint_id FROM %s where sink_id='%s' and sub_id=%d;", table, operatorId, subtaskIdx);

			Iterator<Row> resultIt = session.execute(statement).iterator();
			if (resultIt.hasNext()) {
				lastCommittedCheckpoint = resultIt.next().getLong("checkpoint_id");
				lastCommittedCheckpoints.put(subtaskIdx, lastCommittedCheckpoint);
			}
		}
		return lastCommittedCheckpoint != null && checkpointId <= lastCommittedCheckpoint;
	}
}
