package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.mesosphere.dcos.cassandra.common.config.CassandraSchedulerConfiguration;
import com.mesosphere.dcos.cassandra.common.config.DefaultConfigurationManager;
import com.mesosphere.dcos.cassandra.common.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.placementrule.AwsInfrastructure;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;

import org.apache.http.client.ClientProtocolException;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.scheduler.plan.DefaultPhase;
import org.apache.mesos.scheduler.plan.Step;
import org.apache.mesos.scheduler.plan.strategy.SerialStrategy;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CassandraDaemonPhase extends DefaultPhase {

    private static List<Step> createSteps(
            final CassandraState cassandraState,
            final PersistentOfferRequirementProvider provider,
            final DefaultConfigurationManager configurationManager,
            final AwsInfrastructure awsInfrastructure)
                throws ConfigStoreException, IOException, URISyntaxException {
        final int servers = ((CassandraSchedulerConfiguration)configurationManager.getTargetConfig())
                .getServers();

        final List<String> names = new ArrayList<>(servers);
        
		Map<Integer, String> nodeIndexToZoneCodeMap = getNodeToZoneMap(servers, awsInfrastructure);
        
        
        for (int id = 0; id < servers; ++id) {
            names.add(CassandraDaemonTask.NAME_PREFIX + id);
        }

        Collections.sort(names);

        // here we will add a step for all tasks we have recorded and create a
        // new step with a newly recorded task for a scale out
        final List<Step> steps = new ArrayList<>();
        
		Map<String, String> nodeTaskNameToZoneCodeMap = new HashMap<String, String>(); 
        for (int i = 0; i < servers; i++) {
            steps.add(CassandraDaemonStep.create(names.get(i), provider, cassandraState));
			nodeTaskNameToZoneCodeMap.put(names.get(i), nodeIndexToZoneCodeMap.get(i));
        }
        
		PersistentOfferRequirementProvider.setNodeToZoneInformationMap(nodeTaskNameToZoneCodeMap);
		
        return steps;
    }

	private static Map<Integer, String> getNodeToZoneMap(int servers, final AwsInfrastructure awsInfrastructure)
			throws ClientProtocolException, URISyntaxException, IOException {

		List<String> zones = awsInfrastructure.getMySiblingZones();
		int totalZones = zones.size();

		int serversOnEachZone = servers / totalZones;
		int remainingServers = servers % totalZones;

		int serverNumber = 0;
		Map<Integer, String> serverIndexToZoneMap = new HashMap<>();
		for (String zone : zones) {
			int serverCount = 0;
			while (serverCount < serversOnEachZone) {
				serverIndexToZoneMap.put(serverNumber, zone);
				serverCount++;
				serverNumber++;
			}
		}

		while (remainingServers > 0) {
			serverIndexToZoneMap.put(serverNumber, zones.get(remainingServers));
			serverNumber++;
			remainingServers--;
		}

		return serverIndexToZoneMap;
	}
	
    public static final CassandraDaemonPhase create(
            final CassandraState cassandraState,
            final PersistentOfferRequirementProvider provider,
            final SchedulerClient client,
            final DefaultConfigurationManager configurationManager,
            final AwsInfrastructure awsInfrastructure) {
        try {
            return new CassandraDaemonPhase(
					createSteps(cassandraState, provider, configurationManager, awsInfrastructure),
                    new ArrayList<>());
        } catch (Throwable e) {
            return new CassandraDaemonPhase(new ArrayList<>(), Arrays.asList(String.format(
                    "Error creating CassandraDaemonStep : message = %s", e.getMessage())));
        }
    }

    public CassandraDaemonPhase(
            final List<Step> steps,
            final List<String> errors) {
        super("Deploy", steps, new SerialStrategy<>(), errors);
    }
}
