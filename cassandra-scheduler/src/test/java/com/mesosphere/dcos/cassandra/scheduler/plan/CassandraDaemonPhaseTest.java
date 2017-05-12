package com.mesosphere.dcos.cassandra.scheduler.plan;

import com.mesosphere.dcos.cassandra.common.config.CassandraSchedulerConfiguration;
import com.mesosphere.dcos.cassandra.common.config.DefaultConfigurationManager;
import com.mesosphere.dcos.cassandra.common.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.placementrule.AwsInfrastructure;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraContainer;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import org.apache.commons.collections.CollectionUtils;
import org.apache.http.client.ClientProtocolException;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.state.StateStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Optional;

import static org.mockito.Mockito.when;

public class CassandraDaemonPhaseTest {
    @Mock
    private PersistentOfferRequirementProvider persistentOfferRequirementProvider;
    @Mock
    private CassandraState cassandraState;
    @Mock
    private SchedulerClient client;
    @Mock
    private static DefaultConfigurationManager configurationManager;

    @Before
    public void beforeEach() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateEmptyPhase() throws ClientProtocolException, URISyntaxException, IOException {
        CassandraSchedulerConfiguration configuration = Mockito.mock(CassandraSchedulerConfiguration.class);
        final DefaultConfigurationManager mockConfigManager = Mockito.mock(DefaultConfigurationManager.class);
        when(mockConfigManager.getTargetConfig()).thenReturn(configuration);
        Mockito.when(configuration.getServers()).thenReturn(0);
        AwsInfrastructure awsInfrastructure = Mockito.mock(AwsInfrastructure.class);
		when(awsInfrastructure.getMySiblingZones())
				.thenReturn(Arrays.asList(AwsInfrastructure.AwsRegion.AP_NORTH_EAST_1.getAvaliablityZones()));
        final CassandraDaemonPhase phase = CassandraDaemonPhase.create(
                cassandraState,
                persistentOfferRequirementProvider,
                client,
                mockConfigManager,
                awsInfrastructure);
        Assert.assertTrue(CollectionUtils.isEmpty(phase.getErrors()));
        Assert.assertTrue(phase.getChildren().isEmpty());
        Assert.assertEquals("Deploy", phase.getName());
    }

    @Test
    public void testCreateSingleStepPhase() throws Exception {
        final CassandraDaemonTask daemonTask = Mockito.mock(CassandraDaemonTask.class);
        final CassandraContainer cassandraContainer = Mockito.mock(CassandraContainer.class);
        when(cassandraContainer.getDaemonTask()).thenReturn(daemonTask);
        final String EXPECTED_NAME = "node-0";
        when(daemonTask.getName()).thenReturn(EXPECTED_NAME);
        final StateStore stateStore = Mockito.mock(StateStore.class);
        when(cassandraState.getStateStore()).thenReturn(stateStore);
        when(stateStore.fetchStatus(EXPECTED_NAME))
                .thenReturn(Optional.empty());

        when(cassandraState.getOrCreateContainer(EXPECTED_NAME)).thenReturn(cassandraContainer);
        CassandraSchedulerConfiguration configuration = Mockito.mock(CassandraSchedulerConfiguration.class);
        when(configurationManager.getTargetConfig()).thenReturn(configuration);
        Mockito.when(configuration.getServers()).thenReturn(1);
        
        AwsInfrastructure awsInfrastructure = Mockito.mock(AwsInfrastructure.class);
		when(awsInfrastructure.getMySiblingZones())
				.thenReturn(Arrays.asList(AwsInfrastructure.AwsRegion.AP_NORTH_EAST_1.getAvaliablityZones()));
		
        final CassandraDaemonPhase phase = CassandraDaemonPhase.create(
                cassandraState,
                persistentOfferRequirementProvider,
                client,
                configurationManager,
                awsInfrastructure);
        Assert.assertTrue(CollectionUtils.isEmpty(phase.getErrors()));
        Assert.assertTrue(phase.getChildren().size() == 1);
        Assert.assertEquals("Deploy", phase.getName());
    }
}
