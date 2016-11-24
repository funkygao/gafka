# helix

### typical coding block

    admin = new ZKHelixAdmin(ZK_ADDRESS);
    
    // create cluster namespace in zookeeper
    admin.addCluster(CLUSTER_NAME);
    
    // configure nodes of the cluster
    InstanceConfig instanceConfig = new InstanceConfig(hosts[i]+ "_" + ports[i]);
    instanceConfig.setHostName(hosts[i]);
    instanceConfig.setPort(ports[i]);
    instanceConfig.setInstanceEnabled(true);
    admin.addInstance(CLUSTER_NAME, instanceConfig);
    
    // optional: define state model and constraints
    // StateModelDefinition.Builder builder = new StateModelDefinition.Builder(STATE_MODEL_NAME);
    // builder.addState(MASTER, 1);
    // builder.addState(SLAVE, 2);
    // builder.addState(OFFLINE);
    // builder.initialState(OFFLINE);
    // builder.addTransition(OFFLINE, SLAVE);
    // builder.addTransition(SLAVE, OFFLINE);
    // builder.addTransition(SLAVE, MASTER);
    // builder.addTransition(MASTER, SLAVE);
    // builder.upperBound(MASTER, 1);
    // builder.dynamicUpperBound(SLAVE, "R"); // R stands for replica
    // StateModelDefinition statemodelDefinition = builder.build();
    // admin.addStateModelDef(CLUSTER_NAME, STATE_MODEL_NAME, myStateModel);
    
    // configure resource
    admin.addResource(CLUSTER_NAME, RESOURCE_NAME, NUM_PARTITIONS, STATE_MODEL_NAME, MODE);

    // trigger rebalance
    admin.rebalance(CLUSTER_NAME, RESOURCE_NAME, NUM_REPLICAS);
