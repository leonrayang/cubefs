package master

import (
	"sync"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
)

func TestZone_createNodeSetWithSpecifiedNodes(t *testing.T) {
	// Create a mock cluster
	c := &Cluster{
		Name: "test-cluster",
		cfg: &clusterConfig{
			nodeSetCapacity: 10,
		},
	}
	c.DecommissionLimit = 5
	c.dataNodes = *new(sync.Map)
	c.metaNodes = *new(sync.Map)

	// Create a mock zone
	zone := newZone("test-zone", proto.MediaType_SSD)

	// Test data
	dataNodeAddrs := []string{"192.168.1.10:17310", "192.168.1.11:17310"}
	metaNodeAddrs := []string{"192.168.1.20:17210", "192.168.1.21:17210"}
	allowedVolumes := []string{"vol1", "vol2"}

	// Test the function
	ns, err := zone.createNodeSetWithSpecifiedNodes(c, dataNodeAddrs, metaNodeAddrs, allowedVolumes)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, ns)
	assert.Equal(t, zone.name, ns.zoneName)
	assert.Equal(t, int32(5), ns.decommissionParallelLimit)

	// Verify the nodeset is restricted
	assert.True(t, ns.IsRestricted())
	assert.Equal(t, allowedVolumes, ns.GetAllowedVolumes())

	// Verify datanodes were added
	assert.Equal(t, 2, ns.dataNodeLen())

	// Verify metanodes were added
	assert.Equal(t, 2, ns.metaNodeLen())

	// Verify nodes are in the cluster maps
	for _, addr := range dataNodeAddrs {
		if node, ok := c.dataNodes.Load(addr); ok {
			dataNode := node.(*DataNode)
			assert.Equal(t, ns.ID, dataNode.NodeSetID)
		} else {
			t.Errorf("datanode %s not found in cluster", addr)
		}
	}

	for _, addr := range metaNodeAddrs {
		if node, ok := c.metaNodes.Load(addr); ok {
			metaNode := node.(*MetaNode)
			assert.Equal(t, ns.ID, metaNode.NodeSetID)
		} else {
			t.Errorf("metanode %s not found in cluster", addr)
		}
	}
}

func TestZone_createNodeSetWithSpecifiedNodes_EmptyLists(t *testing.T) {
	// Create a mock cluster
	c := &Cluster{
		Name: "test-cluster",
		cfg: &clusterConfig{
			nodeSetCapacity: 10,
		},
	}
	c.DecommissionLimit = 5
	c.dataNodes = *new(sync.Map)
	c.metaNodes = *new(sync.Map)

	// Create a mock zone
	zone := newZone("test-zone", proto.MediaType_SSD)

	// Test with empty lists
	ns, err := zone.createNodeSetWithSpecifiedNodes(c, []string{}, []string{}, []string{"vol1"})

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, ns)
	assert.Equal(t, 0, ns.dataNodeLen())
	assert.Equal(t, 0, ns.metaNodeLen())
	assert.True(t, ns.IsRestricted())
}

func TestZone_createNodeSetWithSpecifiedNodes_ExistingNodes(t *testing.T) {
	// Create a mock cluster
	c := &Cluster{
		Name: "test-cluster",
		cfg: &clusterConfig{
			nodeSetCapacity: 10,
		},
	}
	c.DecommissionLimit = 5
	c.dataNodes = *new(sync.Map)
	c.metaNodes = *new(sync.Map)

	// Create a mock zone
	zone := newZone("test-zone", proto.MediaType_SSD)

	// Create existing nodes
	existingDataNode := newDataNode("192.168.1.10:17310", "test-zone", "test-cluster", proto.MediaType_SSD)
	existingDataNode.ID = 1001
	c.dataNodes.Store(existingDataNode.Addr, existingDataNode)

	existingMetaNode := newMetaNode("192.168.1.20:17210", "test-zone", "test-cluster")
	existingMetaNode.ID = 2001
	c.metaNodes.Store(existingMetaNode.Addr, existingMetaNode)

	// Test data
	dataNodeAddrs := []string{"192.168.1.10:17310", "192.168.1.11:17310"}
	metaNodeAddrs := []string{"192.168.1.20:17210", "192.168.1.21:17210"}
	allowedVolumes := []string{"vol1", "vol2"}

	// Test the function
	ns, err := zone.createNodeSetWithSpecifiedNodes(c, dataNodeAddrs, metaNodeAddrs, allowedVolumes)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, ns)
	assert.Equal(t, 2, ns.dataNodeLen())
	assert.Equal(t, 2, ns.metaNodeLen())

	// Verify existing nodes were updated
	if node, ok := c.dataNodes.Load("192.168.1.10:17310"); ok {
		dataNode := node.(*DataNode)
		assert.Equal(t, ns.ID, dataNode.NodeSetID)
		assert.Equal(t, uint64(1001), dataNode.ID) // ID should be preserved
	}

	if node, ok := c.metaNodes.Load("192.168.1.20:17210"); ok {
		metaNode := node.(*MetaNode)
		assert.Equal(t, ns.ID, metaNode.NodeSetID)
		assert.Equal(t, uint64(2001), metaNode.ID) // ID should be preserved
	}
}
