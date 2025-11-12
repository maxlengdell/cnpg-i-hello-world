package operator

import (
	"context"

	"github.com/cloudnative-pg/cnpg-i/pkg/operator"
	"github.com/cloudnative-pg/machinery/pkg/log"
)

// Implementation implements the operator service
type Implementation struct {
	operator.OperatorServer
}

// GetCapabilities gets the capabilities of this operator lifecycle hook
func (Implementation) GetCapabilities(
	context.Context,
	*operator.OperatorCapabilitiesRequest,
) (*operator.OperatorCapabilitiesResult, error) {
	return &operator.OperatorCapabilitiesResult{
		Capabilities: []*operator.OperatorCapability{
			{
				Type: &operator.OperatorCapability_Rpc{
					Rpc: &operator.OperatorCapability_RPC{
						Type: operator.OperatorCapability_RPC_TYPE_VALIDATE_CLUSTER_CREATE,
					},
				},
			},
			{
				Type: &operator.OperatorCapability_Rpc{
					Rpc: &operator.OperatorCapability_RPC{
						Type: operator.OperatorCapability_RPC_TYPE_VALIDATE_CLUSTER_CHANGE,
					},
				},
			},
			{
				Type: &operator.OperatorCapability_Rpc{
					Rpc: &operator.OperatorCapability_RPC{
						Type: operator.OperatorCapability_RPC_TYPE_MUTATE_CLUSTER,
					},
				},
			},
		},
	}, nil
}

// ValidateClusterCreate validates cluster creation
func (Implementation) ValidateClusterCreate(
	ctx context.Context,
	request *operator.OperatorValidateClusterCreateRequest,
) (*operator.OperatorValidateClusterCreateResult, error) {
	logger := log.FromContext(ctx).WithName("validate-cluster-create")
	logger.Info("ValidateClusterCreate called")

	return &operator.OperatorValidateClusterCreateResult{}, nil
}

// ValidateClusterChange validates cluster changes
func (Implementation) ValidateClusterChange(
	ctx context.Context,
	request *operator.OperatorValidateClusterChangeRequest,
) (*operator.OperatorValidateClusterChangeResult, error) {
	logger := log.FromContext(ctx).WithName("validate-cluster-change")
	logger.Info("ValidateClusterChange called")

	return &operator.OperatorValidateClusterChangeResult{}, nil
}

// MutateCluster mutates cluster during admission
func (Implementation) MutateCluster(
	ctx context.Context,
	request *operator.OperatorMutateClusterRequest,
) (*operator.OperatorMutateClusterResult, error) {
	logger := log.FromContext(ctx).WithName("mutate-cluster")
	logger.Info("MutateCluster called")

	return &operator.OperatorMutateClusterResult{
		JsonPatch: []byte("[]"),
	}, nil
}
