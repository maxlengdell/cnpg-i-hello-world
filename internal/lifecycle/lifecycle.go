// Package lifecycle implements the lifecycle hooks for version downgrade reset
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/decoder"
	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/object"
	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"
	"github.com/cloudnative-pg/machinery/pkg/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/cloudnative-pg/cnpg-i-hello-world/internal/utils"
)

// Implementation is the implementation of the lifecycle handler
type Implementation struct {
	lifecycle.UnimplementedOperatorLifecycleServer
}

// GetCapabilities exposes the lifecycle capabilities for first startup reset
func (impl Implementation) GetCapabilities(
	_ context.Context,
	_ *lifecycle.OperatorLifecycleCapabilitiesRequest,
) (*lifecycle.OperatorLifecycleCapabilitiesResponse, error) {
	return &lifecycle.OperatorLifecycleCapabilitiesResponse{
		LifecycleCapabilities: []*lifecycle.OperatorLifecycleCapabilities{
			{
				Group: "",
				Kind:  "Pod",
				OperationTypes: []*lifecycle.OperatorOperationType{
					{
						Type: lifecycle.OperatorOperationType_TYPE_CREATE,
					},
					{
						Type: lifecycle.OperatorOperationType_TYPE_DELETE,
					},
				},
			},
		},
	}, nil
}

// LifecycleHook is called when creating Kubernetes pods
func (impl Implementation) LifecycleHook(
	ctx context.Context,
	request *lifecycle.OperatorLifecycleRequest,
) (*lifecycle.OperatorLifecycleResponse, error) {
	logger := log.FromContext(ctx).WithName("cnpg-i-hello-world")

	// Early version downgrade check
	cluster, err := decoder.DecodeClusterLenient(request.GetClusterDefinition())
	if err != nil {
		logger.Error(err, "Failed to decode cluster definition")
		return nil, err
	}

	if !impl.isVersionDowngrade(cluster, logger) {
		logger.Info("No version downgrade detected, skipping all actions")
		return &lifecycle.OperatorLifecycleResponse{}, nil
	}

	kind, err := utils.GetKind(request.GetObjectDefinition())
	if err != nil {
		logger.Error(err, "Failed to get kind from object definition")
		return nil, err
	}

	operation := request.GetOperationType().GetType().Enum()
	if operation == nil {
		logger.Error(nil, "No operation set in request")
		return nil, errors.New("no operation set")
	}

	// Log current number of instances deployed
	logger.Info("Current cluster instances", "instances", cluster.Spec.Instances)

	logger.Info("Processing lifecycle hook", "kind", kind, "operation", operation.String())

	switch kind {
	case "Pod":
		if *operation == lifecycle.OperatorOperationType_TYPE_CREATE {
			logger.Info("Resetting before startup")
			return impl.resetForFirstStartup(ctx, request)
		} else if *operation == lifecycle.OperatorOperationType_TYPE_DELETE {
			logger.Info("Executing backup before delete")
			return impl.backupBeforeDelete(ctx, request)
		}
	}

	logger.Info("No action taken for this lifecycle hook")
	return &lifecycle.OperatorLifecycleResponse{}, nil
}

// resetForFirstStartup modifies the pod to reset cluster state only on version downgrade
func (impl Implementation) resetForFirstStartup(
	ctx context.Context,
	request *lifecycle.OperatorLifecycleRequest,
) (*lifecycle.OperatorLifecycleResponse, error) {
	logger := log.FromContext(ctx).WithName("downgrade_reset")
	logger.Info("Checking for version downgrade scenario")

	cluster, err := decoder.DecodeClusterLenient(request.GetClusterDefinition())
	if err != nil {
		logger.Error(err, "Failed to decode cluster definition")
		return nil, err
	}

	pod, err := decoder.DecodePodJSON(request.GetObjectDefinition())
	if err != nil {
		logger.Error(err, "Failed to decode pod definition")
		return nil, err
	}

	logger.Info("Decoded cluster and pod definitions", "clusterName", cluster.Name, "podName", pod.Name)

	// Version downgrade already checked in LifecycleHook

	logger.Info("Adding reset init container")

	mutatedPod := pod.DeepCopy()

	// Add init container with same config as initdb job
	initContainer := corev1.Container{
		Name:    "downgrade-reset",
		Image:   cluster.Status.Image,
		Command: []string{"/controller/manager"},
		Args: []string{
			"instance", "init",
			"--initdb-flags", "--encoding=UTF8 --lc-collate=C --lc-ctype=C",
			"--app-db-name", "app",
			"--app-user", "app",
			"--log-level=info",
		},
		Env: []corev1.EnvVar{
			{Name: "PGDATA", Value: "/var/lib/postgresql/data/pgdata"},
			{Name: "POD_NAME", Value: pod.Name},
			{Name: "NAMESPACE", Value: cluster.Namespace},
			{Name: "CLUSTER_NAME", Value: cluster.Name},
			{Name: "PSQL_HISTORY", Value: "/controller/tmp/.psql_history"},
			{Name: "PGPORT", Value: "5432"},
			{Name: "PGHOST", Value: "/controller/run"},
			{Name: "TMPDIR", Value: "/controller/tmp"},
			{Name: "APP_USERNAME", ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Name + "-app"},
					Key:                  "username",
					Optional:             &[]bool{false}[0],
				},
			}},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "scratch-data", MountPath: "/controller"},
			{Name: "shm", MountPath: "/dev/shm"},
			{Name: "scratch-data", MountPath: "/run"},
			{Name: "pgdata", MountPath: "/var/lib/postgresql/data"},
		},
		SecurityContext: &corev1.SecurityContext{
			SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		},
	}

	mutatedPod.Spec.InitContainers = append(mutatedPod.Spec.InitContainers, initContainer)

	// Set the instances to 1 as well.

	patch, err := object.CreatePatch(mutatedPod, pod)
	if err != nil {
		return nil, err
	}

	logger.Info("Successfully added downgrade reset init container")

	return &lifecycle.OperatorLifecycleResponse{
		JsonPatch: patch,
	}, nil
}

// isVersionDowngrade checks if current version is lower than existing data version
func (impl Implementation) isVersionDowngrade(cluster *apiv1.Cluster, logger log.Logger) bool {
	// Use built-in function to get current major version
	currentMajor := cluster.Status.PGDataImageInfo.MajorVersion

	// Extract the major version from the image name in cluster.Spec.ImageName
	imageParts := strings.Split(cluster.Status.Image, ":")
	if len(imageParts) < 2 {
		logger.Error(nil, "Invalid image name format", "imageName", cluster.Spec.ImageName)
		return false
	}
	versionParts := strings.Split(imageParts[1], ".")
	if len(versionParts) < 1 {
		logger.Error(nil, "Invalid version format in image name", "version", imageParts[1])
		return false
	}
	imageMajor, err := strconv.Atoi(versionParts[0])
	if err != nil {
		logger.Error(err, "Failed to parse major version from image name", "version", versionParts[0])
		return false
	}
	logger.Info(fmt.Sprintf("Current major version: %d, Requested Image major version: %d", currentMajor, imageMajor))

	return imageMajor < currentMajor
}

// executePgDump executes pg_dump to backup database before reset
func (impl Implementation) executePgDump(ctx context.Context, cluster *apiv1.Cluster, logger log.Logger) error {
	logger.Info("Executing pg_dump backup")

	// Create timeout context for pg_dump (5 minutes)
	dumpCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Get connection details from cluster with FQDN for cross-namespace access
	host := fmt.Sprintf("%s-rw.%s.svc.cluster.local", cluster.Name, cluster.Namespace)
	port := "5432"
	user := "postgres"
	backupFile := fmt.Sprintf("/tmp/%s_backup_%d.sql", cluster.Name, cluster.Status.PGDataImageInfo.MajorVersion)

	// Get password from CNPG superuser secret
	password, err := impl.getSuperuserPassword(cluster, logger)
	if err != nil {
		logger.Error(err, "Failed to get superuser password")
		return fmt.Errorf("failed to get superuser password: %w", err)
	}

	// Execute pg_dumpall command with timeout context
	cmd := exec.CommandContext(dumpCtx, "pg_dumpall",
		"-h", host,
		"-p", port,
		"-U", user,
		"-f", backupFile,
		"-c",
		"--if-exists",
	)

	// Set environment variables with password from secret
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", string(password)))

	logger.Info("Running pg_dumpall", "host", host, "user", user, "backupFile", backupFile)

	output, err := cmd.CombinedOutput()
	if err != nil {
		logger.Error(err, "pg_dumpall failed", "output", string(output))
		return fmt.Errorf("pg_dumpall failed: %w", err)
	}

	// Clean non-backwards compatible SQL from dump file
	if err := impl.cleanDumpFile(backupFile, logger); err != nil {
		logger.Error(err, "Failed to clean dump file")
		return fmt.Errorf("failed to clean dump file: %w", err)
	}

	logger.Info("pg_dump backup completed and cleaned", "backupFile", backupFile)
	return nil
}

// backupBeforeDelete executes pg_dump backup before pod deletion
func (impl Implementation) backupBeforeDelete(
	ctx context.Context,
	request *lifecycle.OperatorLifecycleRequest,
) (*lifecycle.OperatorLifecycleResponse, error) {
	logger := log.FromContext(ctx).WithName("backup_before_delete")
	logger.Info("Executing backup before pod deletion")

	cluster, err := decoder.DecodeClusterLenient(request.GetClusterDefinition())
	if err != nil {
		logger.Error(err, "Failed to decode cluster definition")
		return &lifecycle.OperatorLifecycleResponse{}, nil // Don't block deletion
	}

	// Execute pg_dump backup
	if err := impl.executePgDump(ctx, cluster, logger); err != nil {
		logger.Error(err, "Failed to execute pg_dump backup before deletion")
		return nil, err
	}

	// Create downgrade marker for restore service
	if err := impl.createDowngradeMarker(cluster, logger); err != nil {
		logger.Error(err, "Failed to create downgrade marker")
	}

	logger.Info("Backup completed and downgrade marker created")

	return &lifecycle.OperatorLifecycleResponse{}, nil
}

// getSuperuserPassword retrieves the postgres password from the cluster's superuser secret
func (impl Implementation) getSuperuserPassword(cluster *apiv1.Cluster, logger log.Logger) (string, error) {
	// Create in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		return "", fmt.Errorf("failed to create in-cluster config: %w", err)
	}

	// Create Kubernetes client
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "", fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// Get the superuser secret
	secretName := fmt.Sprintf("%s-superuser", cluster.Name)
	secret, err := clientset.CoreV1().Secrets(cluster.Namespace).Get(context.Background(), secretName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get superuser secret %s/%s: %w", cluster.Namespace, secretName, err)
	}

	// Extract password
	password, exists := secret.Data["password"]
	if !exists {
		return "", fmt.Errorf("password not found in superuser secret")
	}

	logger.Info("Successfully retrieved superuser password", "secretName", secretName)
	return string(password), nil
}

// createDowngradeMarker creates a file to signal that a downgrade occurred
func (impl Implementation) createDowngradeMarker(cluster *apiv1.Cluster, logger log.Logger) error {
	markerFile := fmt.Sprintf("/tmp/%s-downgrade-marker", cluster.Name)
	markerContent := fmt.Sprintf("downgrade=true\ntimestamp=%d\n", time.Now().Unix())

	err := os.WriteFile(markerFile, []byte(markerContent), 0644)
	if err != nil {
		return fmt.Errorf("failed to create downgrade marker: %w", err)
	}

	logger.Info("Created downgrade marker", "file", markerFile)
	return nil
}

// cleanDumpFile removes non-backwards compatible SQL statements from dump file
func (impl Implementation) cleanDumpFile(backupFile string, logger log.Logger) error {
	cmd := exec.Command("sed", "-i", "-E",
		"s/LOCALE_PROVIDER = \\w+ |^SET transaction_timeout = 0;| WITH INHERIT TRUE GRANTED BY \\w+//",
		backupFile)

	output, err := cmd.CombinedOutput()
	if err != nil {
		logger.Error(err, "sed command failed", "output", string(output))
		return fmt.Errorf("sed command failed: %w", err)
	}

	logger.Info("Cleaned non-backwards compatible SQL from dump file")
	return nil
}
