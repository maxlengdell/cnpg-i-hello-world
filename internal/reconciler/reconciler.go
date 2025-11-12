// Package reconciler implements reconciler hooks for cluster state management
package reconciler

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/decoder"
	"github.com/cloudnative-pg/cnpg-i/pkg/reconciler"
	"github.com/cloudnative-pg/machinery/pkg/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Implementation is the implementation of the reconciler hooks
type Implementation struct {
	reconciler.UnimplementedReconcilerHooksServer
}

// GetCapabilities returns the reconciler capabilities
func (impl Implementation) GetCapabilities(
	ctx context.Context,
	_ *reconciler.ReconcilerHooksCapabilitiesRequest,
) (*reconciler.ReconcilerHooksCapabilitiesResult, error) {
	return &reconciler.ReconcilerHooksCapabilitiesResult{
		ReconcilerCapabilities: []*reconciler.ReconcilerHooksCapability{
			{
				Kind: reconciler.ReconcilerHooksCapability_KIND_CLUSTER,
			},
		},
	}, nil
}

// Pre is called before cluster reconciliation
func (impl Implementation) Pre(
	ctx context.Context,
	request *reconciler.ReconcilerHooksRequest,
) (*reconciler.ReconcilerHooksResult, error) {
	logger := log.FromContext(ctx).WithName("reconciler-pre")

	// Check if downgrade was requested and handle instance scaling
	if err := impl.handleDowngradeScaling(ctx, request, logger); err != nil {
		logger.Error(err, "Failed to handle downgrade scaling")
	}

	return &reconciler.ReconcilerHooksResult{
		Behavior: reconciler.ReconcilerHooksResult_BEHAVIOR_CONTINUE,
	}, nil
}

// Post is called after cluster reconciliation
func (impl Implementation) Post(
	ctx context.Context,
	request *reconciler.ReconcilerHooksRequest,
) (*reconciler.ReconcilerHooksResult, error) {
	logger := log.FromContext(ctx).WithName("reconciler-post")

	// Check if this is a cluster ready state and we need to restore
	if impl.shouldRestore(request, logger) {
		if err := impl.executeRestore(ctx, request, logger); err != nil {
			logger.Error(err, "Restore failed")
			return &reconciler.ReconcilerHooksResult{
				Behavior: reconciler.ReconcilerHooksResult_BEHAVIOR_CONTINUE,
			}, nil
		}
		logger.Info("=== RESTORE COMPLETED SUCCESSFULLY ===")
	}

	return &reconciler.ReconcilerHooksResult{
		Behavior: reconciler.ReconcilerHooksResult_BEHAVIOR_CONTINUE,
	}, nil
}

// shouldRestore checks if restore is needed based on downgrade marker file
func (impl Implementation) shouldRestore(request *reconciler.ReconcilerHooksRequest, logger log.Logger) bool {
	clusterName := "freddie" // TODO: extract from request
	markerFile := fmt.Sprintf("/tmp/%s-downgrade-marker", clusterName)

	logger.Info("Checking for marker file", "file", markerFile)

	// List all files in /tmp for debugging
	files, err := os.ReadDir("/tmp")
	if err != nil {
		logger.Error(err, "Failed to read /tmp directory")
	} else {
		logger.Info("Files in /tmp:", "count", len(files))
		for _, file := range files {
			logger.Info("Found file", "name", file.Name())
		}
	}

	_, err = os.Stat(markerFile)
	if err == nil {
		logger.Info("=== MARKER FILE EXISTS ===", "file", markerFile)
		return true
	} else {
		logger.Info("Marker file does not exist", "file", markerFile, "error", err.Error())
		return false
	}
}

// executeRestore performs the restore operation
func (impl Implementation) executeRestore(ctx context.Context, request *reconciler.ReconcilerHooksRequest, logger log.Logger) error {
	// Extract cluster info from request object definition
	clusterName := "freddie" // TODO: extract from request
	namespace := "test"      // TODO: extract from request

	host := fmt.Sprintf("%s-rw.%s.svc.cluster.local", clusterName, namespace)
	// Look for backup files with version suffix (created by lifecycle hook)
	backupFile := fmt.Sprintf("/tmp/%s_backup_17.sql", clusterName)

	// Check if backup file exists
	if _, err := os.Stat(backupFile); os.IsNotExist(err) {
		// Try without version suffix as fallback
		backupFile = fmt.Sprintf("/tmp/%s_backup.sql", clusterName)
		if _, err := os.Stat(backupFile); os.IsNotExist(err) {
			return fmt.Errorf("backup file not found: %s", backupFile)
		}
	}

	// Get superuser password
	password, err := impl.getSuperuserPassword(clusterName, namespace, logger)
	if err != nil {
		return fmt.Errorf("failed to get superuser password: %w", err)
	}

	// Wait for database to be ready
	if err := impl.waitForDatabase(host, password, logger); err != nil {
		return fmt.Errorf("database not ready: %w", err)
	}

	// Execute restore using psql (since we're using pg_dumpall format)
	restoreCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	// Use psql to restore pg_dumpall output (SQL text format)
	cmd := exec.CommandContext(restoreCtx, "psql",
		"-h", host,
		"-p", "5432",
		"-U", "postgres",
		"-d", "postgres", // Connect to postgres database
		"-f", backupFile)
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", password))

	logger.Info("Executing restore", "host", host, "backupFile", backupFile)
	output, err := cmd.CombinedOutput()
	if err != nil {
		logger.Error(err, "Restore failed", "output", string(output))
		return fmt.Errorf("restore failed: %w", err)
	}

	// Update cluster status after successful restore
	if err := impl.updateDowngradeStatus(ctx, request, logger); err != nil {
		logger.Error(err, "Failed to update downgrade status")
	}

	// Cleanup marker
	if err := impl.cleanupMarker(clusterName, namespace, logger); err != nil {
		logger.Error(err, "Failed to cleanup marker")
	}

	logger.Info("Restore completed successfully", "output", string(output))
	return nil
}

// waitForDatabase waits for database readiness
func (impl Implementation) waitForDatabase(host, password string, logger log.Logger) error {
	for i := 0; i < 30; i++ {
		cmd := exec.Command("pg_isready", "-h", host, "-p", "5432", "-U", "postgres")
		cmd.Env = append(cmd.Env, fmt.Sprintf("PGPASSWORD=%s", password))

		if err := cmd.Run(); err == nil {
			logger.Info("Database is ready")
			return nil
		}

		logger.Info("Waiting for database", "attempt", i+1)
		time.Sleep(5 * time.Second)
	}

	return fmt.Errorf("database not ready after 30 attempts")
}

// getSuperuserPassword retrieves postgres password
func (impl Implementation) getSuperuserPassword(clusterName, namespace string, logger log.Logger) (string, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return "", err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "", err
	}

	secretName := fmt.Sprintf("%s-superuser", clusterName)
	secret, err := clientset.CoreV1().Secrets(namespace).Get(context.Background(), secretName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}

	password, exists := secret.Data["password"]
	if !exists {
		return "", fmt.Errorf("password not found")
	}

	return string(password), nil
}

// cleanupMarker removes the downgrade marker file
func (impl Implementation) cleanupMarker(clusterName, namespace string, logger log.Logger) error {
	markerFile := fmt.Sprintf("/tmp/%s-downgrade-marker", clusterName)
	err := os.Remove(markerFile)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	logger.Info("Cleaned up downgrade marker", "file", markerFile)
	return nil
}

// updateDowngradeStatus updates PGDataImageInfo after successful restore
func (impl Implementation) updateDowngradeStatus(ctx context.Context, request *reconciler.ReconcilerHooksRequest, logger log.Logger) error {
	cluster, err := decoder.DecodeClusterLenient(request.GetClusterDefinition())
	if err != nil {
		return fmt.Errorf("failed to decode cluster: %w", err)
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to create config: %w", err)
	}

	scheme := runtime.NewScheme()
	if err := apiv1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("failed to add scheme: %w", err)
	}

	c, err := client.New(config, client.Options{
		Scheme: scheme,
	})
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	// Retry logic for resource version conflicts
	for i := 0; i < 3; i++ {
		// Fetch fresh cluster to get latest resource version
		freshCluster := &apiv1.Cluster{}
		if err := c.Get(ctx, client.ObjectKey{
			Namespace: cluster.Namespace,
			Name:      cluster.Name,
		}, freshCluster); err != nil {
			return fmt.Errorf("failed to get fresh cluster: %w", err)
		}

		// Get current image major version
		imageParts := strings.Split(freshCluster.Status.Image, ":")
		if len(imageParts) < 2 {
			return nil // Invalid image format, skip
		}
		versionParts := strings.Split(imageParts[1], ".")
		if len(versionParts) < 1 {
			return nil // Invalid version format, skip
		}
		imageMajor, err := strconv.Atoi(versionParts[0])
		if err != nil {
			return nil // Failed to parse, skip
		}

		// Check if PGDataImageInfo needs update
		if freshCluster.Status.PGDataImageInfo != nil && freshCluster.Status.PGDataImageInfo.MajorVersion == imageMajor {
			return nil // Already aligned
		}

		// Update with fresh cluster
		freshCluster.Status.PGDataImageInfo = &apiv1.ImageInfo{
			Image:        freshCluster.Status.Image,
			MajorVersion: imageMajor,
		}

		if err := c.Status().Update(ctx, freshCluster); err != nil {
			logger.Info("Retry status update", "attempt", i+1, "error", err.Error())
			time.Sleep(time.Second)
			continue
		}

		logger.Info("Updated PGDataImageInfo after restore", "majorVersion", imageMajor)
		return nil
	}

	return fmt.Errorf("failed to update status after 3 retries")
}

// handleDowngradeScaling checks for downgrade and scales instances to 1 if needed
func (impl Implementation) handleDowngradeScaling(ctx context.Context, request *reconciler.ReconcilerHooksRequest, logger log.Logger) error {
	cluster, err := decoder.DecodeClusterLenient(request.GetClusterDefinition())
	if err != nil {
		return fmt.Errorf("failed to decode cluster: %w", err)
	}

	// Check if downgrade marker exists
	markerFile := fmt.Sprintf("/tmp/%s-downgrade-marker", cluster.Name)
	if _, err := os.Stat(markerFile); os.IsNotExist(err) {
		return nil // No downgrade requested
	}

	// Check if instances > 1
	if cluster.Spec.Instances <= 1 {
		return nil // Already at 1 instance
	}

	logger.Info("Downgrade detected with multiple instances, scaling to 1", "currentInstances", cluster.Spec.Instances)

	// Create Kubernetes client
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to create config: %w", err)
	}

	scheme := runtime.NewScheme()
	if err := apiv1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("failed to add scheme: %w", err)
	}

	c, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	// Update cluster spec to 1 instance
	cluster.Spec.Instances = 1
	if err := c.Update(ctx, cluster); err != nil {
		return fmt.Errorf("failed to update cluster instances: %w", err)
	}

	logger.Info("Successfully scaled cluster to 1 instance for downgrade")
	return nil
}
