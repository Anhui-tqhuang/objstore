// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package azure

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake"
	azfilesystem "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/filesystem"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"

	"github.com/thanos-io/objstore/exthttp"
)

// resolveDFSEndpoint returns the DFS endpoint to use for Data Lake Gen2 operations.
// If conf.DFSEndpoint is set, it is used as-is. Otherwise the endpoint is derived
// from conf.Endpoint by replacing the first "blob." with "dfs." — this covers Azure
// public, Azure Gov, Azure China, and the common "{account}.privatelink.blob.*"
// Private Link pattern. Users with non-standard topologies should set DFSEndpoint.
func resolveDFSEndpoint(conf Config) (string, error) {
	if conf.DFSEndpoint != "" {
		return conf.DFSEndpoint, nil
	}
	if !strings.Contains(conf.Endpoint, "blob.") {
		return "", errors.Errorf(
			"cannot derive DFS endpoint from blob endpoint %q: expected to contain \"blob.\"; set dfs_endpoint explicitly",
			conf.Endpoint)
	}
	return strings.Replace(conf.Endpoint, "blob.", "dfs.", 1), nil
}

// DirDelim is the delimiter used to model a directory structure in an object store bucket.
const DirDelim = "/"

// If the Azure Storage Account type is not known, we use the gen1 (azblob) SDK to query the account properties
// then discard the client. If the account supports hierarchical namespaces, it is a Data Lake Gen2 account.
func autodiscoverStorageAccountType(containerClient *container.Client, logger log.Logger, conf Config) (AzStorageAccountType, error) {
	ctx := context.Background()
	accountProps, err := containerClient.GetAccountInfo(ctx, nil)
	if err != nil {
		return AzStorageAccountType_Unset, errors.Wrapf(err, "error autodiscovering Azure storage account type for account: %s", conf.StorageAccountName)
	}

	if accountProps.IsHierarchicalNamespaceEnabled == nil {
		level.Warn(logger).Log("msg", "unable to autodiscover Azure storage account type: IsHierarchicalNamespaceEnabled is nil; assuming gen1 blob", "account", conf.StorageAccountName)
		return AzStorageAccountType_Blob, nil
	}

	if *accountProps.IsHierarchicalNamespaceEnabled {
		level.Info(logger).Log("msg", "autodiscovered Azure Data Lake Storage Gen2 account type", "account", conf.StorageAccountName)
		return AzStorageAccountType_DataLake, nil
	}

	level.Info(logger).Log("msg", "autodiscovered Azure Blob Storage account type", "account", conf.StorageAccountName)
	return AzStorageAccountType_Blob, nil
}

func getDataLakeGen2FilesystemClient(conf Config, wrapRoundtripper func(http.RoundTripper) http.RoundTripper) (*azfilesystem.Client, error) {
	var rt http.RoundTripper
	rt, err := exthttp.DefaultTransport(conf.HTTPConfig)
	if err != nil {
		return nil, err
	}
	if conf.HTTPConfig.Transport != nil {
		rt = conf.HTTPConfig.Transport
	}
	if wrapRoundtripper != nil {
		rt = wrapRoundtripper(rt)
	}

	opt := &azfilesystem.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Retry: policy.RetryOptions{
				MaxRetries:    conf.PipelineConfig.MaxTries,
				TryTimeout:    time.Duration(conf.PipelineConfig.TryTimeout),
				RetryDelay:    time.Duration(conf.PipelineConfig.RetryDelay),
				MaxRetryDelay: time.Duration(conf.PipelineConfig.MaxRetryDelay),
			},
			Telemetry: policy.TelemetryOptions{
				ApplicationID: "Thanos",
			},
			Transport: &http.Client{Transport: rt},
		},
	}

	// Connection strings carry their own endpoint; no URL construction needed.
	if conf.StorageConnectionString != "" {
		return azfilesystem.NewClientFromConnectionString(conf.StorageConnectionString, conf.ContainerName, opt)
	}

	dfsEndpoint, err := resolveDFSEndpoint(conf)
	if err != nil {
		return nil, err
	}
	fileSystemURL := fmt.Sprintf("https://%s.%s/%s", conf.StorageAccountName, dfsEndpoint, conf.ContainerName)

	if conf.StorageAccountKey != "" {
		creds, err := azdatalake.NewSharedKeyCredential(conf.StorageAccountName, conf.StorageAccountKey)
		if err != nil {
			return nil, err
		}

		return azfilesystem.NewClientWithSharedKeyCredential(fileSystemURL, creds, opt)
	}

	cred, err := getTokenCredential(conf)
	if err != nil {
		return nil, err
	}

	return azfilesystem.NewClient(fileSystemURL, cred, opt)
}

func getContainerClient(conf Config, wrapRoundtripper func(http.RoundTripper) http.RoundTripper) (*container.Client, error) {
	var rt http.RoundTripper
	rt, err := exthttp.DefaultTransport(conf.HTTPConfig)
	if err != nil {
		return nil, err
	}
	if conf.HTTPConfig.Transport != nil {
		rt = conf.HTTPConfig.Transport
	}
	if wrapRoundtripper != nil {
		rt = wrapRoundtripper(rt)
	}
	opt := &container.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Retry: policy.RetryOptions{
				MaxRetries:    conf.PipelineConfig.MaxTries,
				TryTimeout:    time.Duration(conf.PipelineConfig.TryTimeout),
				RetryDelay:    time.Duration(conf.PipelineConfig.RetryDelay),
				MaxRetryDelay: time.Duration(conf.PipelineConfig.MaxRetryDelay),
			},
			Telemetry: policy.TelemetryOptions{
				ApplicationID: "Thanos",
			},
			Transport: &http.Client{Transport: rt},
		},
	}

	// Use connection string if set
	if conf.StorageConnectionString != "" {
		containerClient, err := container.NewClientFromConnectionString(conf.StorageConnectionString, conf.ContainerName, opt)
		if err != nil {
			return nil, err
		}
		return containerClient, nil
	}

	containerURL := fmt.Sprintf("https://%s.%s/%s", conf.StorageAccountName, conf.Endpoint, conf.ContainerName)

	// Use shared keys if set
	if conf.StorageAccountKey != "" {
		cred, err := container.NewSharedKeyCredential(conf.StorageAccountName, conf.StorageAccountKey)
		if err != nil {
			return nil, err
		}
		containerClient, err := container.NewClientWithSharedKeyCredential(containerURL, cred, opt)
		if err != nil {
			return nil, err
		}
		return containerClient, nil
	}

	// Otherwise use a token credential
	cred, err := getTokenCredential(conf)

	if err != nil {
		return nil, err
	}

	containerClient, err := container.NewClient(containerURL, cred, opt)
	if err != nil {
		return nil, err
	}

	return containerClient, nil
}

func getTokenCredential(conf Config) (azcore.TokenCredential, error) {
	if conf.ClientSecret != "" && conf.AzTenantID != "" && conf.ClientID != "" {
		return azidentity.NewClientSecretCredential(conf.AzTenantID, conf.ClientID, conf.ClientSecret, &azidentity.ClientSecretCredentialOptions{})
	}

	if conf.UserAssignedID == "" {
		return azidentity.NewDefaultAzureCredential(nil)
	}

	msiOpt := &azidentity.ManagedIdentityCredentialOptions{}
	msiOpt.ID = azidentity.ClientID(conf.UserAssignedID)
	return azidentity.NewManagedIdentityCredential(msiOpt)
}
