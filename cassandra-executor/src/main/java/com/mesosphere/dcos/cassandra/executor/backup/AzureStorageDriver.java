package com.mesosphere.dcos.cassandra.executor.backup;

import static com.mesosphere.dcos.cassandra.executor.backup.azure.PageBlobOutputStream.ORIGINAL_SIZE_KEY;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.security.InvalidKeyException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.CloudPageBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

/**
 * Implements a BackupStorageDriver that provides upload and download
 * functionality to an Azure Storage using Page Blobs.
 * Page Blobs allow for 1TB file sizes.
 * Page Blobs require a Storage Account (but NOT a blob storage account)
 */
public class AzureStorageDriver implements BackupStorageDriver {

    private static final Logger logger = LoggerFactory.getLogger(AzureStorageDriver.class);

    @Override
    public void upload(final BackupRestoreContext ctx) throws Exception {

        final String accountName = ctx.getAccountId();
        final String accountKey = ctx.getSecretKey();
        final String localLocation = ctx.getLocalLocation();
        final String backupName = ctx.getName();
        final String nodeId = ctx.getNodeId();

        final String key = String.format("%s/%s", backupName, nodeId);
        final String containerName = StringUtils.lowerCase(getContainerName(ctx.getExternalLocation()));
        // https://<account_name>.blob.core.windows.net/<container_name>
        final CloudBlobContainer container = getCloudBlobContainer(accountName, accountKey, containerName);

        final File dataDirectory = new File(localLocation);
        if (container == null || !dataDirectory.isDirectory()) {
            logger.error("Error uploading snapshots.  Unable to connect to {}, for container {} or Directory {} doesn't exist.",
                            ctx.getExternalLocation(), containerName, localLocation);
            throw new IOException("Upload Failed. Could not connect to container");
        }

        // Ex: data/<keyspace>/<cf>/snapshots/</snapshot-dir>/<files>
        for (final File keyspaceDir : dataDirectory.listFiles()) {
            if (keyspaceDir.isFile()) {
                // Skip any files in the data directory.
                // Only enter keyspace directory.
                continue;
            }
            logger.info("Entering keyspace: {}", keyspaceDir.getName());
            for (final File cfDir : keyspaceDir.listFiles()) {
                logger.info("Entering column family: {}", cfDir.getName());
                final File snapshotDir = new File(cfDir, "snapshots");
                final File backupDir = new File(snapshotDir, backupName);
                if (!StorageUtil.isValidBackupDir(keyspaceDir, cfDir, snapshotDir, backupDir)) {
                    logger.info("Skipping directory: {}", snapshotDir.getAbsolutePath());
                    continue;
                }
                logger.info("Valid backup directories. KeyspaceDir: {} | ColumnFamilyDir: {} | SnapshotDir: {} | BackupName: {}",
                                keyspaceDir.getAbsolutePath(), cfDir.getAbsolutePath(), snapshotDir.getAbsolutePath(),
                                backupName);

                final Optional<File> snapshotDirectory = StorageUtil.getValidSnapshotDirectory(snapshotDir, backupName);
                logger.info("Valid snapshot directory: {}", snapshotDirectory.isPresent());

                if (snapshotDirectory.isPresent()) {
                    logger.info("Going to upload directory: {}", snapshotDirectory.get().getAbsolutePath());

                    uploadDirectory(snapshotDirectory.get().getAbsolutePath(), container, containerName, key,
                                    keyspaceDir.getName(), cfDir.getName());

                } else {
                    logger.warn("Snapshots directory: {} doesn't contain the current backup directory: {}",
                                    snapshotDir.getName(), backupName);
                }
            }
        }

        logger.info("Done uploading snapshots for backup: {}", backupName);
    }

    private void uploadDirectory(final String localLocation, final CloudBlobContainer azureContainer,
                    final String containerName, final String key, final String keyspaceName, final String cfName)
                    throws Exception {
        final LinkedList<Exception> exceptions = new LinkedList<>();
        logger.info("uploadDirectory() localLocation: {}, containerName: {}, key: {}, keyspaceName: {}, cfName: {}",
                        localLocation, containerName, key, keyspaceName, cfName);

        Files.walk(FileSystems.getDefault().getPath(localLocation)).forEach(filePath -> {
            final File file = filePath.toFile();
            if (file.isFile()) {
                final String fileKey = key + "/" + keyspaceName + "/" + cfName + "/" + file.getName();
                try {
                    uploadFile(azureContainer, fileKey, file);
                } catch (final Exception e) {
                    exceptions.add(e);
                }
            }
        });
        if (exceptions.size() > 0) {
            throw new Exception(exceptions.toString());
        }

    }

    private void uploadFile(final CloudBlobContainer container, final String fileKey, final File sourceFile)
                    throws Exception {

        try {

            logger.info("Initiating upload for file: {} | key: {}", sourceFile.getAbsolutePath(), fileKey);

            final CloudBlockBlob blob = container.getBlockBlobReference(fileKey);
            blob.uploadFromFile(sourceFile.getAbsolutePath());

            logger.info("Upload Complete");

        } catch (StorageException | URISyntaxException | IOException e) {
            logger.error("Unable to store blob", e);
            throw e;
        } catch (final Exception e) {
            logger.error("Exception during Upload", e);
            throw e;
        }
    }

    private void uploadText(final CloudBlobContainer container, final String fileKey, final String textToUpload)
                    throws Exception {

        try {
            logger.info("Initiating upload for destination file: {}", fileKey);

            final CloudBlockBlob blob = container.getBlockBlobReference(fileKey);
            blob.uploadText(textToUpload);
            logger.info("Upload Complete");

        } catch (StorageException | URISyntaxException | IOException e) {
            logger.error("Unable to store blob", e);
            throw e;
        } catch (final Exception e) {
            logger.error("Exception during Upload", e);
            throw e;
        }
    }

    @Override
    public void uploadSchema(final BackupRestoreContext ctx, final String schema) throws Exception {
        // Path: <backupname/node-id/schema.cql>
        final String accountName = ctx.getAccountId();
        final String accountKey = ctx.getSecretKey();
        final String backupName = ctx.getName();
        final String nodeId = ctx.getNodeId();

        final String key = String.format("%s/%s", backupName, nodeId);
        final String containerName = StringUtils.lowerCase(getContainerName(ctx.getExternalLocation()));
        // https://<account_name>.blob.core.windows.net/<container_name>
        final CloudBlobContainer container = getCloudBlobContainer(accountName, accountKey, containerName);

        final BufferedInputStream inputStream = new BufferedInputStream(IOUtils.toInputStream(schema), schema.length());
        if (container == null) {
            logger.error("Error uploading schema.  Unable to connect to {}, for container {}doesn't exist.",
                            ctx.getExternalLocation(), containerName);
            throw new IOException("Upload schema failed. Could not connect to container");
        }
        final String fileKey = key + "/schema.cql";
        uploadText(container, fileKey, schema);
        return;


    }

    @Override
    public void download(final BackupRestoreContext ctx) throws Exception {

        final String accountName = ctx.getAccountId();
        final String accountKey = ctx.getSecretKey();
        final String localLocation = ctx.getLocalLocation();
        final String backupName = ctx.getName();
        final String nodeId = ctx.getNodeId();

        final String containerName = StringUtils.lowerCase(getContainerName(ctx.getExternalLocation()));
        // https://<account_name>.blob.core.windows.net/<container_name>
        final CloudBlobContainer container = getCloudBlobContainer(accountName, accountKey, containerName);

        if (container == null) {
            logger.error("Error uploading snapshots.  Unable to connect to {}, for container {}.",
                            ctx.getExternalLocation(), containerName, localLocation);
            throw new IOException("Download failed : Could not connect to container");
        }
        final String keyPrefix = String.format("%s/%s", backupName, nodeId);

        final Map<String, Long> snapshotFileKeys = getSnapshotFileKeys(container, keyPrefix);
        logger.info("Snapshot files for this node: {}", snapshotFileKeys);

        for (final String fileKey : snapshotFileKeys.keySet()) {
            downloadFile(localLocation, container, fileKey, snapshotFileKeys.get(fileKey));
        }
    }

    private void downloadFile(final String localLocation, final CloudBlobContainer container, final String fileKey,
                    final long originalSize) throws IOException {

        logger.info("Downloading |  Local location {} | fileKey: {} | Size: {}", localLocation, fileKey, originalSize);

        final String fileLocation = localLocation + File.separator + fileKey;
        final File file = new File(fileLocation);
        // Only create parent directory once, if it doesn't exist.
        if (!createParentDir(file)) {
            logger.error("Unable to create parent directories!");
            throw new IOException("Unable to create parent directories : " + file.getParent());
        }

        try (FileOutputStream fileOutputStream = new FileOutputStream(file, true);
                        BufferedOutputStream bos = new BufferedOutputStream(fileOutputStream)) {
            final CloudBlockBlob blockBlob = container.getBlockBlobReference(fileKey);

            blockBlob.download(bos);
            bos.flush();
            logger.info("Download Success : localFilePath: {} | Size: {}", file.getAbsolutePath(), file.length());

        } catch (final Exception e) {
            logger.error("Unable to write file: {}", fileKey, e);
            throw new IOException(e);
        }
    }

    @Override
    public String downloadSchema(final BackupRestoreContext ctx)
                    throws Exception { // Path: <backupname/node-id/schema.cql>
        String schema = "";
        final String accountName = ctx.getAccountId();
        final String accountKey = ctx.getSecretKey();
        final String backupName = ctx.getName();
        final String nodeId = ctx.getNodeId();

        final String key = String.format("%s/%s", backupName, nodeId);
        final String containerName = StringUtils.lowerCase(getContainerName(ctx.getExternalLocation()));
        // https://<account_name>.blob.core.windows.net/<container_name>
        final CloudBlobContainer container = getCloudBlobContainer(accountName, accountKey, containerName);
        if (container == null) {
            logger.error("Error downloading schema.  Unable to connect to {}, for container {}.",
                            ctx.getExternalLocation(), containerName);
            throw new IOException("Unable to download schema");
        }
        final String fileKey = key + "/schema.cql";
        logger.info("Downloading schema. fileKey : {}", fileKey);

        try {

            final CloudBlockBlob blockBlob = container.getBlockBlobReference(fileKey);
            schema = blockBlob.downloadText();
            logger.info("Schema download complete. fileKey : {} | Length : {} ", fileKey, schema.length());

        } catch (final Exception e) {
            logger.error("Unable to download schema : {}", fileKey, e);
            throw e;
        }
        return schema;
    }

    private String getContainerName(final String externalLocation) {
        return externalLocation.substring("azure://".length()).replace("/", "");
    }

    private CloudBlobContainer getCloudBlobContainer(final String accountName, final String accountKey,
                    final String containerName) {
        CloudBlobContainer container = null;

        if (StringUtils.isNotBlank(containerName)) {
            final String storageConnectionString =
                            "DefaultEndpointsProtocol=https" + ";AccountName=" + accountName + ";AccountKey="
                                            + accountKey;

            try {
                final CloudStorageAccount account = CloudStorageAccount.parse(storageConnectionString);
                final CloudBlobClient serviceClient = account.createCloudBlobClient();

                container = serviceClient.getContainerReference(containerName);
                container.createIfNotExists();
            } catch (StorageException | URISyntaxException | InvalidKeyException e) {
                logger.error("Error connecting to container for account {} and container name {}", accountName,
                                containerName, e);
            }
        }

        return container;
    }

    private boolean createParentDir(final File file) {
        final File parentDir = new File(file.getParent());
        logger.info("Checking parent directory : Directory : {} | Exists : {}", parentDir.getAbsolutePath(),
                        parentDir.exists());
        if (!parentDir.isDirectory()) {
            final boolean parentDirCreated = parentDir.mkdirs();
            if (!parentDirCreated) {
                logger.error("Error creating parent directory for file: {}. Skipping to next");
                return false;
            }
        }
        return true;
    }

    private Map<String, Long> getSnapshotFileKeys(final CloudBlobContainer container, final String keyPrefix) {
        Map<String, Long> snapshotFiles = new HashMap<>();

        try {
            for (final ListBlobItem item : container.listBlobs(keyPrefix, true)) {
                if (item instanceof CloudPageBlob) {
                    final CloudPageBlob cloudBlob = (CloudPageBlob) item;
                    snapshotFiles.put(cloudBlob.getName(), getOriginalFileSize(cloudBlob));
                }
                if (item instanceof CloudBlockBlob) {
                    final CloudBlockBlob cloudBlob = (CloudBlockBlob) item;
                    snapshotFiles.put(cloudBlob.getName(), cloudBlob.getProperties().getLength());
                }
            }
        } catch (final StorageException e) {
            logger.error("Unable to retrieve metadata.", e);
            // all or none
            snapshotFiles = new HashMap<>();
        }
        return snapshotFiles;
    }

    private long getOriginalFileSize(final CloudPageBlob pageBlobReference) throws StorageException {
        long size = 0;

        pageBlobReference.downloadAttributes();
        final HashMap<String, String> map = pageBlobReference.getMetadata();
        if (map != null && map.size() > 0) {
            try {
                size = Long.parseLong(map.get(ORIGINAL_SIZE_KEY));
            } catch (final Exception e) {
                logger.error("File size metadata missing or is not a number.");
            }
        }

        return size;
    }
}
