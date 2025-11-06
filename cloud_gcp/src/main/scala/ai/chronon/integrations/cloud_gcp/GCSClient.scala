package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark.submission.StorageClient
import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.storage.{BlobId, Storage, StorageOptions}

import scala.jdk.CollectionConverters._

class GCSClient(storageClient: Storage) extends StorageClient {

  override def listFiles(bucketName: String, prefix: String): Iterator[String] = {
    val blobs = storageClient.list(bucketName, BlobListOption.prefix(prefix))
    blobs
      .iterateAll()
      .iterator()
      .asScala
      .map(_.getName)
  }

  override def listFiles(objectPath: String): Iterator[String] = {
    val blobId = BlobId.fromGsUtilUri(objectPath)
    listFiles(blobId.getBucket, blobId.getName)
  }

  override def fileExists(bucketName: String, objectName: String): Boolean = {
    val blob = Option(storageClient.get(bucketName, objectName))
    if (blob.isEmpty) {
      false
    } else {
      blob.get.exists()
    }
  }

  override def fileExists(objectPath: String): Boolean = {
    val blobId = BlobId.fromGsUtilUri(objectPath)
    fileExists(blobId.getBucket, blobId.getName)
  }

  override def downloadObjectToMemory(bucketName: String, objectName: String): Array[Byte] = {
    storageClient.readAllBytes(bucketName, objectName)
  }

  override def downloadObjectToMemory(objectPath: String): Array[Byte] = {
    val blobId = BlobId.fromGsUtilUri(objectPath)
    downloadObjectToMemory(blobId.getBucket, blobId.getName)
  }
}

object GCSClient {
  def apply(storageClient: Storage): GCSClient = {
    new GCSClient(storageClient)
  }

  def apply(): GCSClient = {
    val gcsStorageClient = StorageOptions.getDefaultInstance.getService
    new GCSClient(gcsStorageClient)
  }

  def apply(projectId: String): GCSClient = {
    val gcsStorageClient = StorageOptions.newBuilder().setProjectId(projectId).build().getService
    new GCSClient(gcsStorageClient)
  }
}
