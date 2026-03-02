package ai.chronon.integrations.cloud_azure

import ai.chronon.spark.submission.StorageClient
import com.azure.storage.blob.BlobServiceClient

import java.io.ByteArrayInputStream
import java.net.URI

class AzureBlobStorageClient(blobServiceClient: BlobServiceClient) extends StorageClient {

  override def downloadObjectToMemory(bucketName: String, objectName: String): Array[Byte] = {
    val blobClient = blobServiceClient.getBlobContainerClient(bucketName).getBlobClient(objectName)
    val outputStream = new java.io.ByteArrayOutputStream()
    blobClient.downloadStream(outputStream)
    outputStream.toByteArray
  }

  override def downloadObjectToMemory(objectPath: String): Array[Byte] = {
    val (container, blobName) = parsePath(objectPath)
    downloadObjectToMemory(container, blobName)
  }

  override def listFiles(bucketName: String, prefix: String): Iterator[String] = {
    import scala.jdk.CollectionConverters._
    blobServiceClient
      .getBlobContainerClient(bucketName)
      .listBlobsByHierarchy(prefix)
      .iterator()
      .asScala
      .map(_.getName)
  }

  override def listFiles(bucketPath: String): Iterator[String] = {
    val (container, prefix) = parsePath(bucketPath)
    listFiles(container, prefix)
  }

  override def fileExists(bucketName: String, objectName: String): Boolean = {
    blobServiceClient.getBlobContainerClient(bucketName).getBlobClient(objectName).exists()
  }

  override def fileExists(objectPath: String): Boolean = {
    val (container, blobName) = parsePath(objectPath)
    fileExists(container, blobName)
  }

  override def upload(objectPath: String, content: Array[Byte]): Unit = {
    val (container, blobName) = parsePath(objectPath)
    val blobClient = blobServiceClient.getBlobContainerClient(container).getBlobClient(blobName)
    blobClient.upload(new ByteArrayInputStream(content), content.length, true)
  }

  private[cloud_azure] def parsePath(path: String): (String, String) = {
    val uri = URI.create(path)
    val container = uri.getUserInfo
    val blobPath = uri.getPath.stripPrefix("/")
    require(container != null && container.nonEmpty,
            s"Expected abfss://container@account.dfs.core.windows.net/path, got: $path")
    require(blobPath.nonEmpty, s"Expected abfss://container@account.dfs.core.windows.net/path, got: $path")
    (container, blobPath)
  }
}
