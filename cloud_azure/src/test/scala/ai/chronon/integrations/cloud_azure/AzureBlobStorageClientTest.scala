package ai.chronon.integrations.cloud_azure

import org.mockito.Mockito.{mock, when, verify, never}
import org.mockito.ArgumentMatchers.any
import com.azure.storage.blob.{BlobClient, BlobContainerClient, BlobServiceClient}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AzureBlobStorageClientTest extends AnyFlatSpec with Matchers {

  private val client = new AzureBlobStorageClient(mock(classOf[BlobServiceClient]))

  it should "parse abfss:// URI into container and blob path" in {
    val (container, blobPath) = client.parsePath("abfss://mycontainer@myaccount.dfs.core.windows.net/some/blob/path")
    container shouldBe "mycontainer"
    blobPath shouldBe "some/blob/path"
  }

  it should "parse abfs:// URI into container and blob path" in {
    val (container, blobPath) = client.parsePath("abfs://mycontainer@myaccount.dfs.core.windows.net/data/file.parquet")
    container shouldBe "mycontainer"
    blobPath shouldBe "data/file.parquet"
  }

  it should "handle deeply nested blob paths" in {
    val (container, blobPath) = client.parsePath("abfss://c@a.dfs.core.windows.net/a/b/c/d/e/f.json")
    container shouldBe "c"
    blobPath shouldBe "a/b/c/d/e/f.json"
  }

  it should "reject URI without container (no @ delimiter)" in {
    an[IllegalArgumentException] should be thrownBy {
      client.parsePath("abfss://myaccount.dfs.core.windows.net/path")
    }
  }

  it should "reject URI without blob path" in {
    an[IllegalArgumentException] should be thrownBy {
      client.parsePath("abfss://mycontainer@myaccount.dfs.core.windows.net")
    }
  }

  it should "reject URI with only trailing slash (empty path)" in {
    an[IllegalArgumentException] should be thrownBy {
      client.parsePath("abfss://mycontainer@myaccount.dfs.core.windows.net/")
    }
  }

  it should "reject completely malformed input" in {
    an[IllegalArgumentException] should be thrownBy {
      client.parsePath("not a uri at all")
    }
  }

  it should "upload without checking or creating the container" in {
    val mockBlobServiceClient = mock(classOf[BlobServiceClient])
    val mockContainerClient = mock(classOf[BlobContainerClient])
    val mockBlobClient = mock(classOf[BlobClient])

    when(mockBlobServiceClient.getBlobContainerClient("mycontainer")).thenReturn(mockContainerClient)
    when(mockContainerClient.getBlobClient("some/path.json")).thenReturn(mockBlobClient)

    val uploadClient = new AzureBlobStorageClient(mockBlobServiceClient)
    uploadClient.upload("abfss://mycontainer@myaccount.dfs.core.windows.net/some/path.json", "hello".getBytes)

    verify(mockBlobClient).upload(any(), any[Long](), any[Boolean]())
    verify(mockContainerClient, never()).exists()
    verify(mockContainerClient, never()).create()
  }
}
