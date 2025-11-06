package ai.chronon.integrations.cloud_gcp

import com.google.api.gax.paging.Page
import com.google.cloud.storage.Blob
import org.junit.Assert.assertEquals
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

class GCSClientTest extends AnyFlatSpec with MockitoSugar {
  it should "listFiles successfully given an objectPath" in {
    val mockGcsClient = mock[com.google.cloud.storage.Storage]
    val ziplineGcsClient = GCSClient(mockGcsClient)
    val objectPath = "gs://bucket-name/prefix"

    val mockBlobPage = mock[Page[Blob]]

    when(mockGcsClient.list("bucket-name", com.google.cloud.storage.Storage.BlobListOption.prefix("prefix")))
      .thenReturn(mockBlobPage)

    val mockBlob = mock[Blob]
    val mockIterable = mock[java.lang.Iterable[Blob]]
    when(mockBlobPage.iterateAll()).thenReturn(
      mockIterable
    )
    when(mockIterable.iterator).thenReturn(
      java.util.Arrays
        .asList(
          mockBlob
        )
        .iterator())

    val expectedFileName = "gs://bucket-name/prefix/file1"
    when(mockBlob.getName).thenReturn(expectedFileName)

    val result = ziplineGcsClient.listFiles(objectPath)

    assertEquals(result.next(), expectedFileName)
  }

  it should "fileExists successfully given an objectPath" in {
    val mockGcsClient = mock[com.google.cloud.storage.Storage]
    val ziplineGcsClient = GCSClient(mockGcsClient)

    val objectPath = "gs://bucket-name/prefix/file1"

    val mockBlob = mock[Blob]
    when(mockGcsClient.get("bucket-name", "prefix/file1"))
      .thenReturn(mockBlob)
    when(mockBlob.exists()).thenReturn(true)

    val result = ziplineGcsClient.fileExists(objectPath)
    assert(result)
  }

  it should "downloadObjectToMemory successfully given an objectPath" in {
    val mockGcsClient = mock[com.google.cloud.storage.Storage]
    val ziplineGcsClient = GCSClient(mockGcsClient)

    val objectPath = "gs://bucket-name/prefix/file1"
    val expectedContent = "Hello, World!"
    val expectedBytes = expectedContent.getBytes

    when(mockGcsClient.readAllBytes("bucket-name", "prefix/file1"))
      .thenReturn(expectedBytes)

    val result = ziplineGcsClient.downloadObjectToMemory(objectPath)
    assertEquals(result, expectedBytes)
  }
}
