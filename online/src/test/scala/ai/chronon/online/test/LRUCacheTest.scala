package ai.chronon.online.test
import ai.chronon.online.fetcher.LRUCache
import com.github.benmanes.caffeine.cache.{Cache => CaffeineCache}
import org.scalatest.flatspec.AnyFlatSpec

class LRUCacheTest extends AnyFlatSpec {

  it should "gets nothing when there is nothing" in {
    val testCache: CaffeineCache[String, String] = LRUCache[String, String]("testCache")
    assert(testCache.getIfPresent("key") == null)
    assert(testCache.estimatedSize() == 0)
  }

  it should "gets something when there is something" in {
    val testCache: CaffeineCache[String, String] = LRUCache[String, String]("testCache")
    assert(testCache.getIfPresent("key") == null)
    testCache.put("key", "value")
    assert(testCache.getIfPresent("key") == "value")
    assert(testCache.estimatedSize() == 1)
  }

  it should "evicts when something is set" in {
    val testCache: CaffeineCache[String, String] = LRUCache[String, String]("testCache")
    assert(testCache.estimatedSize() == 0)
    assert(testCache.getIfPresent("key") == null)
    testCache.put("key", "value")
    assert(testCache.estimatedSize() == 1)
    assert(testCache.getIfPresent("key") == "value")
    testCache.invalidate("key")
    assert(testCache.estimatedSize() == 0)
    assert(testCache.getIfPresent("key") == null)
  }
}
