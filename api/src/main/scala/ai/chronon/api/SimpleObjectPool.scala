package ai.chronon.api

import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig}

import java.time.Duration
import scala.util.Try

/** Simple helper class for managing Apache Commons Pool 2.x
  * Takes a factory function in the constructor for creating objects
  *
  * @param f             Function that creates new instances of type T
  * @param maxTotal      Maximum number of objects in pool (-1 for unlimited)
  * @param maxIdle       Maximum number of idle objects in pool (-1 for unlimited)
  * @param maxWaitMillis Maximum time to wait for an object (ms, -1 for indefinite)
  * @tparam T Type of objects managed by the pool
  */
class SimpleObjectPool[T](f: () => T, maxTotal: Int = 10000, maxIdle: Int = 30, maxWaitMillis: Long = -1L) {

  private val pool: GenericObjectPool[T] = {
    val config = new GenericObjectPoolConfig[T]()
    config.setMaxTotal(maxTotal)
    config.setMaxIdle(maxIdle)
    config.setMaxWait(Duration.ofMillis(maxWaitMillis))
    config.setTestOnBorrow(true)
    config.setTestOnReturn(true)
    config.setBlockWhenExhausted(true)

    new GenericObjectPool[T](new FunctionBasedFactory(), config)
  }

  /** Borrows an object from the pool and executes a function with it
    * Automatically returns the object to the pool when done
    */
  def withResource[R](func: T => R): R = {
    val obj = pool.borrowObject()
    try {
      func(obj)
    } finally {
      pool.returnObject(obj)
    }
  }

  /** Close the pool and release all resources
    */
  def close(): Unit = {
    Try(pool.close())
  }

  // Internal factory that wraps the user's function
  private class FunctionBasedFactory extends BasePooledObjectFactory[T] {
    override def create(): T = f()

    override def wrap(obj: T): PooledObject[T] = new DefaultPooledObject[T](obj)

    // Optional: override these if you need custom lifecycle management
    override def destroyObject(pooledObj: PooledObject[T]): Unit = { // Custom cleanup logic can go here
      super.destroyObject(pooledObj)
    }

    override def validateObject(pooledObj: PooledObject[T]): Boolean = { // Custom validation logic can go here
      pooledObj.getObject != null
    }
  }
}
