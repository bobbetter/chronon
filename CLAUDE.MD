# Building
We use the mill build system for this project. A couple of examples of command incantations:
- Clean the build artifacts: `./mill clean`
- Build the whole repo: `./mill __.compile`
- Build a module: `./mill cloud_gcp.compile`
- Run tests in a module: `./mill cloud_gcp.test`
- Run a particular test case inside a test class: `./mill spark.test.testOnly "ai.chronon.spark.kv_store.KVUploadNodeRunnerTest" -- -z "should handle GROUP_BY_UPLOAD_TO_KV successfully"`
- Run specific tests that match a pattern: `./mill spark.test.testOnly "ai.chronon.spark.analyzer.*"`
- List which modules / tasks are available: `./mill resolve _`
 
# Workflow
- Make sure to sanity check compilation works when you’re done making a series of code changes
- When done with compilation checks, make sure to run the related unit tests as well (either for the class or module)
- When applicable, suggest test additions / extensions to go with your code changes
