from model import Resource, Collector, CollectorConfig

# 1. import resources.yml
# 2. parse using the models
# 3. instantiate threadpool
# 4. register instance on shared redis state
# 5. pick-up collection tasks based on config, checking in redis shared state if the resource isn't already being collected
# 6. update the shared state with the collection status when collecting so no other collector picks it up
# 7. when collecting resources, apply transformers and retry conditions if any
# 8. expose on config based generated endpoints
# 9. done!
