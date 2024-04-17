const { MongoClient } = require('mongodb');
const redis = require('redis');

// MongoDB setup
const uri = "mongodb://localhost:27017";
const client = new MongoClient(uri);

// Redis setup
const redisClient = redis.createClient({
  host: 'localhost',
  port: 6379
});

redisClient.on('error', (err) => {
  console.error('Redis error:', err);
});

async function fetchFromCache(key) {
  return new Promise((resolve, reject) => {
    redisClient.get(key, (err, data) => {
      if (err) {
        reject(err);
      } else {
        resolve(data ? JSON.parse(data) : null);
      }
    });
  });
}

async function saveToCache(key, data, expiration = 3600) {
  redisClient.setex(key, expiration, JSON.stringify(data), (err) => {
    if (err) console.error('Redis error setting cache:', err);
  });
}

async function run() {
  try {
    await client.connect();
    const database = client.db("DB_Project_2");
    const collection = database.collection("users");
    const cacheKey = 'averageWeightByGender';

    // First try to fetch results from cache
    let cachedResults = await fetchFromCache(cacheKey);
    if (cachedResults) {
      console.log("Returning cached results:", cachedResults);
      return cachedResults;
    }

    // MongoDB aggregation pipeline
    const agg = [
      {
        '$lookup': {
          'from': 'health_records',
          'localField': '_id',
          'foreignField': 'user_id',
          'as': 'health_record_info'
        }
      }, {
        '$unwind': {
          'path': '$health_record_info',
          'preserveNullAndEmptyArrays': true
        }
      }, {
        '$group': {
          '_id': '$gender',
          'avgWeight': {
            '$avg': '$health_record_info.weight'
          }
        }
      }
    ];

    const result = await collection.aggregate(agg).toArray();
    console.log("Query results:", result);

    // Cache the result
    await saveToCache(cacheKey, result);

    return result;
  } catch (err) {
    console.error("An error occurred:", err);
  } finally {
    await client.close();
    redisClient.quit(); // Close the Redis connection
  }
}

run().catch(console.dir);
