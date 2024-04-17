const { MongoClient } = require('mongodb');
const { createClient } = require('redis');

const EXPIRATION_TIME = 60; // Cache expiration time in seconds

// Function to retrieve health records from cache
async function getHealthRecordsFromCache(userId) {
  const client = createClient();
  client.on('error', (err) => console.log('Redis Client Error', err));
  await client.connect();

  const key = `health_records:${userId}`;

  try {
    const cached = await client.exists(key + ':cached');
    if (cached) {
      const healthRecords = [];
      const recordKeys = await client.lRange(key, 0, -1);
      for (const recordKey of recordKeys) {
        const recordData = await client.hGetAll(recordKey);
        healthRecords.push(recordData);
      }
      return healthRecords;
    } else {
      return null;
    }
  } finally {
    await client.disconnect();
  }
}

// Function to save health records to cache
async function saveHealthRecordsToCache(userId, healthRecords) {
  const client = createClient();
  client.on('error', (err) => console.log('Redis Client Error', err));
  await client.connect();

  const key = `health_records:${userId}`;

  try {
    for (const record of healthRecords) {
      const recordKey = `health_record:${userId}:${record._id}`;
      await client.hSet(
        recordKey,
        ...Object.entries(record).flat().map((d) => d.toString())
      );
      await client.rPush(key, recordKey);
    }
    await client.set(key + ':cached', '1', 'EX', EXPIRATION_TIME);
    console.log('Health records saved to cache for user', userId);
  } finally {
    await client.disconnect();
  }
}

// Function to retrieve health records from MongoDB
async function getHealthRecordsFromMongo(userId) {
  const client = new MongoClient('mongodb://localhost:27017/');
  await client.connect();
  const coll = client.db('healthTracker').collection('Health_Records');
  const records = await coll.find({ user_id: userId }).toArray();
  await client.close();
  return records;
}

// Main function to get health records, checks cache first
async function getHealthRecords(userId) {
  let records = await getHealthRecordsFromCache(userId);
  if (!records) {
    console.log('No cache found for user health records', userId);
    records = await getHealthRecordsFromMongo(userId);
    console.log('Retrieved health records from MongoDB for user', userId);
    await saveHealthRecordsToCache(userId, records);
  } else {
    console.log('Retrieved health records from cache for user', userId);
  }
  return records;
}

// Example usage within an async IIFE (Immediately Invoked Function Expression)
(async () => {
  try {
    const userId = 123; // Example user ID
    const records = await getHealthRecords(userId);
    console.log(records);
  } catch (error) {
    console.error('An error occurred:', error);
  }
})();
