const express = require('express');
const cluster = require('cluster');
const Redis = require('ioredis');
const RateLimit = require('express-rate-limit');
const RateLimitRedis = require('rate-limit-redis');
const Queue = require('bull');
const fs = require('fs');
const path = require('path');

// Redis configuration
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

// Create Redis client
const redisClient = new Redis(REDIS_URL);

// Create task queue
const taskQueue = new Queue('tasks', REDIS_URL);

// Rate limiter configuration
const rateLimiter = new RateLimit({
  store: new RateLimitRedis({
    client: redisClient,
    prefix: 'rate-limit:',
    points: 20, // 20 requests
    duration: 60, // per 1 minute
  }),
  message: 'Too many requests, please try again later.',
  keyGenerator: (req) => req.body.user_id, // Use user_id as the key
});

// Task processing function
async function task(user_id) {
  const logMessage = `${user_id}-task completed at-${Date.now()}\n`;
  fs.appendFile(path.join(__dirname, 'task.log'), logMessage, (err) => {
    if (err) console.error('Error writing to log file:', err);
  });
  console.log(logMessage);
}

// Worker process
if (cluster.isWorker) {
  const app = express();
  app.use(express.json());

  // Apply rate limiter to the task route
  app.post('/api/v1/task', rateLimiter, async (req, res) => {
    const { user_id } = req.body;

    if (!user_id) {
      return res.status(400).json({ error: 'user_id is required' });
    }

    // Add task to the queue
    await taskQueue.add({ user_id }, {
      removeOnComplete: true,
      attempts: 3,
    });

    res.status(202).json({ message: 'Task queued successfully' });
  });

  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => {
    console.log(`Worker ${process.pid} is running on port ${PORT}`);
  });

  // Process tasks from the queue
  taskQueue.process(async (job) => {
    await task(job.data.user_id);
  });

// Master process
} else {
  const numCPUs = require('os').cpus().length;
  console.log(`Master ${process.pid} is running`);

  // Fork workers
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died`);
    cluster.fork(); // Replace the dead worker
  });
}