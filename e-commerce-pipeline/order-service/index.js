const express = require('express');
const { Kafka } = require('kafkajs');
const { MongoClient } = require('mongodb');
const { v4: uuidv4 } = require('uuid');

const app = express();
const port = 3000;

// Configs are unchanged
const kafka = new Kafka({ clientId: 'order-service', brokers: ['localhost:9092'] });
const producer = kafka.producer();
const mongoUrl = 'mongodb://localhost:27017';
const client = new MongoClient(mongoUrl);
let db;

app.use(express.json());

const main = async () => {
  await producer.connect();
  await client.connect();
  db = client.db('ecommerce');
  console.log("✅ Connected to Kafka producer and MongoDB.");
  app.listen(port, () => {
    console.log(`Order service is listening at http://localhost:${port}`);
  });
};

app.post('/orders', async (req, res) => {
  try {
    const { customerId, items } = req.body;
    if (!customerId || !items || !Array.isArray(items) || items.length === 0) {
      return res.status(400).json({ message: 'Invalid order data.' });
    }

    const order = {
      orderId: uuidv4(),
      customerId,
      items,
      status: 'PENDING',
      createdAt: new Date(),
    };

    const ordersCollection = db.collection('orders');
    await ordersCollection.insertOne(order);
    console.log(`[MongoDB] Order saved with ID: ${order.orderId}`);

    // --- START OF DEMO MODIFICATION ---
    // If the customerId is a special demo ID, we stop here.
    if (order.customerId === 'STOP_AT_PENDING') {
      console.log(`[DEMO] Order stopped at PENDING for customer: ${order.customerId}`);
    } else {
      // Otherwise, we proceed as normal
      await producer.send({
        topic: 'orders',
        messages: [{ key: order.orderId, value: JSON.stringify(order) }],
      });
      console.log(`[Kafka] 'OrderCreated' event sent for Order ID: ${order.orderId}`);
    }
    // --- END OF DEMO MODIFICATION ---

    res.status(201).json({ message: 'Order created successfully', order: order });

  } catch (error) {
    console.error('Failed to process order:', error);
    res.status(500).json({ message: 'Internal Server Error' });
  }
});

main().catch(console.error);