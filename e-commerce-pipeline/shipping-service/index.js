const { Kafka } = require('kafkajs');
const { MongoClient } = require('mongodb');

// Kafka Configuration
const kafka = new Kafka({
  clientId: 'shipping-service',
  brokers: ['localhost:9092']
});
const consumer = kafka.consumer({ groupId: 'shipping-group' }); // Final unique group ID
const producer = kafka.producer();


// MongoDB Configuration
const mongoUrl = 'mongodb://localhost:27017';
const client = new MongoClient(mongoUrl);
let db;

const main = async () => {
  console.log("Connecting to Kafka and MongoDB...");
  await consumer.connect();
  await producer.connect();
  await client.connect();
  db = client.db('ecommerce');
  console.log("✅ Connected to Kafka (producer & consumer) and MongoDB.");

  // Subscribe to the 'payment' topic
  await consumer.subscribe({ topic: 'payment', fromBeginning: true });
  console.log("Subscribed to 'payment' topic.");

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      // 1. Receive the 'PaymentAuthorized' event
      const order = JSON.parse(message.value.toString());
      console.log(`[Kafka] Received PaymentAuthorized event for Order ID: ${order.orderId}`);

      // 2. Process shipping
      console.log(`Preparing shipment for Order ID: ${order.orderId}...`);
      const shippingInfo = {
          trackingId: `TRK-${Math.floor(Math.random() * 900000) + 100000}`,
          shippedAt: new Date()
      };

      // 3. Update the order in MongoDB with final status and shipping info
      const ordersCollection = db.collection('orders');
      await ordersCollection.updateOne(
        { orderId: order.orderId },
        { $set: { status: 'SHIPPED', shipping: shippingInfo } }
      );
      console.log(`[MongoDB] Updated order status to SHIPPED for Order ID: ${order.orderId}`);

      // 4. Produce the final 'OrderShipped' event
      const finalOrderState = await ordersCollection.findOne({ orderId: order.orderId });
      await producer.send({
        topic: 'shipping',
        messages: [{
          key: order.orderId,
          value: JSON.stringify(finalOrderState)
        }]
      });
      console.log(`[Kafka] 'OrderShipped' event sent for Order ID: ${order.orderId}`);
    },
  });
};

main().catch(error => {
  console.error("Failed to start the shipping service:", error);
  process.exit(1);
});