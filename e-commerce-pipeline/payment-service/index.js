// (The top part of the file with configs is unchanged)
const { Kafka } = require('kafkajs');
const { MongoClient } = require('mongodb');
const kafka = new Kafka({ clientId: 'payment-service', brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'payment-group' });
const producer = kafka.producer();
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
  await consumer.subscribe({ topic: 'inventory', fromBeginning: true });
  console.log("Subscribed to 'inventory' topic.");

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const order = JSON.parse(message.value.toString());
      console.log(`[Kafka] Received InventoryReserved event for Order ID: ${order.orderId}`);
      const ordersCollection = db.collection('orders');
      
      if (order.customerId === 'FAIL-CUSTOMER') {
        // (This logic is unchanged)
        console.log(`Payment FAILED for Order ID: ${order.orderId}`);
        await ordersCollection.updateOne({ orderId: order.orderId }, { $set: { status: 'PAYMENT_FAILED', failureReason: 'Insufficient funds' } });
        console.log(`[MongoDB] Updated order status to PAYMENT_FAILED for Order ID: ${order.orderId}`);
        await producer.send({ topic: 'payment-failed', messages: [{ key: order.orderId, value: JSON.stringify(order) }] });
        console.log(`[Kafka] 'PaymentFailed' event sent for Order ID: ${order.orderId}`);
      } else {
        // PAYMENT SUCCEEDED
        console.log(`Authorizing payment for Order ID: ${order.orderId}...`);
        await ordersCollection.updateOne({ orderId: order.orderId }, { $set: { status: 'PAYMENT_AUTHORIZED' } });
        console.log(`[MongoDB] Updated order status to PAYMENT_AUTHORIZED for Order ID: ${order.orderId}`);
        
        // --- START OF DEMO MODIFICATION ---
        if (order.customerId === 'STOP_AT_PAYMENT') {
            console.log(`[DEMO] Order stopped at PAYMENT_AUTHORIZED for customer: ${order.customerId}`);
        } else {
            await producer.send({
                topic: 'payment',
                messages: [{ key: order.orderId, value: JSON.stringify(order) }]
            });
            console.log(`[Kafka] 'PaymentAuthorized' event sent for Order ID: ${order.orderId}`);
        }
        // --- END OF DEMO MODIFICATION ---
      }
    },
  });
};
main().catch(console.error);