// (The top part of the file with configs is unchanged)
const { Kafka } = require('kafkajs');
const { MongoClient } = require('mongodb');
const kafka = new Kafka({ clientId: 'inventory-service', brokers: ['localhost:9092'] });
const producer = kafka.producer();
const forwardConsumer = kafka.consumer({ groupId: 'inventory-group' });
const compensationConsumer = kafka.consumer({ groupId: 'inventory-compensation-group' });
const mongoUrl = 'mongodb://localhost:27017';
const client = new MongoClient(mongoUrl);
let db;


const handleOrderCreated = async (order) => {
  console.log(`[Forward] Received OrderCreated event for Order ID: ${order.orderId}`);
  console.log(`[Forward] Reserving inventory for Order ID: ${order.orderId}...`);

  const ordersCollection = db.collection('orders');
  await ordersCollection.updateOne(
    { orderId: order.orderId },
    { $set: { status: 'INVENTORY_RESERVED' } }
  );
  console.log(`[MongoDB] Updated order status to INVENTORY_RESERVED for Order ID: ${order.orderId}`);

  // --- START OF DEMO MODIFICATION ---
  if (order.customerId === 'STOP_AT_INVENTORY') {
    console.log(`[DEMO] Order stopped at INVENTORY_RESERVED for customer: ${order.customerId}`);
  } else {
    await producer.send({
      topic: 'inventory',
      messages: [{ key: order.orderId, value: JSON.stringify(order) }]
    });
    console.log(`[Kafka] 'InventoryReserved' event sent for Order ID: ${order.orderId}`);
  }
  // --- END OF DEMO MODIFICATION ---
};

// (The handlePaymentFailed function and the main function are unchanged)
const handlePaymentFailed = async (order) => {
  console.log(`[Compensation] Received PaymentFailed event for Order ID: ${order.orderId}`);
  console.log(`[Compensation] Releasing inventory for Order ID: ${order.orderId}...`);
  const ordersCollection = db.collection('orders');
  await ordersCollection.updateOne(
    { orderId: order.orderId },
    { $set: { status: 'INVENTORY_RELEASED', failureReason: 'Payment was not authorized' } }
  );
  console.log(`[MongoDB] Updated order status to INVENTORY_RELEASED for Order ID: ${order.orderId}`);
};
const main = async () => {
  console.log("Connecting to Kafka and MongoDB...");
  await producer.connect();
  await forwardConsumer.connect();
  await compensationConsumer.connect();
  await client.connect();
  db = client.db('ecommerce');
  console.log("✅ Connected to Kafka (producer & consumers) and MongoDB.");
  await forwardConsumer.subscribe({ topic: 'orders', fromBeginning: true });
  console.log("Subscribed to 'orders' topic for forward processing.");
  await compensationConsumer.subscribe({ topic: 'payment-failed', fromBeginning: true });
  console.log("Subscribed to 'payment-failed' topic for compensation.");
  await forwardConsumer.run({
    eachMessage: async ({ message }) => {
      const order = JSON.parse(message.value.toString());
      await handleOrderCreated(order);
    },
  });
  await compensationConsumer.run({
    eachMessage: async ({ message }) => {
      const order = JSON.parse(message.value.toString());
      await handlePaymentFailed(order);
    },
  });
};
main().catch(console.error);