const express = require('express');
const { MongoClient, ObjectId } = require('mongodb');
const cors = require('cors');

const app = express();
const port = 4000; // A new port for our UI backend

// --- Middleware ---
app.use(cors()); // Allow requests from our React app
app.use(express.json());

// --- MongoDB Configuration ---
const mongoUrl = 'mongodb://localhost:27017';
const client = new MongoClient(mongoUrl);
let db;

// --- API Endpoint ---
// This endpoint finds an order by its orderId string
app.get('/order/:orderId', async (req, res) => {
  try {
    const { orderId } = req.params;
    const ordersCollection = db.collection('orders');

    console.log(`Searching for order with ID: ${orderId}`);
    const order = await ordersCollection.findOne({ orderId: orderId });

    if (order) {
      res.status(200).json(order);
    } else {
      res.status(404).json({ message: 'Order not found' });
    }
  } catch (error) {
    console.error('Failed to fetch order:', error);
    res.status(500).json({ message: 'Internal Server Error' });
  }
});

// --- Main connection function ---
const main = async () => {
  await client.connect();
  db = client.db('ecommerce');
  console.log("✅ Connected to MongoDB.");

  app.listen(port, () => {
    console.log(`UI service backend listening at http://localhost:${port}`);
  });
};

main().catch(console.error);