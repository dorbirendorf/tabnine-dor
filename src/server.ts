import express from 'express';
import bodyParser from 'body-parser';
import amqp, {Channel, Connection} from 'amqplib';

const app = express();
const PORT = 3000;
const TIMETOWAIT = 100
const RABBITMQ_URL = 'amqp://127.0.0.1';
let connection:Connection;
let channel:Channel;

app.use(bodyParser.json());

async function connectToRabbitMQ() {
    try {
        connection = await amqp.connect(RABBITMQ_URL);
        channel = await connection.createChannel();
        console.log('Connected to RabbitMQ');
    } catch (error) {
        console.log('Error connecting to RabbitMQ:', error);
        throw error;
    }
}

// POST /api/{queue_name}
app.post('/api/:queueName', async (req, res) => {
    const queueName = req.params.queueName;
    const message = req.body;

    if (!connection || !channel) {
        return res.status(503).send('RabbitMQ connection unavailable.');
    }

    try {
        await channel.assertQueue(queueName, { durable: true });
        channel.sendToQueue(queueName, Buffer.from(JSON.stringify(message)));
        res.status(201).send('Message added to queue successfully.');
    } catch (error) {
        console.error('Error adding message to queue:', error);
        res.status(500).send('Error adding message to queue.');
    }
});

// GET /api/{queue_name}?timeout={ms}
app.get('/api/:queueName', async (req, res) => {
    const queueName = req.params.queueName;
    const timeout = Number(req.query.timeout) || 10000;

    if (!connection || !channel) {
        return res.status(503).send('RabbitMQ connection unavailable.');
    }

    try {
        const message = await getMessageFromQueue(queueName, timeout);
        if (!message) {
            return res.status(204).send('No message in queue.');
        }
        res.status(200).send(message.content.toString());
    } catch (error) {
        console.error('Error retrieving message from queue:', error);
        res.status(500).send('Error retrieving message from queue.');
    }
});

app.listen(PORT, async () => {
    await connectToRabbitMQ();
    console.log(`Server listening on port 3000`);
});

async function getMessageFromQueue(queueName:string, timeout:number) {
    const startTime = Date.now();
    while (Date.now() - startTime < timeout) {
        const message = await channel.get(queueName, { noAck: true });
        if (message) {
            return message;
        }
        await new Promise(resolve => setTimeout(resolve, TIMETOWAIT));
    }
    return null;
}
