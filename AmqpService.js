import amqp from "amqplib";


class AmqpService {
  

  constructor(){
    this.connection = null;
    this.channel = null;    
  }

  async init(client, password, host) {
    try {
      this.connection = await amqp.connect(`amqp://${client}:${password}@${host}/neptun?heartbeat=45`);
      this.channel = await this.connection.createChannel();
      console.log("✅ Connected to RabbitMQ");

      this.connection.on("close", () => {
        console.error("❌ RabbitMQ connection closed. Reconnecting...");
        setTimeout(() => this.init(), 5000);
      });
      return this;
    } catch (err) {
      console.error("❌ RabbitMQ init error:", err);
      setTimeout(() => this.init(), 5000);
    }
  }

  /** PUBLISH DIRECTLY TO QUEUE (legacy compatible) */
  async publishToQueue(queueName, data) {
    try {
      if (process.env.NODE_ENV === "dev" || process.env.NODE_ENV === "test") {
        queueName = `${queueName}-dev`;
      }

      await this.channel.assertQueue(queueName, { durable: true });
      const message = Buffer.from(JSON.stringify(data));
      this.channel.sendToQueue(queueName, message, { persistent: true });

      console.log(`📤 Sent message to queue '${queueName}':`, data);
    } catch (err) {
      console.error("❌ publishToQueue error:", err);
    }
  }

  /** CONSUME MESSAGES FROM A QUEUE */
  async consumeQueue(queueName, callback) {
    try {
      if (process.env.NODE_ENV === "dev" || process.env.NODE_ENV === "test") {
        queueName = `${queueName}-dev`;
      }

      const ok = await this.channel.assertQueue(queueName, { durable: true });
      console.log(`🚀 Waiting for messages on '${queueName}'...`, ok);

      this.channel.consume(queueName, (msg) => {
        if (msg) {
          const data = JSON.parse(msg.content.toString());
          callback(data, msg);          
        }
      });
    } catch (err) {
      console.error("❌ consumeQueue error:", err);
    }
  }

  /** 🔁 FANOUT PUBLISH (BROADCAST TO ALL BOUND QUEUES) */
  async publishFanout(exchangeName, data) {
    try {
      await this.channel.assertExchange(exchangeName, "fanout", {
        durable: true,
      });
      this.channel.publish(exchangeName, "", Buffer.from(JSON.stringify(data)));
      console.log(`📡 Published to fanout exchange '${exchangeName}':`, data);
    } catch (err) {
      console.error("❌ publishFanout error:", err);
    }
  }

  /** 🔁 FANOUT CONSUMER (EACH CONSUMER GETS ITS OWN QUEUE) */
  async consumeFanout(exchangeName, callback) {
    try {
      await this.channel.assertExchange(exchangeName, "fanout", {
        durable: true,
      });

      const { queue } = await this.channel.assertQueue("", { exclusive: true });
      await this.channel.bindQueue(queue, exchangeName, "");

      console.log(
        `📡 Listening on fanout exchange '${exchangeName}' (queue: ${queue})`
      );
      this.channel.prefetch(1);

      this.channel.consume(queue, (msg) => {
        if (msg) {
          const data = JSON.parse(msg.content.toString());
          callback(data, msg);          
        }
      });
    } catch (err) {
      console.error("❌ consumeFanout error:", err);
    }
  }

  async messageCount(queueName, cb){
    console.log("process.env.NODE_ENV", process.env.NODE_ENV);
    if( process.env.NODE_ENV == "dev" || process.env.NODE_ENV == "test" ){
      queueName = `${queueName}-dev`;
    }

    // Consumer
    /*
    this.channelPromise.then( (ch) => {
      return ch.assertQueue(queueName).then( (ok) => {
        console.log(ok);
        cb(ok);        
      }).catch(console.warn);    
    }).catch(console.warn);
    */
   const ok = await this.channel.assertQueue(queueName, { durable: true });
   return cb(ok);
  }

  async ack(msg){
    this.channel.ack(msg);
  }

  async close() {
    await this.channel.close();
    await this.connection.close();
    console.log("🔒 Closed RabbitMQ connection");
  }
}

export default AmqpService;