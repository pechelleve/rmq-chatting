using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Consumer_cmd
{
    // Connection + messaging service class (based on module section 1.5.2 step 3)
    class RMQ
    {
        public ConnectionFactory connectionFactory;
        public IConnection connection;
        public IModel channel;
        private bool isReceiving = false;

        public void InitRMQConnection(string host = "localhost", int port = 5672, string user = "guest", string pass = "guest")
        {
            connectionFactory = new ConnectionFactory();
            connectionFactory.HostName = host;
            connectionFactory.Port = port;
            connectionFactory.UserName = user;
            connectionFactory.Password = pass;
        }

        public void CreateRMQConnection()
        {
            connection = connectionFactory.CreateConnection();
            Console.WriteLine("Koneksi " + (connection.IsOpen ? "Berhasil!" : "Gagal!"));
        }

        public void WaitingMessage(string queue_name)
        {
            // The module wraps the channel in "using" and then closes it again in Disconnect(),
            // which closes it twice. Here the channel is opened once and closed once in Disconnect().
            channel = connection.CreateModel();
            channel.QueueDeclare(queue: queue_name,
                                 durable: true,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                // RabbitMQ.Client 6.x delivers the body as ReadOnlyMemory<byte>, so convert it to an array first.
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine(" [x] Pesan diterima: {0}", message);
            };
            channel.BasicConsume(queue: queue_name,
                                 autoAck: true,
                                 consumer: consumer);
            isReceiving = true;

            Console.WriteLine(" Tekan [enter] untuk memutus koneksi.");
            Console.ReadLine();
            Disconnect();
        }

        public void Disconnect()
        {
            isReceiving = false;
            channel.Close();
            channel = null;
            Console.WriteLine("Channel ditutup!");
            if (connection.IsOpen)
            {
                connection.Close();
            }

            Console.WriteLine("Koneksi diputus!");
            connection.Dispose();
            connection = null;
        }
    }

    class Program
    {
        // Main function (based on module section 1.5.2 step 4)
        static void Main(string[] args)
        {
            RMQ rmq = new RMQ();
            Console.WriteLine("Tekan tombol apapun untuk inisialisasi RMQ parameters.");
            Console.ReadKey();
            rmq.InitRMQConnection(); // default parameters: localhost, guest/guest
            Console.WriteLine("Tekan tombol apapun untuk membuka koneksi ke RMQ.");
            Console.ReadKey();
            rmq.CreateRMQConnection(); // open the connection to RabbitMQ
            Console.Write("Masukkan nama queue channel untuk menerima pesan melalui RMQ.\n>> ");
            string queue_name = Console.ReadLine();
            Console.WriteLine("Menunggu pesan masuk...");
            rmq.WaitingMessage(queue_name);
        }
    }
}
