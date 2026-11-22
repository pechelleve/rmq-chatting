using System.Text;
using RabbitMQ.Client;

namespace Publisher_cmd
{
    // Connection + messaging service class (based on module section 1.5.1 step 3)
    class RMQ
    {
        public ConnectionFactory connectionFactory;
        public IConnection connection;
        public IModel channel;

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

        public void CreateRMQChannel(string queue_name, string routingKey = "", string exchange_name = "")
        {
            if (connection.IsOpen)
            {
                channel = connection.CreateModel();
                Console.WriteLine("Channel " + (channel.IsOpen ? "Berhasil!" : "Gagal!"));

                // Addition (not in the module): with mandatory=true the broker returns a message
                // it cannot route to any queue, so the publisher can show that it was not delivered.
                channel.BasicReturn += (sender, ea) =>
                {
                    var returned = Encoding.UTF8.GetString(ea.Body.ToArray());
                    Console.WriteLine($"[!] Pesan '{returned}' TIDAK sampai: tidak ada queue bernama '{ea.RoutingKey}' ({ea.ReplyText}).");
                };
            }

            if (channel.IsOpen)
            {
                channel.QueueDeclare(queue: queue_name,
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);
                Console.WriteLine("Queue telah dideklarasikan..");
            }
        }

        public void SendMessage(string tujuan, string msg = "send")
        {
            byte[] responseBytes = Encoding.UTF8.GetBytes(msg); // convert the message string into bytes

            // Default exchange "" routes the message to the queue whose name equals the routing key (tujuan).
            channel.BasicPublish(exchange: "",
                                 routingKey: tujuan,
                                 mandatory: true,
                                 basicProperties: null,
                                 body: responseBytes);
            Console.WriteLine("Pesan: '" + msg + "' telah dikirim.");
        }

        public void Disconnect()
        {
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
        // Main function (based on module section 1.5.1 step 4)
        static void Main(string[] args)
        {
            RMQ rmq = new RMQ();
            Console.WriteLine("Tekan tombol apapun untuk inisialisasi RMQ parameters.");
            Console.ReadKey();
            rmq.InitRMQConnection(); // default parameters: localhost, guest/guest
            Console.WriteLine("Tekan tombol apapun untuk membuka koneksi ke RMQ.");
            Console.ReadKey();
            rmq.CreateRMQConnection(); // open the connection to RabbitMQ
            Console.Write("Masukkan nama queue channel untuk mengirim pesan melalui RMQ.\n>> ");
            string queue_name = Console.ReadLine();
            rmq.CreateRMQChannel(queue_name);

            Console.Write("Masukkan pesan yang akan dikirim atau 'exit' to close.\n");
            Console.Write(">> tujuan: ");
            string tujuan = Console.ReadLine();
            Console.Write(">> pesan: ");
            string inputMsg = Console.ReadLine();

            while (inputMsg != "exit")
            {
                rmq.SendMessage(tujuan, inputMsg);
                Thread.Sleep(200); // give the broker a moment to return an unroutable message
                Console.Write(">> tujuan: ");
                tujuan = Console.ReadLine();
                Console.Write(">> pesan: ");
                inputMsg = Console.ReadLine();
            }
            rmq.Disconnect();
        }
    }
}
