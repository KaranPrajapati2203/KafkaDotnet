using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using System;
using System.Threading;
using System.Threading.Tasks;

class Program
{
    private static int messageCounter = 1;

    public static async Task Main(string[] args)
    {
        var configuration = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

        var config = new ProducerConfig
        {
            BootstrapServers = $"{configuration["BootstrapService:Server"]}:{configuration["BootstrapService:Port"]}"
        };

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            while (true)
            {
                var message = GenerateMessage();

                try
                {
                    await producer.ProduceAsync(configuration["BootstrapService:Topic"], new Message<Null, string> { Value = message });
                    Console.WriteLine($"Produced message: {message}");
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }

                Thread.Sleep(1000);
            }
        }
    }

    private static string GenerateMessage()
    {
        var random = new Random();
        var message = new
        {
            Counter = messageCounter++,
            Timestamp = DateTime.Now,
            Content = $"Random message {random.Next(1, 1000)}"
        };

        return JsonConvert.SerializeObject(message);
    }
}
