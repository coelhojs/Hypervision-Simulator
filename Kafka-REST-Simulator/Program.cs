using Microsoft.Extensions.Configuration;
using Newtonsoft.Json.Linq;
using Simulator;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka-REST-Simulator
{
    public class Program
    {
        private static string RESTProxyIP;
        private static string RESTProxyPort;
        public static IConfigurationRoot Configuration { get; set; }

        public static async Task Main(string[] args)
        {
            try
            {
                Console.WriteLine("Simulador de publicações em tópicos Kafka\r");

                if (args.Length > 0)
                {
                    for (int i = 0; i < args.Length; i++)
                    {
                        string value = args[i];

                        switch (value)
                        {
                            case "--ip":
                                RESTProxyIP = args[i + 1];
                                break;
                            case "--port":
                                RESTProxyPort = args[i + 1];
                                break;
                        }
                    }
                }
                else
                {
                    using (StreamReader r = new StreamReader("appsettings.json"))
                    {
                        var appSettings = JObject.Parse(r.ReadToEnd());

                        RESTProxyIP = (string)appSettings["RESTProxyIP"];
                        RESTProxyPort = (string)appSettings["RESTProxyPort"];
                    }
                }

                Console.WriteLine("Selecione o tópico e pressione Enter:");
                Console.WriteLine("1-Meteorological-Alerts + Fire-Alerts\n");
                Console.WriteLine("2-Meteorological-Alerts\n");
                Console.WriteLine("3-Fire-Alerts\n");
                var option = Convert.ToInt32(Console.ReadLine());
                
                Console.WriteLine("Digite a quantidade de mensagens a serem geradas\n");
                var iterations = Convert.ToInt32(Console.ReadLine());
                
                Console.WriteLine("Digite o intervalo em segundos entre o envio de mensagens:\n");
                var interval = Convert.ToInt32(Console.ReadLine()) * 1000;
                
                var FireAlertPublisher = new Publisher(RESTProxyIP, RESTProxyPort, "Fire-Alerts");
                var MeteorologicalAlertPublisher = new Publisher(RESTProxyIP, RESTProxyPort, "Meteorological-Alerts");

                for (int i = 0; i < iterations; i++)
                {
                    switch (option)
                    {
                        case 1:
                            var fireAlert = new MeteorologicalAlert().Json;
                            var meteorologicalAlert = new MeteorologicalAlert().Json;

                            MeteorologicalAlertPublisher.SendMessage(meteorologicalAlert);

                            break;
                        case 2:
                            //await new MeteorologicalDataPublisher(KafkaServer, SchemaRegistryUrl).ExecuteAsync(iterations);
                            break;
                        case 3:
                            //await new FireDataPublisher(KafkaServer, SchemaRegistryUrl).ExecuteAsync(iterations);
                            break;
                    }

                    Thread.Sleep(interval);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Houve um erro na execução: {ex.Message}");
            }
        }
    }
}
