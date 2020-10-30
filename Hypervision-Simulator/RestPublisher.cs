using RestSharp;
using System;
using System.Text;

namespace Simulator.REST
{
    public class RestPublisher
    {
        private RestClient _httpClient;

        public string IP;
        public string Port;
        public string Topic;

        public RestPublisher(string ip, string port, string topic)
        {
            IP = ip;
            Port = port;
            Topic = topic;

            _httpClient = new RestClient($"http://{IP}:{Port}/topics/{Topic}");
            _httpClient.Encoding = Encoding.UTF8;
        }

        public void SendMessage(string message)
        {
            var request = new RestRequest();

            request.Method = Method.POST;

            request.AddHeader("Accept", "application/vnd.kafka.v2+json");
            request.AddHeader("Content-Type", "application/vnd.kafka.jsonschema.v2+json");

            request.AddJsonBody(message);

            var response = _httpClient.Execute(request);

            if (response.IsSuccessful)
            {
                Console.WriteLine("Mensagem enviada com sucesso - Tópico: " + Topic);
            }
            else
            {
                Console.WriteLine("Erro no envio da mensagem - Tópico: " + Topic + "Erro: " + response.ErrorMessage);
            }
        }

        //public async Task<string> GetLatestOffset(string topic, string partitionId = "0")
        //{
        //    var result = await _httpClient.GetAsync($"http://{IP}:{Port}/topics/{topic}/partitions/{partitionId}/offsets");

        //    if (result.IsSuccessStatusCode)
        //    {
        //        return result.Content.ReadAsStringAsync().Result;
        //    }
        //    else
        //    {
        //        return "";
        //    }
        //}
    }
}