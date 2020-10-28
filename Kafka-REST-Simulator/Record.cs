using Newtonsoft.Json;

namespace Simulator
{
    internal class Record
    {
        private string key;
        private string message;

        public Record(string message)
        {
            this.message = message;
        }

        public string ToJson()
        {
            return JsonConvert.SerializeObject(message);
        }

    }
}