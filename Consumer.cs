using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Globalization;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Web;

namespace GEData.Analytics
{
    /// <summary>
    /// Consumer interface
    /// </summary>
    public interface IGEConsumer
    {
        void Send(Dictionary<string, object> message);

        void Flush();

        void Close();

    }

    /// <summary>
    /// Report data by http
    /// </summary>
    public class GEBatchConsumer : IGEConsumer
    {
        protected const int MaxFlushBatchSize = 20;
        protected const int DefaultTimeOutSecond = 30;

        private readonly List<Dictionary<string, object>> _messageList;


        private readonly string _url;
        private readonly int _batchSize;
        private readonly int _requestTimeoutMillisecond;
        private readonly bool _throwException;
        private readonly bool _compress;
        private readonly HttpClient _httpClient = new HttpClient();


        /// <summary>
        /// init BatchConsumer
        /// </summary>
        /// <param name="serverUrl">server url</param>
        /// <param name="batchSize">flush event count each time</param>
        /// <param name="requestTimeoutSecond">http timeout</param>
        /// <param name="throwException">throw exception or not</param>
        /// <param name="compress">http compress or not</param>
        public GEBatchConsumer(string serverUrl, int batchSize, int requestTimeoutSecond,
            bool throwException, bool compress = true)
        {
            _messageList = new List<Dictionary<string, object>>();
            _url = serverUrl;
            this._batchSize = Math.Min(MaxFlushBatchSize, batchSize);
            this._throwException = throwException;
            this._compress = compress;
            this._requestTimeoutMillisecond = requestTimeoutSecond * 1000;

            _httpClient.Timeout = TimeSpan.FromMilliseconds(_requestTimeoutMillisecond);
            _httpClient.DefaultRequestHeaders.TryAddWithoutValidation("compress", _compress ? "gzip" : "none");
            _httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Content-Type", "application/json");
        }

        public void Send(Dictionary<string, object> message)
        {
            lock (_messageList)
            {
                _messageList.Add(message);

                if (_messageList.Count >= _batchSize)
                {
                    Flush();
                }
            }
        }

        public void Flush()
        {
            lock (_messageList)
            {

                var client_id_map = new Dictionary<string, object>();

                while (_messageList.Count != 0)
                {
                    var batchRecordCount = Math.Min(_batchSize, _messageList.Count);
                    var batchList = _messageList.GetRange(0, batchRecordCount);
                    var allOK = true;
                    for (int i = 0; i < batchRecordCount; i++)
                    {
                        var j = batchList[i];
                        string client_id = j.ContainsKey("client_id") ? j["client_id"].ToString() : "";
                        GELog.Log("client_id: {0}", client_id);


                        if (client_id=="")
                        {
                            throw new SystemException("client_id error");

                        }
                        if (!client_id_map.ContainsKey(client_id))
                        {
                            client_id_map[client_id] = new List<object>();

                        }
                        if (!j.ContainsKey("event_list"))
                        {
                            throw new SystemException("event_list error");

                        }
                        else
                        {
                           for (int k = 0; k < ((List<object>)j["event_list"]).Count; k++)
                           {
                               var evt = ((List<object>)j["event_list"])[k] ;
                                ((List<object>)client_id_map[client_id]).Add(evt);
                            }
                        }
                    }
     
                    foreach (var key in client_id_map.Keys)
                    {
                        var evt_obj = new Dictionary<string, object>();
                        evt_obj["client_id"] = key;
                        evt_obj["event_list"] = client_id_map[key];
                        string sendingData;
                        try
                        {
                            sendingData = JsonConvert.SerializeObject(evt_obj);
                        }
                        catch (Exception exception)
                        {
                           allOK=false;
                            if (_throwException)
                            {
                                throw new SystemException("Failed to serialize data.", exception);
                            }
                            continue;
                        }

                        try
                        {
                            SendToServer(sendingData);
                        }
                        catch (Exception exception)
                        {
                            allOK = false;
                            if (_throwException)
                            {
                                throw new SystemException("Failed to send message with BatchConsumer.", exception);
                            }

                        }
                    }
                    if (allOK)
                    {
                        _messageList.RemoveRange(0, batchRecordCount);
                    }
                }
            }
        }

        private async void SendToServer(string dataStr)
        {
            byte[] dataBytes = _compress ? Gzip(dataStr) : Encoding.UTF8.GetBytes(dataStr);
            try
            {
                StringContent content = new StringContent(dataStr, Encoding.UTF8, "application/json");
                GELog.Log("Send data, _url: {0}", _url);
                HttpResponseMessage response = _httpClient.PostAsync(_url, content).Result;
                var responseString = response.Content.ReadAsStringAsync().Result;
                GELog.Log("Send data, response: {0}", responseString);
                var resultJson = JsonConvert.DeserializeObject<Dictionary<string, object>>(responseString);
                var errorLevel = Convert.ToInt32(resultJson["code"]);
                if (errorLevel != 0)
                {
                    throw new Exception("\n Can't send because :\n" + responseString);
                }
                else
                {
                    GELog.Log("Send data, SUCCESS");
                }
            }
            catch (Exception e)
            {
                GELog.Log("Send data,Exception: {0}", e);
                throw new SystemException(e + "\n Cannot post message to " + this._url);
            }
        }

        private static byte[] Gzip(string inputStr)
        {
            var inputBytes = Encoding.UTF8.GetBytes(inputStr);
            using (var outputStream = new MemoryStream())
            {
                using (var gzipStream = new GZipStream(outputStream, CompressionMode.Compress))
                    gzipStream.Write(inputBytes, 0, inputBytes.Length);
                return outputStream.ToArray();
            }
        }

        public void Close()
        {
            Flush();
        }
         
    }

    /// <summary>
    /// The data is reported one by one, and when an error occurs, the exception will be throwed
    /// </summary>
    public class GEDebugConsumer : IGEConsumer
    {
        private readonly string _url;
        private readonly HttpClient _httpClient = new HttpClient();


        /// <summary>
        /// Init debug consumer
        /// </summary>
        /// <param name="serverUrl">server url</param>
        public GEDebugConsumer(string serverUrl)
        {
            GELog.Log("Init debug consumer. ServerUrl: {0}", serverUrl);

            _url = serverUrl; 

            _httpClient.Timeout = TimeSpan.FromMilliseconds(60000);
            _httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Content-Type", "application/json");
        }

        public  void Send(Dictionary<string, object> message)
        {

            try
            {
                var sendingData = JsonConvert.SerializeObject(message);
                GELog.Log("Send data, request: {0}", sendingData);
                SendToServer(sendingData);
            }
            catch (Exception exception)
            {
                throw new SystemException("Failed to send message with DebugConsumer.", exception);
            }
        }

        private async void SendToServer(string dataStr)
        {
            try
            {
                StringContent content = new StringContent(dataStr, Encoding.UTF8, "application/json");
                GELog.Log("Send data, _url: {0}", _url);
                HttpResponseMessage response = _httpClient.PostAsync(_url, content).Result;
                var responseString = response.Content.ReadAsStringAsync().Result;
                GELog.Log("Send data, response: {0}", responseString);
                var resultJson = JsonConvert.DeserializeObject<Dictionary<string, object>>(responseString);
                var errorLevel = Convert.ToInt32(resultJson["code"]);
                if (errorLevel != 0)
                {
                    throw new Exception("\n Can't send because :\n" + responseString);
                }
                else
                {
                    GELog.Log("Send data, SUCCESS");
                }
            }
            catch (Exception e)
            {
                GELog.Log("Send data,Exception: {0}", e);
                throw new SystemException(e + "\n Cannot post message to " + this._url);
            }
        }

        public void Flush()
        {
        }

        public void Close()
        {
        }

    }

    /// <summary>
    /// Log module
    /// </summary>
    public class GELog
    {
        public static bool Enable { get; set; }

        public static void Log(string format, params object[] args)
        {
            if (Enable)
            {
                string prefix = string.Format("[GEData][{0}] ", (DateTime.Now).ToString("yyyy-MM-dd HH:mm:ss.fff"));
                if (args != null)
                {
                    Console.WriteLine(prefix + format, args);
                }
                else
                {
                    Console.WriteLine(prefix + format, null, null);
                }
            }
        }
    }

}