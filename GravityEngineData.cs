 
namespace GEData.Analytics
{
    /// <summary>
    /// Entry of SDK
    /// </summary>
    public class GEAnalytics
    {
        public const string LibVersion = "1.0.0";
        public const string LibName = "C#";
        private readonly IGEConsumer _consumer;


        /// <summary>
        /// init SDK with consumer
        /// </summary>
        /// <param name="consumer">data consumer</param>
        public GEAnalytics(IGEConsumer consumer)
        {
            GELog.Log("Init SDK");
            _consumer = consumer;
        }

        /// <summary>
        /// report ordinary event
        /// </summary>
        /// <param name="client_id">client_id</param>
        /// <param name="event_name">event name</param>
        /// <param name="properties">properties</param>
        /// <exception cref="SystemException">SystemException</exception>
        public void Track(string client_id, string event_name, Dictionary<string, object> properties)
        {
           Add(client_id, "track", event_name, properties);
        }


        /// <summary>
        /// set user properties. would overwrite existing names
        /// </summary>
        /// <param name="client_id">client_id</param>
        /// <param name="properties">properties</param>
        public void UserSet(string client_id, Dictionary<string, object> properties)
        {
           Add(client_id, "profile","profile_set", properties);
        }

        /// <summary>
        /// clear the user properties of users
        /// </summary>
        /// <param name="client_id">client_id</param>
        /// <param name="properties">propertie keys list</param>
        public void UserUnSet(string client_id, List<string> properties)
        {
            var props = properties.ToDictionary<string, string, object>(property => property, property => 0);
           Add(client_id, "profile","profile_unset", props);
        }

        /// <summary>
        /// set user properties, If such property had been set before, this message would be ingored
        /// </summary>
        /// <param name="client_id">client_id</param>
        /// <param name="properties">properties</param>
        public void UserSetOnce(string client_id, Dictionary<string, object> properties)
        {
           Add(client_id, "profile","profile_set_once", properties);
        }


        /// <summary>
        /// to accumulate operations against the property
        /// </summary>
        /// <param name="client_id">client_id</param>
        /// <param name="properties">properties</param>
        public void UserIncrement(string client_id, Dictionary<string, object> properties)
        {
           Add(client_id, "profile","profile_increment", properties);
        }


        /// <summary>
        /// to set max val against the property
        /// </summary>
        /// <param name="client_id">client_id</param>
        /// <param name="properties">properties</param>
        public void UserMax(string client_id, Dictionary<string, object> properties)
        {
           Add(client_id, "profile","profile_number_max", properties);
        }

        /// <summary>
        /// to set min val against the property        
        /// </summary>
        /// <param name="client_id">client_id</param>
        /// <param name="properties">properties</param>
        public void UserMin(string client_id, Dictionary<string, object> properties)
        {
           Add(client_id, "profile","profile_number_min", properties);
        }


        /// <summary>
        /// to add user properties of array type
        /// </summary>
        /// <param name="client_id">client_id</param>
        /// <param name="properties">properties</param>
        public void UserAppend(string client_id, Dictionary<string, object> properties)
        {
           Add(client_id, "profile","profile_append", properties);
        }

        /// <summary>
        /// append user properties to array type by unique
        /// </summary>
        /// <param name="client_id">client_id</param>
        /// <param name="properties">properties</param>
        public void UserUniqAppend(string client_id, Dictionary<string, object> properties)
        {
           Add(client_id, "profile","profile_uniq_append", properties);
        }

        /// <summary>
        /// delete a user, This operation cannot be undone
        /// </summary>
        /// <param name="client_id">client_id</param>
        public void UserDelete(string client_id)
        {
           Add(client_id, "profile","profile_delete", new Dictionary<string, object>());
        }

        /// <summary>
        /// report data immediately
        /// </summary>
        public void Flush()
        {
            GELog.Log("SDK flush data.");
            _consumer.Flush();
        }

        /// <summary>
        /// close and exit sdk
        /// </summary>
        public void Close()
        {
            _consumer.Close();
            GELog.Log("SDK close.");
        }


        private void Add(string client_id, string type, string event_name, Dictionary<string, object> properties)
        {
            var evt = new Dictionary<string, object>();
            var p = new Dictionary<string, object>();
            evt["client_id"]=client_id;
           
            var event_list= new List<object>();

            p["type"] = type;
            p["event"] = event_name; 
            
            long millisecondsSinceEpoch = (DateTime.UtcNow.Ticks - 621355968000000000) / 10000;
            p["time"] = millisecondsSinceEpoch;
            if (type == "track")
            {
                properties.Add("$lib", GEAnalytics.LibName);
                properties.Add("$lib_version", GEAnalytics.LibVersion);
            }
            p["properties"] = properties;
            event_list.Add(p);
            evt["event_list"] = event_list;
            _consumer.Send(evt);
        }
    }

}