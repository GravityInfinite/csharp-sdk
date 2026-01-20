using  GEData.Analytics;

namespace DemoNamespace

{
    class Program
    {
        static void Main(string[] args)

        {
            GELog.Enable = true;

            String serverUrl = "https://backend.gravity-engine.com/event_center/api/v1/event/collect/?access_token=___XXX___";
            GEAnalytics ge = new(new GEBatchConsumer(serverUrl, 200,2,true, false));

            //GEAnalytics ge = new(new GEDebugConsumer(serverUrl));


            String client_id = "_test_client_id_0";

            for (int i = 0; i < 30; i = i + 1)
            {
                Dictionary<string, Object> dic2 = new Dictionary<string, object>();
                dic2.Add("idx", i);
                dic2.Add("create_date", Convert.ToDateTime("2019-7-8 20:23:22"));
                dic2.Add("group_no", "T22514");
                dic2.Add("group_title", "title");
                dic2.Add("group_purchase_id", 438);
                dic2.Add("group_order_is_vip", 3);
                dic2.Add("service_id", 0);
                ge.Track(client_id, "$AdClick", dic2);
            }



            Dictionary<string, Object> dic3 = new Dictionary<string, object>();
            dic3.Add("user_name", "xxx");
            ge.UserSet(client_id, dic3);


            Dictionary<string, Object> dic4 = new Dictionary<string, object>();
            dic4.Add("TotalRevenue", 648);
            ge.UserIncrement(client_id, dic4);

            Dictionary<string, Object> dic4_1 = new Dictionary<string, object>();
            dic4_1.Add("TotalRevenue", 1);
            ge.UserMin(client_id, dic4_1);

            Dictionary<string, Object> dic4_2 = new Dictionary<string, object>();
            dic4_2.Add("TotalRevenue", 1000);
            ge.UserMax(client_id, dic4_2);

            Dictionary<string, Object> dic5 = new Dictionary<string, object>();
            dic5.Add("prop_set_once", "xxx");
            ge.UserSetOnce(client_id, dic5);


            List<string> list2 = new List<string>();
            list2.Add("user_name");
            ge.UserUnSet(client_id, list2);


            Dictionary<string, object> dictionary = new Dictionary<string, object>();
            List<string> list6 = new List<string>();
            list6.Add("a");
            list6.Add("b");
            list6.Add("a");
            dictionary.Add("prop_list_type", list6);
            ge.UserAppend(client_id, dictionary);

            ge.UserUniqAppend(client_id, dictionary);


            ge.UserDelete(client_id);
  
            ge.Flush();
            ge.Close();

        }
    }
}
