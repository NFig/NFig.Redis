﻿using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using NFig;
using NFig.Redis;

namespace SampleApplication
{
    using Override = SettingValue<Tier, DataCenter>;
    using NFigRedis = NFigRedisStore<SampleSettings, Tier, DataCenter>;
    using NFigStore = NFigStore<SampleSettings, Tier, DataCenter>;

    class Program
    {
        static void Main(string[] args)
        {
            var thread = new Thread(KeepAlive);
            thread.Start(0);

            RedisExample();
        }

        public static void RedisExample()
        {
            var tier = Tier.Prod;
            var dc = DataCenter.Oregon;

            var nfig = new NFigRedis("localhost:6379", 11);
            nfig.SubscribeToAppSettings("Sample", tier, dc, OnSettingsUpdate);
        }

        private static int s_updateInteration = 0;
        public static void OnSettingsUpdate(Exception ex, SampleSettings settings, NFigStore nfig)
        {
            if (ex != null)
                throw ex;

            Console.WriteLine(settings.ApplicationName + " settings updated. Commit: " + settings.Commit);
            Console.WriteLine(settings.ConnectionStrings.AdServer);
            Console.WriteLine(nfig.IsCurrent(settings));
            Console.WriteLine();
            var info = nfig.GetAllSettingInfos(settings.ApplicationName).First(i => i.Name == "ConnectionStrings.AdServer");
            var ac = info.GetActiveValueFor(settings.Tier, settings.DataCenter);

            s_updateInteration++;

            var tier = Tier.Prod;
            var dc = DataCenter.Any;

            if (s_updateInteration == 1)
            {
                nfig.SetOverride(settings.ApplicationName, "ConnectionStrings.AdServer", "connection string in redis", tier, dc);
            }
            else if (s_updateInteration == 2)
            {
                nfig.ClearOverride(settings.ApplicationName, "ConnectionStrings.AdServer", tier, dc);
            }
            else
            {
                Environment.Exit(0);
            }
        }

        static void KeepAlive(object data)
        {
            while (true)
            {
                Thread.Sleep(60000);
            }
        }
    }
}
