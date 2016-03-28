using System;
using System.Threading.Tasks;
using NUnit.Framework;

namespace NFig.Redis.Tests
{
    public class RedisTests
    {
        [Test]
        public async Task Overrides()
        {
            var updateInteration = 0;
            var lastCommit = "NONE";

            var nfig = new NFigRedisStore<SampleSettings, Tier, DataCenter>("localhost:6379", 11);
            nfig.SubscribeToAppSettings("Sample", Tier.Prod, DataCenter.Oregon, (ex, settings, store) =>
            {
                if (ex != null)
                    throw ex;

                Assert.IsTrue(nfig.IsCurrent(settings));
                Assert.AreNotEqual(lastCommit, settings.Commit);
                lastCommit = settings.Commit;

                updateInteration++;

                Console.WriteLine("Update Iteration " + updateInteration);
                Console.WriteLine(settings.ApplicationName + " settings updated. Commit: " + settings.Commit);
                Console.WriteLine(settings.ConnectionStrings.AdServer);
                Console.WriteLine(nfig.IsCurrent(settings));
                Console.WriteLine();

                var tier = Tier.Prod;
                var dc = DataCenter.Any;

                if (updateInteration == 1)
                {
                    Assert.IsNull(settings.ConnectionStrings.AdServer);
                    nfig.SetOverride(settings.ApplicationName, "ConnectionStrings.AdServer", "connection string in redis", tier, dc);
                }
                else if (updateInteration == 2)
                {
                    Assert.AreEqual("connection string in redis", settings.ConnectionStrings.AdServer);
                    nfig.ClearOverride(settings.ApplicationName, "ConnectionStrings.AdServer", tier, dc);
                }
                else if (updateInteration == 3)
                {
                    Assert.IsNull(settings.ConnectionStrings.AdServer);
                }
            });

            var delays = 0;
            do
            {
                await Task.Delay(100);
                delays++;

            } while (updateInteration < 3 && delays < 30);

            if (updateInteration != 3)
                throw new Exception("Updates did not complete");
        }
    }
}
