using System.Collections.Generic;
using System;
using System.Linq;
using System.Diagnostics;
using System.Threading;
using NFig;
using NFig.Redis;

using StackExchange.Redis;

using NUnit.Framework;
using System.Threading.Tasks;

using System.Linq.Expressions;

namespace NFig.Redis.Tests
{
    using NFigRedis = NFigRedisStore<SampleSettings, Tier, DataCenter>;

    [TestFixture]
    public class RedisTests
    {
        const int DEFAULT_DATABASE = 11;
        const Tier TestTier = Tier.Prod;
        const DataCenter TestDataCenter = DataCenter.Any;

        private static ConnectionMultiplexer s_redis;
        private static NFigRedis s_nfig;

        static void ClearOutRedis()
        {
            var keys = s_redis.GetServer("localhost:6379").Keys(DEFAULT_DATABASE, pattern: "NFig*").ToArray();
            s_redis.GetDatabase().KeyDelete(keys);
        }

        [OneTimeSetUp]
        public static void OneTimeSetUp()
        {
            var options = ConfigurationOptions.Parse("localhost:6379");
            options.DefaultDatabase = DEFAULT_DATABASE;
            s_redis = ConnectionMultiplexer.Connect(options);

            // Clear anything out
            ClearOutRedis();

            s_nfig = NFigRedis.FromConnectionMultiplexer(s_redis, db: DEFAULT_DATABASE);
            // Prime settings generation
            s_nfig.GetAppSettings("TestsInit", TestTier, TestDataCenter);
        }

        [OneTimeTearDown]
        public void TearDown()
        {
            TestContext.Out.WriteLine("Clearing out Redis");
            ClearOutRedis();
        }

        [Test]
        public async Task CanSetOverride()
        {
            await SetOverride(s => s.ConnectionStrings.AdServer, "connection string in redis");
        }

        [Test]
        public async Task SettingsAreCurrentAfterOverride()
        {
            var newSettings = await SetOverride(s => s.ConnectionStrings.AdServer, "connection string in redis");
            Assert.IsTrue(await s_nfig.IsCurrentAsync(newSettings));
        }

        [Test]
        public async Task SettingIsCorrectValueAfterOverride()
        {
            const string expected =  "connection string in redis";
            var newSettings = await SetOverride(s => s.ConnectionStrings.AdServer, expected);
            Assert.AreEqual(expected, newSettings.ConnectionStrings.AdServer);
        }

        [Test]
        public async Task OverrideForDifferentDataCenterDoesntAffectCurrentValue()
        {
            var newValue = Guid.NewGuid().ToString();
            var currentSettings = await s_nfig.GetAppSettingsAsync(AppName(), TestTier, DataCenter.Local);
            var newSettings = await SetOverride(s => s.Creatives.ImpressionSeparator, newValue, dataCenter: DataCenter.NewYork);
            Assert.AreEqual(newSettings.ApplicationName, currentSettings.ApplicationName);
            Assert.AreNotEqual(newSettings.Commit, currentSettings.Commit);
            Assert.AreNotEqual(newSettings.DataCenter, currentSettings.DataCenter);
        }

        string AppName() => TestContext.CurrentContext.Test.FullName;

        // Helper for turning the callback based
        // SubscribeToAppSettings API into a Task<TSettings>
        // Might be useful in NFig.Redis itself
        // Uses an Expression to get the name of the setting
        Task<SampleSettings> SetOverride<T>(
            Expression<Func<SampleSettings, T>> setting,
            string value,
            Tier? tier = null,
            DataCenter? dataCenter = null
            )
        {
            var appName = AppName();
            return Task.Run(() =>
            {
                var t = new TaskCompletionSource<SampleSettings>();
                var firstCall = true;
                void Handler(Exception ex, SampleSettings settings, NFigRedis nfig)
                {
                    if (firstCall)
                    {
                        firstCall = false;
                        return; // skip initial
                    }
                    TestContext.Out.WriteLine("[{0}] Got new settings", appName);
                    if (ex != null)
                        t.SetException(ex);
                    else
                        t.SetResult(settings);
                    nfig.UnsubscribeFromAppSettings(appName, tier ?? TestTier, dataCenter ?? TestDataCenter, Handler);
                }
                s_nfig.SubscribeToAppSettings(appName, tier ?? TestTier, dataCenter ?? TestDataCenter, Handler);
                s_nfig.SetOverride(appName, GetSettingName(setting), value, tier ?? TestTier, dataCenter ?? TestDataCenter);
                return t.Task;
            });
        }
        static string GetSettingName<T>(Expression<Func<SampleSettings, T>> sexpr)
        {
            Expression expr = (MemberExpression)sexpr.Body;
            var parts = new Stack<string>();
            while (expr is MemberExpression member)
            {
                parts.Push(member.Member.Name);
                expr = member.Expression;
            }
            return string.Join(".", parts);
        }
    }
}