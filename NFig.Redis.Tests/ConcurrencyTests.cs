using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using NUnit.Framework;
using StackExchange.Redis;

namespace NFig.Redis.Tests
{
    public class ConcurrencyTests
    {
        const string SETTING_NAME = "ConnectionStrings.AdServer";

        private NFigRedisStore<SampleSettings, Tier, DataCenter> _nfig;
        private ConnectionMultiplexer _redis;


        [SetUp]
        public void SetUp()
        {
            var opts = ConfigurationOptions.Parse("localhost:6379");
            opts.DefaultDatabase = 11;
            _redis = ConnectionMultiplexer.Connect(opts);

            _nfig = NFigRedisStore<SampleSettings, Tier, DataCenter>.FromConnectionMultiplexer(Tier.Any, _redis, opts.DefaultDatabase.Value);

            // Nuke any existing
            _redis.GetDatabase().KeyDelete(_nfig.RedisKeyPrefix + AppName());
        }

        [TearDown]
        public void TearDown()
        {
            _nfig.Dispose();
            _redis.Dispose();
        }

        [Test]
        public async Task CanSetInitialOverride()
        {
            var settings = await _nfig.GetAppSettingsAsync(AppName(), DataCenter.Any);
            await _nfig.SetOverrideAsync(AppName(), SETTING_NAME, "connection string in redis", DataCenter.Any, settings.Commit);
        }

        [Test]
        public async Task InitialOverrideUpdatesCommit()
        {
            var settings = await _nfig.GetAppSettingsAsync(AppName(), DataCenter.Any);
            var initialCommit = settings.Commit;
            Console.WriteLine("Initial Commit: {0}", initialCommit);
            await _nfig.SetOverrideAsync(AppName(), SETTING_NAME, "connection string in redis", DataCenter.Any, settings.Commit);
            var newCommit = await _nfig.GetCurrentCommitAsync(AppName());
            Console.WriteLine("New Commit: {0}", newCommit);
            Assert.AreNotEqual(initialCommit, newCommit);
        }

        [Test]
        public void InitialOverrideWithCommitFails()
        {
            Assert.Throws<NFigException>(async () =>
            {
                await _nfig.SetOverrideAsync(AppName(), SETTING_NAME, "connection string in redis", DataCenter.Any, "some commit");
            });
        }

        [Test]
        public async Task OverrideWithCurrentCommitSucceeds()
        {
            await _nfig.SetOverrideAsync(AppName(), SETTING_NAME, "connection string in redis", DataCenter.Any); // set commit

            // set a new one
            var commitId = await _nfig.GetCurrentCommitAsync(AppName());
            await _nfig.SetOverrideAsync(AppName(), SETTING_NAME, "some new value", DataCenter.Any, commitId);
        }

        [Test]
        public void OverrideWithOldCommitFails()
        {
            Assert.Throws<NFigException>(async () =>
            {
                await _nfig.SetOverrideAsync(AppName(), SETTING_NAME, "connection string in redis", DataCenter.Any); // set commit

                // set a new one
                var commitId = await _nfig.GetCurrentCommitAsync(AppName());
                await _nfig.SetOverrideAsync(AppName(), SETTING_NAME, "some new value", DataCenter.Any, commitId);
                // reuse old commit
                await _nfig.SetOverrideAsync(AppName(), SETTING_NAME, "another new value", DataCenter.Any, commitId);
            });
        }

        [Test]
        public void DelayedOverrideWithOldCommitFails()
        {
            Assert.Throws<NFigException>(async () =>
            {
                const DataCenter dc = DataCenter.Any;
                var appName = AppName();
                var settings = await _nfig.GetAppSettingsAsync(appName, dc);
                await _nfig.SetOverrideAsync(appName, SETTING_NAME, "initial value", dc, settings.Commit);
                var commitId = await _nfig.GetCurrentCommitAsync(appName);

                var delayedTask = Task.Run(async () =>
                {
                    await Task.Delay(500);
                    await _nfig.SetOverrideAsync(appName, SETTING_NAME, "delayed value", dc, commitId);
                });

                // Set a new value before delayed task can complete
                await _nfig.SetOverrideAsync(appName, SETTING_NAME, "new value", dc, commitId);

                // should throw here
                await delayedTask;
            });
        }

        private static string AppName() => $"ConcurrencySample-{TestContext.CurrentContext.Test.Name}";
    }
}